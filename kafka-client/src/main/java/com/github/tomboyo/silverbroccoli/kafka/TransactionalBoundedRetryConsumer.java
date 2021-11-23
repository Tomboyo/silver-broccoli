package com.github.tomboyo.silverbroccoli.kafka;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.String.format;

/**
 * @param <K> Key type of messages.
 * @param <V> Value type of messages.
 */
public class TransactionalBoundedRetryConsumer<K, V> implements Runnable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TransactionalBoundedRetryConsumer.class);

  private final TransactionalBoundedRetryConsumerProperties properties;
  private final KafkaConsumer<K, V> consumer;
  private final KafkaProducer<K, Object> producer;
  private final List<String> topics;
  private final ExecutorService executorService;
  private final ConsumerCallback<K, V> worker;
  private final Duration pollTimeout;

  private static <K, V> Map<K, V> combine(Map<K, V> left, Map<K, V> right) {
    var merged = new HashMap<>(left);
    merged.putAll(right);
    return merged;
  }

  // TODO: enforce isolation level and transactional id configs, among possibly others
  public static <K, V> TransactionalBoundedRetryConsumer<K, V> fromConfig(
      CommonProperties commonProperties,
      TransactionalBoundedRetryConsumerProperties config,
      ConsumerCallback<K, V> worker) {

    var kafkaConsumer =
        new KafkaConsumer<K, V>(
            combine(commonProperties.getKafkaCommon(), config.getKafkaConsumerConfig()));

    var kafkaProducer =
        new KafkaProducer<K, Object>(
            combine(commonProperties.getKafkaCommon(), config.getKafkaProducerConfig()));
    kafkaProducer.initTransactions();

    return new TransactionalBoundedRetryConsumer<>(
        config,
        kafkaConsumer,
        kafkaProducer,
        config.getTopics(),
        // One thread for the consumer, one for the non-thread-safe transactional worker.
        Executors.newFixedThreadPool(2),
        worker,
        config.getPollTimeout());
  }

  public TransactionalBoundedRetryConsumer(
      TransactionalBoundedRetryConsumerProperties properties,
      KafkaConsumer<K, V> consumer,
      KafkaProducer<K, Object> producer,
      List<String> topics,
      ExecutorService executorService,
      ConsumerCallback<K, V> worker,
      Duration pollTimeout) {
    this.properties = properties;
    this.consumer = consumer;
    this.producer = producer;
    this.topics = topics;
    this.executorService = executorService;
    this.pollTimeout = pollTimeout;
    this.worker = worker;
  }

  public void start() {
    executorService.submit(this);
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
  }

  public void stop() {
    consumer.wakeup();
    executorService.shutdown();
  }

  @Override
  public void run() {
    var workers = new ExecutorCompletionService<Void>(executorService);

    // Tracks the status of the current batch of tasks.
    var currentBatch = Optional.<Future<Void>>empty();

    try {
      consumer.subscribe(topics);

      while (true) {
        var records = consumer.poll(pollTimeout);

        if (!records.isEmpty() && currentBatch.isPresent()) {
          // We should never retrieve tasks while a batch is in progress.
          throw new IllegalStateException("Got records but a batch is already in progress.");
        }

        // batch.isEmpty() is true.
        if (!records.isEmpty()) {
          var list =
              StreamSupport.stream(records.spliterator(), false).collect(Collectors.toList());
          currentBatch =
              Optional.of(
                  workers.submit(
                      serialWorker(
                          list,
                          producer,
                          transactedBoundedRetryCallback(
                              properties.getMaxAttempts(),
                              properties.getDlt(),
                              consumer.groupMetadata(),
                              worker))));
        }

        if (currentBatch.isPresent()) {
          var batch = currentBatch.get();
          if (batch.isDone()) {
            batch.get(); // unwrap any execution exceptions to preempt commit
            consumer.commitAsync();
            consumer.resume(consumer.assignment());
            currentBatch = Optional.empty();
          } else {
            // Stop retrieving messages until the current batch is finished. Failure to pause means
            // our completion service will accumulate messages until OOME and will never commit.
            consumer.pause(consumer.assignment());
          }
        }
      }
    } catch (InterruptedException | WakeupException e) {
      // InterruptedException may come from the batch.get().unwrapExceptions() call because the
      // underlying executor has shut down. We must stop immediately in this case to avoid
      // committing past unprocessed elements.
      LOGGER.info("Caught normal request to shut down. Pending messages will not be committed.");
    } catch (Exception e) {
      // ExecutionException may come from the batch.get().unwrapExceptions() call because of an
      // unexpected runtime exception in the worker thread. We must stop immediately in this case to
      // avoid committing past unprocessed elements. This error constitutes an urgent bug.
      LOGGER.error(
          "Shutting down due to unexpected processing exception. Pending messages will not be committed.",
          e);
      // TODO: I think this should re-throw?
    } finally {
      try {
        consumer.close();
        LOGGER.info("Closed kafka consumer");
      } catch (KafkaException e) {
        LOGGER.error("Exception while closing kafka consumer", e);
      }

      executorService.shutdown();
    }
  }

  /** Invokes the delegate on each record in the list in the given order. All exceptions bubble. */
  private static <K, V> Callable<Void> serialWorker(
      List<ConsumerRecord<K, V>> records,
      KafkaProducer<K, Object> producer,
      ConsumerCallback<K, V> delegate) {
    return () -> {
      for (var record : records) {
        delegate.consume(producer, record);
      }
      return null;
    };
  }

  /**
   * Attempts to process records until a maximum number of attempts is reached. If the record is
   * unprocessable, it is sent to the given DLT (if any). Exceptions are swallowed except when
   * production to DLT fails.
   */
  private static <K, V> ConsumerCallback<K, V> transactedBoundedRetryCallback(
      int maxAttempts,
      Optional<String> dlt,
      ConsumerGroupMetadata metadata,
      ConsumerCallback<K, V> delegate) {
    return (producer, record) -> {
      // TODO: store this number in a task database. Increment it before processing to protect
      // against unrecoverable errors like OOME. Alternatively, use a kafka topic to track attempts.
      // We will skip implementation for this project because we've done something like this before
      // and are confident it works.
      var attempt = 0;

      while (++attempt <= maxAttempts) {
        try {
          producer.beginTransaction();
          // The delegate may produce any number of records within this transaction.
          delegate.consume(producer, record);

          // Add offsets to the transaction.
          producer.sendOffsetsToTransaction(
              Map.of(
                  new TopicPartition(record.topic(), record.partition()),
                  new OffsetAndMetadata(record.offset())),
              metadata);
          producer.commitTransaction();
          return;
        } catch (Exception e) {
          LOGGER.warn(
              "Failed attempt: attempt={} maxAttempts={} record=\"{}\"",
              attempt,
              maxAttempts,
              record,
              e);
          producer.abortTransaction();
        }
      }

      if (dlt.isEmpty()) {
        LOGGER.error(
            "Skipping unprocessable record (no DLT configured): maxAttempts={} record=\"{}\"",
            maxAttempts,
            record);
        producer.beginTransaction();
        producer.sendOffsetsToTransaction(
            Map.of(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset())),
            metadata);
        producer.commitTransaction();
        return;
      }

      try {
        LOGGER.warn(
            "Publishing unprocessable record to DLT: maxAttempts={} DLT={} record=\"{}\"",
            maxAttempts,
            dlt,
            record);
        producer.beginTransaction();
        producer.send(
            new ProducerRecord<>(
                dlt.get(),
                null,
                null,
                null,
                record.value(),
                List.of(new RecordHeader("original_topic", record.topic().getBytes()))));
        producer.sendOffsetsToTransaction(
            Map.of(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset())),
            metadata);
        producer.commitTransaction();
      } catch (Exception e) {
        // Failure to DLT a record constitutes a serious error. The consumer will shut down to avoid
        // committing over an unprocessed entity. When the application restarts, we will try to
        // process or DLT the message again.
        throw new RuntimeException(
            format(
                "Unable to publish unprocessable record to DLT: maxAttempts=%s DLT=%s record=\"%s\"",
                maxAttempts, dlt, record),
            e);
      }
    };
  }
}
