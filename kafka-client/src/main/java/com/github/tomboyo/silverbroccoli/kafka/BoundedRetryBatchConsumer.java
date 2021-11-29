package com.github.tomboyo.silverbroccoli.kafka;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.String.format;

/**
 * A Kafka consumer with bounded consumer retry, optional dead-lettering, and multi-threading.
 *
 * <p>Suitable for non-transactional (i.e. non-atomic) pipelines only. Messages are retrieved in
 * batches and operated on by a pool of workers until complete, at which point the batch is
 * committed asynchronously. Messages which are unprocessable (i.e. exhaust all allowed attempts)
 * are submitted to a DLT (if configured). If DLT production fails, this consumer aborts to avoid
 * committing past the message.
 *
 * @param <K> Key type of messages.
 * @param <V> Value type of messages.
 */
public class BoundedRetryBatchConsumer<K, V> implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BoundedRetryBatchConsumer.class);

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

  public static <K, V> BoundedRetryBatchConsumer<K, V> fromConfig(
      CommonProperties commonProperties,
      BoundedRetryBatchConsumerProperties config,
      ConsumerCallback<K, V> worker) {

    var kafkaConsumer =
        new KafkaConsumer<K, V>(
            combine(commonProperties.getKafkaCommon(), config.getKafkaConsumerConfig()));

    // TODO: non-transactional producers can be shared application-wide. This is fine, though.
    var kafkaProducer =
        new KafkaProducer<K, Object>(
            combine(commonProperties.getKafkaCommon(), config.getKafkaProducerConfig()));

    return new BoundedRetryBatchConsumer<>(
        kafkaConsumer,
        kafkaProducer,
        config.getTopics(),
        Executors.newFixedThreadPool(config.getWorkers() + 1),
        new BoundedRetryWorker<>(config.getMaxAttempts(), config.getDlt(), worker),
        config.getPollTimeout());
  }

  public BoundedRetryBatchConsumer(
      KafkaConsumer<K, V> consumer,
      KafkaProducer<K, Object> producer,
      List<String> topics,
      ExecutorService executorService,
      ConsumerCallback<K, V> worker,
      Duration pollTimeout) {
    this.consumer = consumer;
    this.producer = producer;
    this.topics = topics;
    this.executorService = executorService;
    this.pollTimeout = pollTimeout;
    this.worker = worker;
  }

  public void start() {
    var consumerMetrics = new KafkaClientMetrics(consumer);
    var producerMetrics = new KafkaClientMetrics(producer);
    consumerMetrics.bindTo(Metrics.globalRegistry);
    producerMetrics.bindTo(Metrics.globalRegistry);
    executorService.submit(this);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  consumerMetrics.close();
                  producerMetrics.close();
                  stop();
                }));
  }

  public void stop() {
    consumer.wakeup();
    executorService.shutdown();
  }

  @Override
  public void run() {
    var workers = new ExecutorCompletionService<Void>(executorService);

    // Tracks the status of the current batch of tasks.
    var currentBatch = Optional.<Batch>empty();

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
          currentBatch = submitBatch(workers, records);
        }

        if (currentBatch.isPresent()) {
          var batch = currentBatch.get();
          if (batch.isDone()) {
            batch.unwrapExceptions();
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

  private Optional<Batch> submitBatch(
      ExecutorCompletionService<Void> workers, ConsumerRecords<K, V> records) {
    var futures =
        StreamSupport.stream(records.spliterator(), false)
            .map(
                record ->
                    workers.submit(
                        () -> {
                          worker.consume(producer, record);
                          return null;
                        }))
            .collect(Collectors.toList());
    if (futures.isEmpty()) {
      return Optional.empty();
    } else {
      var batch = new Batch(futures);
      LOGGER.info("Submitted batch: batch={}", batch);
      return Optional.of(batch);
    }
  }

  private static final class BoundedRetryWorker<K, V> implements ConsumerCallback<K, V> {
    private final int maxAttempts;
    private final Optional<String> dlt;
    private final ConsumerCallback<K, V> delegate;

    public BoundedRetryWorker(
        int maxAttempts, Optional<String> dlt, ConsumerCallback<K, V> delegate) {
      this.maxAttempts = maxAttempts;
      this.dlt = dlt;
      this.delegate = delegate;
    }

    @Override
    public void consume(KafkaProducer<K, Object> producer, ConsumerRecord<K, V> record) {
      var attempt = 0;

      while (++attempt <= maxAttempts) {
        try {
          delegate.consume(producer, record);
          return;
        } catch (Exception e) {
          LOGGER.warn(
              "Failed attempt: attempt={} maxAttempts={} record=\"{}\"",
              attempt,
              maxAttempts,
              record,
              e);
        }
      }

      if (dlt.isEmpty()) {
        LOGGER.error(
            "Skipping unprocessable record (no DLT configured): maxAttempts={} record=\"{}\"",
            maxAttempts,
            record);
        return;
      }

      try {
        LOGGER.error(
            "Publishing unprocessable record to DLT: maxAttempts={} DLT={} record=\"{}\"",
            maxAttempts,
            dlt,
            record);
        // This must be synchronous to guarantee the DLT entry exists before committing.
        producer
            .send(
                new ProducerRecord<>(
                    dlt.get(),
                    null,
                    null,
                    null,
                    record.value(),
                    List.of(new RecordHeader("original_topic", record.topic().getBytes()))))
            .get();
      } catch (Exception e) {
        // Failure to DLT a record constitutes a serious error. The consumer will shut down to avoid
        // committing over an unprocessed entity. When the application restarts, we will try to
        // process or DLT the message again.
        throw new RuntimeException(
            format(
                "Unable to publish unprocessable record to DLT: maxAttempts=%s DLT=%s record=\"%s\"",
                maxAttempts, record, dlt),
            e);
      }
    }
  }

  private static final class Batch {

    private static final AtomicLong sequence = new AtomicLong();

    private final List<Future<Void>> futures;
    private final long serialId;

    private Batch(List<Future<Void>> futures) {
      if (futures.isEmpty()) {
        throw new IllegalArgumentException("A batch must not be empty");
      }

      this.futures = futures;
      this.serialId = sequence.getAndIncrement();
    }

    public boolean isDone() {
      return futures.stream().allMatch(Future::isDone);
    }

    /** Call Future::get on each future in the batch, throwing the first exception found, if any. */
    public void unwrapExceptions() throws InterruptedException, ExecutionException {
      for (var f : futures) {
        f.get();
      }
    }

    @Override
    public String toString() {
      return "Batch{" + " size=\"" + futures.size() + "\" serialId=\"" + serialId + "\" }";
    }
  }
}
