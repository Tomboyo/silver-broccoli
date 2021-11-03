package com.github.tomboyo.silverbroccoli.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.github.tomboyo.silverbroccoli.ConfigurationSupport.composeConfigs;
import static com.github.tomboyo.silverbroccoli.ConfigurationSupport.extractMap;
import static com.github.tomboyo.silverbroccoli.KafkaConfiguration.kafkaPropertyNames;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

// TODO: make DLT optional (for consumers like Loggers, where failure is undesirable but not
// severe.)
public class BatchConsumer<K, V> implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchConsumer.class);

  public enum Conf {
    TOPICS("topics"),
    DLT("dlt"),
    WORKERS("workers"),
    POLL_TIMEOUT("poll.timeout.ms"),
    MAX_ATTEMPTS("max.attempts"),
    KAFKA_CONSUMER("kafka.consumer");

    private final String value;

    Conf(String value) {
      this.value = value;
    }

    public String value() {
      return value;
    }

    public static List<String> allConfs() {
      return Arrays.stream(Conf.values()).map(Conf::value).collect(Collectors.toList());
    }
  }

  private final KafkaConsumer<K, V> consumer;
  private final List<String> topics;
  private final ExecutorCompletionService<Void> workers;
  private final Function<ConsumerRecord<K, V>, Callable<Void>> createTask;
  private final Duration pollTimeout;

  /**
   * Create and start a consumer. The worker must be stateless and thread safe, since it will
   * process all messages on multiple threads.
   *
   * <p>Reads common configuration from "sb.kafka.common". Reads additional properties under the
   * given localConfigPrefix, including "...kafka.consumer" properties which override the common
   * configuration. In other words, every batch consumer reads shared common configuration from a
   * well-known location, and private (overriding) configuration from a given root.
   */
  public static <K, V> void start(
      Environment env,
      KafkaProducer<String, Object> producer,
      String localConfigPrefix,
      Consumer<ConsumerRecord<K, V>> worker) {
    var local = extractMap(env, localConfigPrefix, Conf.allConfs());
    var topics = parseTopics(local);
    var workers = parseWorkers(local, "3");
    var pollTimeout = parsePollTimeoutMs(local, "100");
    var maxAttempts = parseMaxAttempts(local, "3");
    var dlt = parseDlt(local);

    LOGGER.info(
        "Starting batch consumer: config={} topics={} workers={}",
        localConfigPrefix,
        topics,
        workers);

    var commonConf = extractMap(env, "sb.kafka.common", kafkaPropertyNames());
    var localConsumerConf =
        extractMap(
            env, join(".", localConfigPrefix, Conf.KAFKA_CONSUMER.value), kafkaPropertyNames());
    var consumer = new KafkaConsumer<K, V>(composeConfigs(List.of(commonConf, localConsumerConf)));

    // reserve an extra thread for the kafka consumer.
    var executor = Executors.newFixedThreadPool(workers + 1);
    executor.submit(
        new BatchConsumer<>(
            consumer,
            topics,
            new ExecutorCompletionService<>(executor),
            new BoundedRetryWorker<>(producer, maxAttempts, dlt, worker),
            pollTimeout));

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  consumer.wakeup();
                  executor.shutdown();
                }));
  }

  private static List<String> parseTopics(Map<String, Object> config) {
    var rawTopics =
        requireNonNull(((String) config.get(Conf.TOPICS.value)), "topics configuration required");
    return Arrays.stream(rawTopics.split(",")).map(String::trim).collect(Collectors.toList());
  }

  private static Duration parsePollTimeoutMs(Map<String, Object> config, String defaultValue) {
    String millis = (String) config.getOrDefault(Conf.POLL_TIMEOUT.value, defaultValue);
    return Duration.ofMillis(Long.parseLong(millis));
  }

  private static int parseWorkers(Map<String, Object> config, String defaultValue) {
    return Integer.parseInt((String) config.getOrDefault(Conf.WORKERS.value, defaultValue));
  }

  private static int parseMaxAttempts(Map<String, Object> config, String defaultValue) {
    return Integer.parseInt((String) config.getOrDefault(Conf.MAX_ATTEMPTS.value, defaultValue));
  }

  private static String parseDlt(Map<String, Object> config) {
    return requireNonNull(((String) config.get(Conf.DLT.value)), "dlt configuration required");
  }

  public BatchConsumer(
      KafkaConsumer<K, V> consumer,
      List<String> topics,
      ExecutorCompletionService<Void> workers,
      Consumer<ConsumerRecord<K, V>> worker,
      Duration pollTimeout) {
    this.consumer = consumer;
    this.topics = topics;
    this.workers = workers;
    this.createTask =
        (record) ->
            () -> {
              worker.accept(record);
              return null;
            };
    this.pollTimeout = pollTimeout;
  }

  @Override
  public void run() {
    // Tracks the status of the current batch of tasks.
    var batch = Optional.<Batch>empty();

    try {
      consumer.subscribe(topics);

      while (true) {
        var records = consumer.poll(pollTimeout);

        if (!records.isEmpty() && batch.isPresent()) {
          // We should never retrieve tasks while a batch is in progress.
          throw new IllegalStateException("Got records but a batch is already in progress.");
        }

        if (!records.isEmpty() && batch.isEmpty()) {
          batch = submitBatch(records);
        }

        if (batch.isPresent()) {
          if (batch.get().isDone()) {
            LOGGER.info("Committing batch: batch={}", batch.get());
            batch.get().unwrapExceptions();
            consumer.commitAsync();
            consumer.resume(consumer.assignment());
            batch = Optional.empty();
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
    } catch (ExecutionException e) {
      // ExecutionException may come from the batch.get().unwrapExceptions() call because of an
      // unexpected runtime exception in the worker thread. We must stop immediately in this case to
      // avoid committing past unprocessed elements. This error constitutes an urgent bug.
      LOGGER.info(
          "Shutting down due to unexpected processing exception. Pending messages will not be committed.");
      throw new RuntimeException("Unexpected processing error", e);
    } finally {
      try {
        consumer.close();
        LOGGER.info("Closed kafka consumer");
      } catch (KafkaException e) {
        LOGGER.error("Exception while closing kafka consumer", e);
      }
    }
  }

  private Optional<Batch> submitBatch(ConsumerRecords<K, V> records) {
    var futures =
        StreamSupport.stream(records.spliterator(), false)
            .map(createTask.andThen(workers::submit))
            .collect(Collectors.toList());
    if (futures.isEmpty()) {
      return Optional.empty();
    } else {
      var batch = new Batch(futures);
      LOGGER.info("Submitted batch: batch={}", batch);
      return Optional.of(batch);
    }
  }

  private static final class BoundedRetryWorker<K, V> implements Consumer<ConsumerRecord<K, V>> {
    private final KafkaProducer<?, Object> producer;
    private final int maxAttempts;
    private final String dlt;
    private final Consumer<ConsumerRecord<K, V>> delegate;

    public BoundedRetryWorker(
        KafkaProducer<?, Object> producer,
        int maxAttempts,
        String dlt,
        Consumer<ConsumerRecord<K, V>> delegate) {
      this.producer = producer;
      this.maxAttempts = maxAttempts;
      this.dlt = dlt;
      this.delegate = delegate;
    }

    @Override
    public void accept(ConsumerRecord<K, V> record) {
      // TODO: store this number in a task database. Increment it before processing to protect
      // against unrecoverable errors like OOME. Alternatively, use a kafka topic to track attempts.
      // We will skip implementation for this project because we've done something like this before
      // and are confident it works.
      var attempt = 0;

      while (++attempt < maxAttempts) {
        try {
          delegate.accept(record);
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

      // Synchronously send the record to a DLT or fail.
      try {
        LOGGER.error(
            "Publishing unprocessable record to DLT: maxAttempts={} DLT={} record=\"{}\"",
            maxAttempts,
            dlt,
            record);
        producer.send(new ProducerRecord<>(dlt, record)).get();
      } catch (Exception e) {
        // Failure to DLT a record constitutes a serious error. The consumer will shut down to avoid
        // committing over an unprocessed entity. When the application restarts, we will try to
        // process or DLT the message again.
        throw new RuntimeException(
            format(
                "Unable to publish unprocessable record to DLT: maxAttempts=%s DLT=%s record=\"%s\"",
                maxAttempts, record, dlt));
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
