package com.github.tomboyo.silverbroccoli.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

// 1. Because tasks may take a variable amount of time to process and could block the poll loop,
//    tasks should be processed in a separate thread.
// 2. Retry logic should be configured in this consumer to insulate business logic,
// 3. The consumer does not try to prioritize between topics. The application or control plane
//    should start more consumers based on demand.
// (4?) Consider moving message deserialization to the consumer as well. And deser failure should go
// directly to DLT.
// TODO: a rebalance listener should empty all pending tasks to avoid duplicate work. As long as
// tasks are idmepotent, however, this isn't a big deal.

public class BatchConsumer<K, V> implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchConsumer.class);

  public static final String TOPICS_CONF = "sb.topics";
  public static final String WORKERS_CONF = "sb.workers";
  public static final String POLL_TIMEOUT_CONF = "sb.poll.timeout";

  private final KafkaConsumer<K, V> consumer;
  private final List<String> topics;
  private final ExecutorCompletionService<Void> workers;
  private final Function<ConsumerRecord<K, V>, Callable<Void>> createTask;
  private final Duration pollTimeout;

  /**
   * Create and start a consumer. The worker must be stateless and thread safe, since it will
   * process all messages on multiple threads.
   */
  public static <K, V> void start(
      Map<String, Object> config, Consumer<ConsumerRecord<K, V>> worker) {
    var topics = parseTopics(config);
    var workers = parseWorkers(config, "3");
    var pollTimeout = parsePollTimeoutMs(config, "100");
    LOGGER.info("Starting batch consumer: topics={} workers={}", topics, workers, pollTimeout);

    var consumer = new KafkaConsumer<K, V>(config);
    // reserve an extra thread for the kafka consumer.
    var executor = Executors.newFixedThreadPool(workers + 1);
    executor.submit(
        new BatchConsumer<>(
            consumer,
            topics,
            new ExecutorCompletionService<Void>(executor),
            (record) ->
                () -> {
                  worker.accept(record);
                  return null;
                },
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
    return Arrays.stream(((String) config.get(TOPICS_CONF)).split(","))
        .map(String::trim)
        .collect(Collectors.toList());
  }

  private static Duration parsePollTimeoutMs(Map<String, Object> config, String defaultValue) {
    String millis = (String) config.getOrDefault(POLL_TIMEOUT_CONF, defaultValue);
    return Duration.ofMillis(Long.parseLong(millis));
  }

  private static int parseWorkers(Map<String, Object> config, String defaultValue) {
    return Integer.parseInt((String) config.getOrDefault(WORKERS_CONF, defaultValue));
  }

  public BatchConsumer(
      KafkaConsumer<K, V> consumer,
      List<String> topics,
      ExecutorCompletionService<Void> workers,
      Function<ConsumerRecord<K, V>, Callable<Void>> createTask,
      Duration pollTimeout) {
    this.consumer = consumer;
    this.topics = topics;
    this.workers = workers;
    this.createTask = createTask;
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
      // InterruptedException may come from the worker.poll().get() call because the underlying
      // executor has shut down. We must stop immediately in this case to avoid committing past
      // unprocessed elements.
      LOGGER.info("Caught normal request to shut down. Pending messages will not be committed.");
    } catch (ExecutionException e) {
      // ExecutionException may come from the worker.poll().get() call because of an unexpected
      // runtime exception in the worker thread. We must shut down in the case to avoid committing
      // past unprocessed elements. This error constitutes an urgent bug.
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
