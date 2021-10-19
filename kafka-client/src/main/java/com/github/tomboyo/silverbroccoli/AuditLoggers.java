package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomboyo.silverbroccoli.kafka.JacksonObjectSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.github.tomboyo.silverbroccoli.KafkaConfiguration.commonKafkaConfig;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class AuditLoggers {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuditLoggers.class);
  private static final int MAX_RECORDS_PER_POLL = 20;

  // TODO: common consumer configuration
  public static final ObjectMapper DEFAULT_MAPPER =
      new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  // TODO: common consumer configuration
  private static KafkaConsumer<String, byte[]> createKafkaConsumer(Environment env) {
    var config = new HashMap<String, Object>();
    config.putAll(commonKafkaConfig(env));
    config.putAll(
        Map.of(
            AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ENABLE_AUTO_COMMIT_CONFIG,
            "false",
            GROUP_ID_CONFIG,
            "audit-consumer-group-1",
            MAX_POLL_RECORDS_CONFIG,
            Integer.toString(MAX_RECORDS_PER_POLL),
            PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            RoundRobinAssignor.class.getName(),
            KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName()));
    return new KafkaConsumer<>(config);
  }

  // TODO: duplication
  private static KafkaProducer<String, Object> createKafkaProducer(Environment env) {
    var kafkaProps = new Properties();
    kafkaProps.putAll(commonKafkaConfig(env));
    kafkaProps.putAll(
        Map.of(
            KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            VALUE_SERIALIZER_CLASS_CONFIG, JacksonObjectSerializer.class.getName()));
    return new KafkaProducer<>(kafkaProps);
  }

  public static void initializeAuditLoggers(Environment env, AuditLogRepository repository) {
    int numWorkers = env.getProperty("sb.audit-loggers.workers", Integer.class, 3);
    LOGGER.info("Starting kafka consumer with {} threaded workers", numWorkers);
    var consumer = createKafkaConsumer(env);
    var executor = Executors.newFixedThreadPool(numWorkers + 1);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  consumer.wakeup();
                  executor.shutdown();
                }));

    var buffer = new PriorityKafkaBuffer<String, byte[]>(List.of("input-high", "input-low"), 5, 6);
    var concurrentConsumer =
        new ConcurrentKafkaConsumer<>(
            consumer, List.of("input-high", "input-low"), buffer, Duration.ofMillis(100));
    executor.submit(concurrentConsumer);

    for (int i = 0; i < numWorkers; i++) {
      var worker =
          new PriorityConsumer<>(
              buffer,
              record -> {
                try {
                  handle(record, DEFAULT_MAPPER, createKafkaProducer(env), repository);
                } catch (Exception e) {
                  // TODO
                  throw new RuntimeException(e);
                }
              });
      executor.submit(worker);
    }
  }

  private static class PriorityKafkaBuffer<K, V> {

    private final ArrayList<String> topics;
    private final LinkedBlockingQueue<ConsumerRecord<K, V>> acks;
    private final PriorityBlockingQueue<ConsumerRecord<K, V>> queue;
    private final Map<String, AtomicInteger> counts;
    private final int lowWaterMark, highWaterMark;

    public PriorityKafkaBuffer(List<String> topics, int lowWaterMark, int highWaterMark) {
      if (lowWaterMark <= 0 || highWaterMark <= lowWaterMark) {
        throw new IllegalArgumentException("Must have 0 <= lowWaterMark < highWaterMark");
      }

      this.topics = new ArrayList<>(topics);
      this.acks = new LinkedBlockingQueue<>(highWaterMark);
      this.queue = new PriorityBlockingQueue<>(highWaterMark, this::compare);
      this.counts =
          this.topics.stream().collect(toMap(Function.identity(), _topic -> new AtomicInteger()));
      this.lowWaterMark = lowWaterMark;
      this.highWaterMark = highWaterMark;
    }

    /** Comparator which ranks by topic priority and preserves order by offset within a topic. */
    private int compare(ConsumerRecord<K, V> a, ConsumerRecord<K, V> b) {
      var indexA = topics.indexOf(a.topic());
      var indexB = topics.indexOf(b.topic());
      if (indexA < 0 || indexB < 0) {
        throw new IllegalArgumentException("Given illegal topic");
      }
      if (indexA < indexB) {
        return -1;
      } else {
        if (indexA == indexB) {
          var delta = a.offset() - b.offset();
          if (delta < 0) {
            return -1;
          } else if (delta == 0) {
            return 0;
          } else {
            return 1;
          }
        } else {
          return 1;
        }
      }
    }

    public void addAll(Collection<ConsumerRecord<K, V>> records) {
      var countByTopic = records.stream().collect(groupingBy(ConsumerRecord::topic, counting()));
      LOGGER.info("AddAll: countByTopic={}", countByTopic);
      countByTopic.forEach((topic, count) -> counts.get(topic).addAndGet(count.intValue()));
      queue.addAll(records);
    }

    public void add(ConsumerRecord<K, V> record) {
      LOGGER.info(
          "Add: topic={}, partition={}, offset={}",
          record.topic(),
          record.partition(),
          record.offset());
      counts.get(record.topic()).incrementAndGet();
      queue.add(record);
    }

    public boolean isLow(String topic) {
      return counts.get(topic).get() <= lowWaterMark;
    }

    public boolean isHigh(String topic) {
      return counts.get(topic).get() >= highWaterMark;
    }

    public ConsumerRecord<K, V> take() throws InterruptedException {
      var record = queue.take();
      counts.get(record.topic()).decrementAndGet();
      return record;
    }

    public Map<TopicPartition, OffsetAndMetadata> drainAcks() {
      var tmp = new ArrayList<ConsumerRecord<K, V>>();
      acks.drainTo(tmp);
      return tmp.stream()
          .collect(
              toMap(
                  record -> new TopicPartition(record.topic(), record.partition()),
                  record -> new OffsetAndMetadata(record.offset()),
                  // Keep highest offset
                  (a, b) -> a.offset() > b.offset() ? a : b));
    }

    public void ack(ConsumerRecord<K, V> record) {
      LOGGER.info(
          "Ack: topic={} partition={} offset={}",
          record.topic(),
          record.partition(),
          record.offset());
      acks.add(record);
    }

    public void nack(ConsumerRecord<K, V> record, Throwable t) {
      // TODO: handle nacks
    }

    @Override
    public String toString() {
      return "{ queue=" + queue.size() + " acks=" + acks.size() + " }";
    }
  }

  public static class ConcurrentKafkaConsumer<K, V> implements Runnable {

    private final KafkaConsumer<K, V> consumer;
    private final List<String> topics;
    private final PriorityKafkaBuffer<K, V> buffer;
    private final Duration commitInterval;

    public ConcurrentKafkaConsumer(
        KafkaConsumer<K, V> consumer,
        List<String> topics,
        PriorityKafkaBuffer<K, V> buffer,
        Duration commitInterval) {
      this.consumer = consumer;
      this.topics = topics;
      this.buffer = buffer;
      this.commitInterval = commitInterval;
    }

    @Override
    public void run() {
      consumer.subscribe(topics);

      var commitTimeout = Instant.now().plus(commitInterval);

      try {
        while (true) {
          // 1. Offload all new records to the buffer.
          var records =
              StreamSupport.stream(consumer.poll(Duration.ofMillis(100)).spliterator(), false)
                  .collect(Collectors.toList());
          if (!records.isEmpty()) buffer.addAll(records);

          // 2. Pause or resume topics based on buffer demand.
          consumer.assignment().stream()
              .collect(Collectors.groupingBy(TopicPartition::topic))
              .forEach(
                  (topic, partitions) -> {
                    if (buffer.isLow(topic)) {
                      consumer.resume(partitions);
                    } else if (buffer.isHigh(topic)) {
                      consumer.pause(partitions);
                    }
                  });

          // 3. Periodically commit buffered Acks.
          var now = Instant.now();
          if (commitTimeout.isBefore(now)) {
            commitTimeout = now.plus(commitInterval);

            var acks = buffer.drainAcks();
            if (!acks.isEmpty()) {
              LOGGER.info("Committing: acks={}", acks);
              consumer.commitAsync(
                  acks,
                  (_meta, e) -> {
                    if (e != null) LOGGER.warn("Failed to commit", e);
                  });
            }
          }
        }
      } catch (WakeupException e) {
        LOGGER.info("Caught request to close kafka consumer");
        try {
          var acks = buffer.drainAcks();
          if (!acks.isEmpty()) {
            consumer.commitSync(acks);
          }
        } catch (Exception e2) {
          LOGGER.warn("Failed to commit before shutdown", e2);
        }
      } finally {
        try {
          consumer.close();
          LOGGER.info("Closed kafka consumer");
        } catch (KafkaException e) {
          LOGGER.error("Exception while closing kafka consumer", e);
        }
      }
    }
  }

  public static class PriorityConsumer<K, V> implements Runnable {
    private final PriorityKafkaBuffer<K, V> buffer;
    private final Consumer<ConsumerRecord<K, V>> consumer;

    public PriorityConsumer(
        PriorityKafkaBuffer<K, V> buffer, Consumer<ConsumerRecord<K, V>> consumer) {
      this.buffer = buffer;
      this.consumer = consumer;
    }

    @Override
    public void run() {
      try {
        while (true) {
          var record = buffer.take();
          try {
            //            consumer.accept(record);
            Thread.sleep(50);
            buffer.ack(record);
          } catch (Exception e) {
            buffer.nack(record, e);
          }
        }
      } catch (InterruptedException e) {
        LOGGER.info("Caught interrupt. Exiting priority consumer.");
      }
    }
  }

  private static void handle(
      ConsumerRecord<String, byte[]> record,
      ObjectMapper mapper,
      KafkaProducer<String, Object> kafkaProducer,
      AuditLogRepository repository)
      throws IOException {
    var message = record.value();
    var event = mapper.readValue(message, Event.class);
    LOGGER.info("Processing: event={}", event);

    // TODO: transaction for producer!
    kafkaProducer.send(
        new ProducerRecord<>("left", new Event().message("LEFT: " + event.getMessage())));
    repository.createIfNotExists(event.getMessage());

    //    if (event.getMessage().toLowerCase().startsWith("fail")) {
    //      throw new RuntimeException("Simulated failure!");
    //    }

    kafkaProducer.send(
        new ProducerRecord<>("right", new Event().message("RIGHT: " + event.getMessage())),
        loggingCallback());
  }

  // TODO: duplication
  private static Callback loggingCallback() {
    return (meta, e) -> {
      if (e != null) {
        LOGGER.error("Failed to produce message", e);
      }
    };
  }
}
