package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomboyo.silverbroccoli.kafka.JacksonObjectSerializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.github.tomboyo.silverbroccoli.KafkaConfiguration.commonKafkaConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class EventAuditLoggers {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventAuditLoggers.class);

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
            "event-consumer-group-1",
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
    int numConsumers = env.getProperty("sb.audit-loggers.consumers", Integer.class, 4);
    var consumers =
        Stream.generate(() -> createKafkaConsumer(env))
            .limit(numConsumers)
            .collect(Collectors.toList());
    var executor = Executors.newFixedThreadPool(numConsumers);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  consumers.forEach(KafkaConsumer::wakeup);
                  executor.shutdown();
                }));
    consumers.forEach(
        consumer ->
            executor.submit(
                auditLogger(
                    LOGGER, consumer, createKafkaProducer(env), repository, DEFAULT_MAPPER)));
  }

  /**
   * The audit logger persists events as audit logs idempotently, then atomically sends two messages
   * to the left and right topics. These operations are transactional but not XA, so th DB
   * transaction may succeed while the Kafka transactions fails.
   */
  public static Runnable auditLogger(
      Logger logger,
      KafkaConsumer<String, byte[]> kafkaConsumer,
      KafkaProducer<String, Object> kafkaProducer,
      AuditLogRepository repository,
      ObjectMapper mapper) {
    return () -> {
      logger.info("Audot logger is starting");
      kafkaConsumer.subscribe(List.of("input-high", "input-low"));

      try {
        while (true) {
          var records = kafkaConsumer.poll(Duration.ofMinutes(1));
          for (var record : records) {
            var topic = record.topic();
            var partition = record.partition();
            try {
              var event = mapper.readValue(record.value(), Event.class);
              // TODO: transaction for producer!
              kafkaProducer.send(
                  new ProducerRecord<>("left", new Event().message("LEFT: " + event.getMessage())));
              repository.createIfNotExists(event.getMessage());

              if (event.getMessage().toLowerCase().startsWith("fail")) {
                throw new RuntimeException("Simulated failure!");
              }

              kafkaProducer.send(
                  new ProducerRecord<>(
                      "right", new Event().message("RIGHT: " + event.getMessage())),
                  loggingCallback());
            } catch (Exception e) {
              logger.warn("Failed to process event: topic={} partition={}", topic, partition, e);
            }
          }
          kafkaConsumer.commitAsync();
        }
      } catch (WakeupException e) {
        logger.info("Caught request to close kafka consumer");
        try {
          kafkaConsumer.commitSync();
        } catch (Exception e2) {
          logger.warn("Failed to commit before shutdown", e2);
        }
      } finally {
        try {
          kafkaConsumer.close();
          kafkaProducer.close();
          logger.info("Closed kafka consumer");
        } catch (KafkaException e) {
          logger.error("Exception while closing kafka consumer", e);
        }
      }
    };
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
