package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
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

/**
 * The Event Loggers are Kafka consumers which log each Event instance from the input-low and
 * input-high topics. All event loggers are in the same consumer group, so any Kafka message is
 * handled by at most one event logger (they are "competing consumers"). However, the loggers do not
 * compete for messages with other consumer groups, which each get their own copy of every message
 * the loggers get a copy of.
 */
@Configuration
public class EventLoggers {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventLoggers.class);

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

  public static void initializeEventLoggers(Environment env) {
    var numConsumers = env.getProperty("sb.event-loggers.consumers", Integer.class, 4);
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
        consumer -> executor.submit(eventLogger(LOGGER, consumer, EventLoggers.DEFAULT_MAPPER)));
  }

  public static Runnable eventLogger(
      Logger logger, KafkaConsumer<String, byte[]> kafkaConsumer, ObjectMapper mapper) {
    return () -> {
      logger.info("Event Logger is starting");
      kafkaConsumer.subscribe(Topics.topicNames().collect(Collectors.toList()));

      try {
        while (true) {
          var records = kafkaConsumer.poll(Duration.ofMinutes(1));
          for (var record : records) {
            var topic = record.topic();
            var partition = record.partition();
            try {
              var event = mapper.readValue(record.value(), Event.class);
              logger.info("topic={} partition={} event={}", topic, partition, event);
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
          logger.info("Closed kafka consumer");
        } catch (KafkaException e) {
          logger.error("Exception while closing kafka consumer", e);
        }
      }
    };
  }
}
