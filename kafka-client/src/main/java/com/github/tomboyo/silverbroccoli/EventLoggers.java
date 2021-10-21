package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomboyo.silverbroccoli.kafka.BatchConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
  private static Map<String, Object> createConfig(Environment env) {
    var config = new HashMap<String, Object>();
    config.putAll(commonKafkaConfig(env));
    config.putAll(
        Map.of(
            BatchConsumer.TOPICS_CONF,
            "input-high,input-low,input-high.DLT,input-low.DLT,left,right",
            BatchConsumer.WORKERS_CONF,
            env.getProperty("sb.event-loggers.consumers", "2"),
            AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ENABLE_AUTO_COMMIT_CONFIG,
            "false",
            GROUP_ID_CONFIG,
            "EventLoggers",
            PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            RoundRobinAssignor.class.getName(),
            KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName()));
    return config;
  }

  public static void initializeEventLoggers(Environment env) {
    LOGGER.info("Starting kafka loggers");

    var config = createConfig(env);
    BatchConsumer.start(config, EventLoggers::handler);
  }

  private static void handler(ConsumerRecord<String, byte[]> record) {
    try {
      var event = DEFAULT_MAPPER.readValue(record.value(), Event.class);
      LOGGER.info("event={}", event);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deser event", e);
    }
  }
}
