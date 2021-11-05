package com.github.tomboyo.silverbroccoli.processors.loggers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomboyo.silverbroccoli.Event;
import com.github.tomboyo.silverbroccoli.kafka.BoundedRetryBatchConsumer;
import com.github.tomboyo.silverbroccoli.kafka.BoundedRetryBatchConsumerProperties;
import com.github.tomboyo.silverbroccoli.kafka.CommonProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

/**
 * The Event Loggers are Kafka consumers which log each Event instance from the input-low and
 * input-high topics. All event loggers are in the same consumer group, so any Kafka message is
 * handled by at most one event logger (they are "competing consumers"). However, the loggers do not
 * compete for messages with other consumer groups, which each get their own copy of every message
 * the loggers get a copy of.
 */
@Configuration
public class Loggers {

  private static final Logger LOGGER = LoggerFactory.getLogger(Loggers.class);

  public static final ObjectMapper DEFAULT_MAPPER =
      new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static void initialize(
      CommonProperties commonProperties,
      @LeftRightLoggers BoundedRetryBatchConsumerProperties loggersConfig) {
    BoundedRetryBatchConsumer.<String, byte[]>fromConfig(
            commonProperties, loggersConfig, (producer, record) -> handler(record))
        .start();
  }

  private static void handler(ConsumerRecord<String, byte[]> record) {
    try {
      var event = DEFAULT_MAPPER.readValue(record.value(), Event.class);
      LOGGER.info("topic={} event={}", record.topic(), event);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deser event", e);
    }
  }
}
