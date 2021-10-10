package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public final class EventConsumers {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumers.class);

  public static final ObjectMapper DEFAULT_MAPPER =
      new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static KafkaConsumer<String, byte[]> createKafkaConsumer(
      Map<String, Object> commonConfig) {
    var config = new HashMap<String, Object>(commonConfig);
    config.putAll(
        Map.of(
            AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ENABLE_AUTO_COMMIT_CONFIG,
            "false",
            GROUP_ID_CONFIG,
            "event-consumer-group-1",
            KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName()));
    return new KafkaConsumer<>(config);
  }

  public static void run(KafkaConsumer<String, byte[]> kafkaConsumer, ObjectMapper mapper) {
    LOGGER.info("Event consumer is starting");
    kafkaConsumer.subscribe(List.of("input-high"));

    try {
      while (true) {
        var records = kafkaConsumer.poll(Duration.ofMinutes(1));
        for (var record : records) {
          try {
            var event = mapper.readValue(record.value(), Event.class);
            LOGGER.info("Consumed event: event='{}'", event);
          } catch (Exception e) {
            LOGGER.warn("Failed to process event", e);
          }
        }
        kafkaConsumer.commitAsync();
      }
    } catch (WakeupException e) {
      LOGGER.info("Caught request to close kafka consumer");
      try {
        kafkaConsumer.commitSync();
      } catch (Exception e2) {
        LOGGER.warn("Failed to commit before shutdown", e2);
      }
    } finally {
      try {
        kafkaConsumer.close();
        LOGGER.info("Closed kafka consumer");
      } catch (KafkaException e) {
        LOGGER.error("Exception while closing kafka consumer", e);
      }
    }
  }
}
