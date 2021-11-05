package com.github.tomboyo.silverbroccoli.processors.auditors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomboyo.silverbroccoli.AuditLogRepository;
import com.github.tomboyo.silverbroccoli.Event;
import com.github.tomboyo.silverbroccoli.kafka.BoundedRetryBatchConsumer;
import com.github.tomboyo.silverbroccoli.kafka.BoundedRetryBatchConsumerProperties;
import com.github.tomboyo.silverbroccoli.kafka.CommonProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

public class Auditors {

  private static final Logger LOGGER = LoggerFactory.getLogger(Auditors.class);

  public static final ObjectMapper DEFAULT_MAPPER =
      new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static void initialize(
      CommonProperties commonProperties,
      @HighPriorityAuditors BoundedRetryBatchConsumerProperties highPriorityConfig,
      @LowPriorityAuditors BoundedRetryBatchConsumerProperties lowPriorityConfig,
      AuditLogRepository repository) {
    BoundedRetryBatchConsumer.<String, byte[]>fromConfig(
            commonProperties,
            highPriorityConfig,
            (producer, record) -> handle(repository, producer, record))
        .start();
    BoundedRetryBatchConsumer.<String, byte[]>fromConfig(
            commonProperties,
            lowPriorityConfig,
            (producer, record) -> handle(repository, producer, record))
        .start();
  }

  private static void handle(
      AuditLogRepository repository,
      KafkaProducer<String, Object> kafkaProducer,
      ConsumerRecord<String, byte[]> record) {
    var message = record.value();
    Event event;
    try {
      event = DEFAULT_MAPPER.readValue(message, Event.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deser event", e);
    }
    LOGGER.info("Processing: event={}", event);

    kafkaProducer.send(
        new ProducerRecord<>("left", new Event().message("LEFT: " + event.getMessage())));
    repository.createIfNotExists(event.getMessage());

    if (event.getMessage().toLowerCase().startsWith("fail")) {
      throw new RuntimeException("Simulated failure!");
    }

    kafkaProducer.send(
        new ProducerRecord<>("right", new Event().message("RIGHT: " + event.getMessage())),
        loggingCallback());
  }

  private static Callback loggingCallback() {
    return (meta, e) -> {
      if (e != null) {
        LOGGER.error("Failed to produce message", e);
      }
    };
  }
}
