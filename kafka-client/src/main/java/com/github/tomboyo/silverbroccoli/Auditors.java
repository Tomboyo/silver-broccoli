package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomboyo.silverbroccoli.kafka.BatchConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.io.IOException;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

public class Auditors {

  private static final Logger LOGGER = LoggerFactory.getLogger(Auditors.class);

  // TODO: common consumer configuration
  public static final ObjectMapper DEFAULT_MAPPER =
      new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static void initialize(
      Environment env, KafkaProducer<String, Object> producer, AuditLogRepository repository) {
    BatchConsumer.<String, byte[]>start(
        env, "sb.auditors.high.priority", (record) -> handle(record, producer, repository));
    BatchConsumer.<String, byte[]>start(
        env, "sb.auditors.low.priority", (record) -> handle(record, producer, repository));
  }

  private static void handle(
      ConsumerRecord<String, byte[]> record,
      KafkaProducer<String, Object> kafkaProducer,
      AuditLogRepository repository) {
    var message = record.value();
    Event event;
    try {
      event = DEFAULT_MAPPER.readValue(message, Event.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deser event", e);
    }
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

  private static Callback loggingCallback() {
    return (meta, e) -> {
      if (e != null) {
        LOGGER.error("Failed to produce message", e);
      }
    };
  }
}
