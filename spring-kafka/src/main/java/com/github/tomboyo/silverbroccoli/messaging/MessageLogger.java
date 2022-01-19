package com.github.tomboyo.silverbroccoli.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageLogger.class);

  @KafkaListener(
      id = "InputLogger",
      topics = {"input-low", "input-low.DLT", "input-high", "input-high.DLT"})
  public void logInput(ConsumerRecord<Integer, Input> record) {
    LOGGER.info("{} - {}", record.topic(), record.value().getMessage());
  }

  @KafkaListener(
      id = "OutputLogger",
      topics = {"output-left", "output-right"},
      properties = "isolation.level: read_committed")
  public void logOutput(ConsumerRecord<Integer, Output> record) {
    LOGGER.info("{} - {}", record.topic(), record.value().getMessage());
  }
}
