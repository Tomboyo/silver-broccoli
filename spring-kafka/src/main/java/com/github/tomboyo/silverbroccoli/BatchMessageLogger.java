package com.github.tomboyo.silverbroccoli;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BatchMessageLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchMessageLogger.class);

  @KafkaListener(
      id = "batchMessageLogger",
      topics = {"input-high", "input-low", "output-left", "output-right"},
      batch = "true")
  public void logBatch(List<ConsumerRecord<byte[], Input>> consumerRecords) {
    consumerRecords.forEach(record -> LOGGER.info("{}", record));
  }
}
