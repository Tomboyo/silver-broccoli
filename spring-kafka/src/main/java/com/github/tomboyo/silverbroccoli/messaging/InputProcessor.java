package com.github.tomboyo.silverbroccoli.messaging;

import com.github.tomboyo.silverbroccoli.auditlog.AuditLogRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InputProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(InputProcessor.class);

  private final KafkaTemplate<Integer, Object> template;
  private final AuditLogRepository repository;

  @Autowired
  public InputProcessor(KafkaTemplate<Integer, Object> template, AuditLogRepository repository) {
    this.template = template;
    this.repository = repository;
  }

  @KafkaListener(id = "InputProcessorHigh", topics = "input-high", concurrency = "3")
  public void inputHighProcessor(ConsumerRecord<Integer, Input> input) {
    processInputs(input);
  }

  @KafkaListener(id = "InputProcessorLow", topics = "input-low", concurrency = "1")
  public void inputLowProcessor(ConsumerRecord<Integer, Input> input) {
    processInputs(input);
  }

  // Kafka transaction is started by the kafka container.
  public void processInputs(ConsumerRecord<Integer, Input> input) {
    var key = input.key();
    var message = input.value().getMessage();
    var output = new Output().setMessage(message);

    LOGGER.info("Processing {}", message);
    template.send("output-left", key, output);
    repository.createIfNotExists(message); // transactional; idempotent.
    template.send("output-right", key, output);

    if (message.contains("ERROR")) {
      throw new RuntimeException("Simulated processing error!");
    }
  }
}
