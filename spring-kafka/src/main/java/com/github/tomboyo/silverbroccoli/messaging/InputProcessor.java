package com.github.tomboyo.silverbroccoli.messaging;

import com.github.tomboyo.silverbroccoli.auditlog.AuditLogRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InputProcessor {

  private final KafkaTemplate<Integer, Output> template;
  private final AuditLogRepository repository;

  @Autowired
  public InputProcessor(KafkaTemplate<Integer, Output> template, AuditLogRepository repository) {
    this.template = template;
    this.repository = repository;
  }

  @KafkaListener(id = "InputProcessor", topics = "input-high", batch = "true")
  public void processInputs(List<ConsumerRecord<Integer, Input>> inputs) {
    for (var input : inputs) {
      var key = input.key();
      var message = input.value().getMessage();
      var output = new Output().setMessage(message);

      template.send("output-left", key, output);
      repository.createIfNotExists(message);
      //      if (message.contains("ERROR")) {
      //        throw new RuntimeException("Simulated processing error!");
      //      }
      template.send("output-right", key, output);
    }
  }
}
