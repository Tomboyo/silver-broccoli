package com.github.tomboyo.silverbroccoli.messaging;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

@Component
public class InputEmitter {

  private final AtomicInteger counter = new AtomicInteger();
  private final KafkaTemplate<Integer, Input> template;

  @Autowired
  public InputEmitter(KafkaTemplate<Integer, Input> template) {
    this.template = template;
  }

  // Generate messages like OK-3 or ERROR-5 to both input topics.
  @Scheduled(fixedRate = 5, timeUnit = SECONDS)
  @Transactional("kafkaTransactionManager")
  public void emit() {
    var count = counter.getAndIncrement();
    // Alternate between the two topics
    var topic = count % 2 == 0 ? "input-high" : "input-low";
    // Deliver 2 OK, then 2 ERROR
    var message = (count % 4 == 0 || count % 4 == 1 ? "OK" : "ERROR") + "-" + count;
    template.send(topic, count, new Input().setMessage(message));
  }
}
