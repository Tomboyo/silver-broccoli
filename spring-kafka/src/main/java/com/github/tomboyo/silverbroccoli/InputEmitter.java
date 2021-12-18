package com.github.tomboyo.silverbroccoli;

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
  private final KafkaTemplate<byte[], Input> template;

  @Autowired
  public InputEmitter(KafkaTemplate<byte[], Input> template) {
    this.template = template;
  }

  // Generate messages like OK-3 or ERROR-5 to both input topics.
  @Scheduled(fixedRate = 1, timeUnit = SECONDS)
  @Transactional
  public void emit() {
    var count = counter.incrementAndGet();
    var topic = count % 2 == 0 ? "input-high" : "input-low";
    var message = (count % 5 == 0 ? "OK" : "ERROR") + "-" + count;
    template.send(topic, new Input().setKey(count).setMessage(message));
  }
}
