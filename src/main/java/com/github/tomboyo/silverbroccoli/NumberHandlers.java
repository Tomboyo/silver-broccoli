package com.github.tomboyo.silverbroccoli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Configuration
@Profile("numbers")
public class NumberHandlers {
  private static final Logger LOGGER = LoggerFactory.getLogger(NumberHandlers.class);

  // tag::generate-message-with-supplier[]
  @Bean
  public Supplier<Integer> producer() {
    var counter = new AtomicInteger();
    return () -> {
      var n = counter.getAndIncrement();
      LOGGER.info("Producing number: " + n);
      return n;
    };
  }
  // end::generate-message-with-supplier[]

  // tag::transform-message-with-function[]
  @Bean
  public Function<Integer, NumberMessage> transformer() {
    return it -> {
      LOGGER.info("Structuring number: " + it);
      return new NumberMessage(it);
    };
  }
  // end::transform-message-with-function[]

  // tag::consume-message-with-consumer[]
  @Bean
  public Consumer<NumberMessage> consumer() {
    return it -> LOGGER.info("Consuming number: " + it);
  }
  // end::consume-message-with-consumer[]

  @Bean
  public Consumer<Message<NumberMessage>> debugConsumer() {
    return message -> {
      var headers = message.getHeaders();
      var partition = headers.get(KafkaHeaders.RECEIVED_PARTITION_ID);
      var consumerGroup = headers.get(KafkaHeaders.GROUP_ID);
      var payload = message.getPayload();
      LOGGER.info(
          String.format(
              "%nPartition=%s%nThread=%s%nConsumerGroup=%s%nPayload=%s",
              partition, Thread.currentThread().getName(), consumerGroup, payload));
    };
  }
}
