package com.github.tomboyo.silverbroccoli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.Message;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.String.format;

@Configuration
@Profile("error-prone-numbers")
public class ErrorProneHandlers {

  private static final Logger LOGGER = LoggerFactory.getLogger(ErrorProneHandlers.class);

  @Bean
  public Supplier<Integer> errorProneSupplier() {
    // This will produce the number 1, then nothing else.
    var counter = new AtomicInteger();
    return () -> {
      var n = counter.getAndIncrement();
      LOGGER.info("Producing: n=" + n);
      return n;
    };
  }

  @Bean
  public Consumer<Integer> errorProneConsumer() {
    var failCounter = new AtomicInteger();
    var lastMessage = new AtomicInteger(-1);
    return n -> {
      if (lastMessage.get() != n) {
        lastMessage.set(n);
        failCounter.set(0);
      }
      var attempt = failCounter.incrementAndGet();
      LOGGER.info(format("Failed to process message: n=%s attempt=%s", n, attempt));
      throw new RuntimeException("Kaboom! Failed to process message: n=" + n);
    };
  }

  @Bean
  public Consumer<Message<Integer>> debugErrorProneConsumer() {
    return (m) -> LOGGER.info("Found DLQ message: message=" + m);
  };
}
