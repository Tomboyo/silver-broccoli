package com.github.tomboyo.silverbroccoli.txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.annotation.Transactional;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Profile("transactional")
@Configuration
public class TransactionalHandlers {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalHandlers.class);

  @Bean
  @Transactional
  public Supplier<Integer> supplier(AuditLogRepository repository) {
    var counter = new AtomicInteger();
    var random = new Random();

    return () -> {
      var n = counter.get();
      LOGGER.info("Producing n=" + n);

      repository.createIfNotExists("Produced n=" + n);

      if (random.nextDouble() < 0.1) {
        // Simulated failure should cause rollback of the db transaction, and prevent message
        // publish.
        LOGGER.error("Simulated producer failure on n=" + n);
        throw new RuntimeException("Producer failure on n=" + n);
      }

      counter.incrementAndGet();
      return n;
    };
  }

  @Bean
  public Consumer<Integer> supplierConfirmation() {
    return n -> LOGGER.info("Confirming production of n=" + n);
  }

  @Bean
  @Transactional
  public Function<Integer, String> function(AuditLogRepository repository) {
    var random = new Random();

    return n -> {
      LOGGER.info("Transforming n=" + n);
      var result = "{ \"n\": \"" + n + "\" }";

      repository.createIfNotExists("Transformed n=" + n);

      if (random.nextDouble() < 0.3) {
        LOGGER.error("Transformer failure on n=" + n);
        throw new RuntimeException("Transformer failure on n=" + n);
      }

      return result;
    };
  }

  @Bean
  public Consumer<Integer> dlqLogger(AuditLogRepository repository) {
    return n -> {
      LOGGER.info("DLQ n=" + n);
      repository.createIfNotExists("DLQ'd n=" + n);
    };
  }

  @Bean
  public Consumer<String> functionConfirmation() {
    return it -> LOGGER.info("Confirming transformation to message=" + it);
  }
}
