package com.github.tomboyo.silverbroccoli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.function.Consumer;
import java.util.function.Supplier;

@Profile("lag")
@Configuration
public class LagHandlers {

  private static final Logger LOGGER = LoggerFactory.getLogger(LagHandlers.class);

  @Bean
  public Supplier<Long> producer() {
    return () -> {
      var now = System.currentTimeMillis();
      LOGGER.info("Producer: now=" + now);
      return now;
    };
  }

  @Bean
  public Consumer<Long> consumer() {
    return now -> {
      LOGGER.info("Consumer: now=" + now);
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
  }
}
