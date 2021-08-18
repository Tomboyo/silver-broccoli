package com.github.tomboyo.silverbroccoli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

@SpringBootApplication
public class SilverBroccoliApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(SilverBroccoliApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(SilverBroccoliApplication.class, args);
  }

  @Bean
  public Supplier<String> numberSupplier() {
    var counter = new AtomicInteger();
    return () -> {
      var n = counter.getAndIncrement();
      return String.valueOf(n);
    };
  }

  @Bean
  public Consumer<String> printer() {
    return it -> LOGGER.info("printer - " + it);
  }
}
