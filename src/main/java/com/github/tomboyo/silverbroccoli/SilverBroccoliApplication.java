package com.github.tomboyo.silverbroccoli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer.ControlFlag.REQUIRED;

@SpringBootApplication
public class SilverBroccoliApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(SilverBroccoliApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(SilverBroccoliApplication.class, args);
  }

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
  @Profile("sasl")
  public KafkaJaasLoginModuleInitializer jaasConfig(Environment env) throws IOException {
    var module = new KafkaJaasLoginModuleInitializer();
    module.setLoginModule("org.apache.kafka.common.security.plain.PlainLoginModule");
    module.setControlFlag(REQUIRED);
    module.setOptions(
        Map.of(
            "username",
            env.getRequiredProperty("env.kafka.user"),
            "password",
            env.getRequiredProperty("env.kafka.password")));
    return module;
  }
}
