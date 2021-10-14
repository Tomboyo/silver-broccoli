package com.github.tomboyo.silverbroccoli;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@SpringBootApplication
public class Main {
  public static void main(String[] args) {
    SpringApplication.run(Main.class, args);
  }

  @Bean
  public static ApplicationRunner runner(Environment env) {
    return (_args) -> {
      Topics.initializeTopics(env);
      EventLoggers.initializeEventLoggers(env);
    };
  }
}
