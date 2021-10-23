package com.github.tomboyo.silverbroccoli;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
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
  public static ApplicationRunner runner(
      Environment env,
      AdminClient adminClient,
      KafkaProducer<String, Object> producer,
      AuditLogRepository repository) {
    return (_args) -> {
      Topics.initializeTopics(env, adminClient, producer);
      EventLoggers.initialize(env);
      Auditors.initialize(env, producer, repository);
    };
  }
}
