package com.github.tomboyo.silverbroccoli;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class Main {
  public static void main(String[] args) {
    SpringApplication.run(Main.class, args);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  /** Configures shared consumer + producer connection properties */
  private static Consumer<Properties> configurer(Environment env) {
    return properties -> {
      Stream.of(
              "bootstrap.servers",
              "security.protocol",
              "sasl.mechanism",
              "sasl.login.callback.handler.class",
              "sasl.jaas.config")
          .forEach(key -> properties.put(key, env.getRequiredProperty("sb.kafka-client." + key)));
    };
  }

  @Bean
  public static KafkaProducer<String, String> producer(Environment env) {
    var kafkaProps = new Properties();
    configurer(env).accept(kafkaProps);

    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(kafkaProps);
  }

  @Bean
  public ApplicationRunner runner(Environment env, KafkaProducer<String, String> producer) {
    var kafkaProps = new Properties();
    configurer(env).accept(kafkaProps);
    var adminClient = AdminClient.create(kafkaProps);

    return (x) -> {
      LOGGER.info("Creating topics");
      adminClient.createTopics(List.of(new NewTopic("input", Optional.empty(), Optional.empty())));
      LOGGER.info("Topics created. Closing admin client.");
      adminClient.close(Duration.ofMinutes(1));

      Callback callback =
          (meta, e) -> {
            if (e != null) {
              LOGGER.error("Failed to produce message", e);
            } else {
              LOGGER.info("offset={} partition={}", meta.offset(), meta.partition());
            }
          };
      Stream.of("PASS-1", "FAIL-2", "PASS-3", "FAIL-4")
          .forEach(
              payload -> {
                LOGGER.info("Producing '{}'", payload);
                try {
                  producer.send(new ProducerRecord<>("input", "PASS-1"), callback);
                } catch (Exception e) {
                  throw new RuntimeException("Failed to produce message " + payload, e);
                }
              });
    };
  }
}
