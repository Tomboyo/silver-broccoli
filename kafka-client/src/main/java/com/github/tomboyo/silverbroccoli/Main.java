package com.github.tomboyo.silverbroccoli;

import com.github.tomboyo.silverbroccoli.kafka.JacksonObjectSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;

@SpringBootApplication
public class Main {
  public static void main(String[] args) {
    SpringApplication.run(Main.class, args);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  /** Configures shared consumer + producer connection properties */
  private static Map<String, Object> commonConfig(Environment env) {
    return Stream.of(
            "bootstrap.servers",
            "security.protocol",
            "sasl.mechanism",
            "sasl.login.callback.handler.class",
            "sasl.jaas.config")
        .collect(
            Collectors.toMap(
                Function.identity(), key -> env.getRequiredProperty("sb.kafka-client." + key)));
  }

  @Bean
  public static KafkaProducer<String, Object> producer(Environment env) {
    var kafkaProps = new Properties();
    kafkaProps.putAll(commonConfig(env));
    kafkaProps.putAll(
        Map.of(
            KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            VALUE_SERIALIZER_CLASS_CONFIG, JacksonObjectSerializer.class.getName()));
    return new KafkaProducer<>(kafkaProps);
  }

  @Bean
  public ApplicationRunner consumerInitializer(Environment env) {
    final var numConsumers = 3;
    var consumers =
        Stream.generate(() -> EventConsumers.createKafkaConsumer(commonConfig(env)))
            .limit(numConsumers)
            .collect(Collectors.toList());
    var executor = Executors.newFixedThreadPool(numConsumers);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  consumers.forEach(KafkaConsumer::wakeup);
                  executor.shutdown();
                }));

    return (_x) ->
        consumers.forEach(
            consumer ->
                executor.submit(() -> EventConsumers.run(consumer, EventConsumers.DEFAULT_MAPPER)));
  }

  @Bean
  public ApplicationRunner topicInitializer(
      Environment env, KafkaProducer<String, Object> producer) {
    var adminClient = AdminClient.create(commonConfig(env));

    return (_x) -> {
      if (env.getProperty("recreate-topics", Boolean.class, false)) {
        LOGGER.info("Deleting topics");
        adminClient.deleteTopics(
            List.of("input-high", "input-low", "input-high.DLT", "input-low.DLT", "left", "right"));
      }

      LOGGER.info("Creating topics");
      adminClient.createTopics(
          List.of(
              new NewTopic("input-high", Optional.empty(), Optional.empty())
                  .configs(
                      Map.of(
                          RETENTION_MS_CONFIG, "60000",
                          RETENTION_BYTES_CONFIG, "1024")),
              new NewTopic("input-low", Optional.empty(), Optional.empty()),
              new NewTopic("input-high.DLT", Optional.empty(), Optional.empty()),
              new NewTopic("input-low.DLT", Optional.empty(), Optional.empty()),
              new NewTopic("left", Optional.empty(), Optional.empty()),
              new NewTopic("right", Optional.empty(), Optional.empty())));
      LOGGER.info("Topics created. Closing admin client.");
      adminClient.close(Duration.ofMinutes(1));

      Stream.of("PASS-1", "FAIL-2", "PASS-3", "FAIL-4")
          .map(message -> new Event().message(message))
          .forEach(
              event -> {
                LOGGER.info("Producing '{}'", event);
                try {
                  producer.send(new ProducerRecord<>("input-high", event), loggingCallback());
                } catch (Exception e) {
                  throw new RuntimeException("Failed to produce message", e);
                }
              });
    };
  }

  private Callback loggingCallback() {
    return (meta, e) -> {
      if (e != null) {
        LOGGER.error("Failed to produce message", e);
      } else {
        LOGGER.info("offset={} partition={}", meta.offset(), meta.partition());
      }
    };
  }
}
