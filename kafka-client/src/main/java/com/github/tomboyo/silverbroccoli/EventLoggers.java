package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.github.tomboyo.silverbroccoli.KafkaConfiguration.commonKafkaConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Configuration
public class EventLoggers {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventLoggers.class);

  public static final ObjectMapper DEFAULT_MAPPER =
      new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static KafkaConsumer<String, byte[]> createKafkaConsumer(Environment env) {
    var config = new HashMap<String, Object>();
    config.putAll(commonKafkaConfig(env));
    config.putAll(
        Map.of(
            AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ENABLE_AUTO_COMMIT_CONFIG,
            "false",
            GROUP_ID_CONFIG,
            "event-consumer-group-1",
            PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            RoundRobinAssignor.class.getName(),
            KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName()));
    return new KafkaConsumer<>(config);
  }

  @Bean
  @Order(1)
  public static ApplicationRunner eventLoggersInitializer(Environment env) {
    return (_args) ->
        startEventLoggers(
            env.getProperty("sb.event-loggers.consumers", Integer.class, 4),
            () -> createKafkaConsumer(env),
            Executors::newFixedThreadPool,
            Runtime.getRuntime());
  }

  /** Create an application runner responsible for starting EventLogger consumers. */
  public static void startEventLoggers(
      int numConsumers,
      Supplier<KafkaConsumer<String, byte[]>> consumerFactory,
      Function<Integer, ExecutorService> executorFactory,
      Runtime runtime) {
    var consumers =
        Stream.generate(consumerFactory).limit(numConsumers).collect(Collectors.toList());

    var executor = executorFactory.apply(numConsumers);

    runtime.addShutdownHook(
        new Thread(
            () -> {
              consumers.forEach(KafkaConsumer::wakeup);
              executor.shutdown();
            }));

    consumers.forEach(
        consumer -> executor.submit(eventLogger(LOGGER, consumer, EventLoggers.DEFAULT_MAPPER)));
  }

  public static Runnable eventLogger(
      Logger logger, KafkaConsumer<String, byte[]> kafkaConsumer, ObjectMapper mapper) {
    return () -> {
      logger.info("Event Logger is starting");
      kafkaConsumer.subscribe(List.of("input-high", "input-low"));

      try {
        while (true) {
          var records = kafkaConsumer.poll(Duration.ofMinutes(1));
          for (var record : records) {
            var topic = record.topic();
            var partition = record.partition();
            try {
              var event = mapper.readValue(record.value(), Event.class);
              logger.info("topic={} partition={} event={}", topic, partition, event);
            } catch (Exception e) {
              logger.warn("Failed to process event: topic={} partition={}", topic, partition, e);
            }
          }
          kafkaConsumer.commitAsync();
        }
      } catch (WakeupException e) {
        logger.info("Caught request to close kafka consumer");
        try {
          kafkaConsumer.commitSync();
        } catch (Exception e2) {
          logger.warn("Failed to commit before shutdown", e2);
        }
      } finally {
        try {
          kafkaConsumer.close();
          logger.info("Closed kafka consumer");
        } catch (KafkaException e) {
          logger.error("Exception while closing kafka consumer", e);
        }
      }
    };
  }
}
