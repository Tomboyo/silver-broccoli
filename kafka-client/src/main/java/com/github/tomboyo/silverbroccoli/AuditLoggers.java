package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomboyo.silverbroccoli.kafka.BatchConsumer;
import com.github.tomboyo.silverbroccoli.kafka.JacksonObjectSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.github.tomboyo.silverbroccoli.KafkaConfiguration.commonKafkaConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class AuditLoggers {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuditLoggers.class);
  private static final int MAX_RECORDS_PER_POLL = 20;

  // TODO: common consumer configuration
  public static final ObjectMapper DEFAULT_MAPPER =
      new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  // TODO: common consumer configuration
  private static Map<String, Object> myConsumerConfig(Environment env) {
    var config = new HashMap<String, Object>();
    config.putAll(commonKafkaConfig(env));
    config.putAll(
        Map.of(
            AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ENABLE_AUTO_COMMIT_CONFIG,
            "false",
            GROUP_ID_CONFIG,
            "AuditLoggers",
            MAX_POLL_RECORDS_CONFIG,
            Integer.toString(MAX_RECORDS_PER_POLL),
            PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            RoundRobinAssignor.class.getName(),
            KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName()));
    return config;
  }

  // TODO: duplication
  private static KafkaProducer<String, Object> createKafkaProducer(Environment env) {
    var kafkaProps = new Properties();
    kafkaProps.putAll(commonKafkaConfig(env));
    kafkaProps.putAll(
        Map.of(
            KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            VALUE_SERIALIZER_CLASS_CONFIG, JacksonObjectSerializer.class.getName()));
    return new KafkaProducer<>(kafkaProps);
  }

  public static void initializeAuditLoggers(Environment env, AuditLogRepository repository) {
    var highWorkers = env.getProperty("sb.audit-loggers.workers", "3");
    var lowWorkers = env.getProperty("sb.audit-loggers.workers", "2");
    LOGGER.info("Starting audit workers: high={} low={}", highWorkers, lowWorkers);

    // Producers are thread safe.
    var sharedKafkaProducer = createKafkaProducer(env);

    // Start input-high consumer and worker pool
    var highConsumerConf = myConsumerConfig(env);
    highConsumerConf.putAll(
        Map.of(BatchConsumer.TOPICS_CONF, "input-high", BatchConsumer.WORKERS_CONF, highWorkers));
    BatchConsumer.<String, byte[]>start(
        highConsumerConf, (record) -> handle(record, sharedKafkaProducer, repository));

    // Start input-low consumer and worker pool
    var lowConsumerConf = myConsumerConfig(env);
    lowConsumerConf.putAll(
        Map.of(BatchConsumer.TOPICS_CONF, "input-low", BatchConsumer.WORKERS_CONF, lowWorkers));
    BatchConsumer.<String, byte[]>start(
        lowConsumerConf, (record) -> handle(record, sharedKafkaProducer, repository));
  }

  private static void handle(
      ConsumerRecord<String, byte[]> record,
      KafkaProducer<String, Object> kafkaProducer,
      AuditLogRepository repository) {
    var message = record.value();
    Event event = null;
    try {
      event = DEFAULT_MAPPER.readValue(message, Event.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deser event", e);
    }
    LOGGER.info("Processing: event={}", event);

    // TODO: transaction for producer!
    kafkaProducer.send(
        new ProducerRecord<>("left", new Event().message("LEFT: " + event.getMessage())));
    repository.createIfNotExists(event.getMessage());

    //    if (event.getMessage().toLowerCase().startsWith("fail")) {
    //      throw new RuntimeException("Simulated failure!");
    //    }

    kafkaProducer.send(
        new ProducerRecord<>("right", new Event().message("RIGHT: " + event.getMessage())),
        loggingCallback());
  }

  // TODO: duplication
  private static Callback loggingCallback() {
    return (meta, e) -> {
      if (e != null) {
        LOGGER.error("Failed to produce message", e);
      }
    };
  }
}
