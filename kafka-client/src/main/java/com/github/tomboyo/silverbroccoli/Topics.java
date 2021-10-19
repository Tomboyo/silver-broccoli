package com.github.tomboyo.silverbroccoli;

import com.github.tomboyo.silverbroccoli.kafka.JacksonObjectSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.tomboyo.silverbroccoli.KafkaConfiguration.commonKafkaConfig;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;

@Configuration
public class Topics {

  private static final Logger LOGGER = LoggerFactory.getLogger(Topics.class);

  /** The names of all topics created by this application */
  public static Stream<String> topicNames() {
    return topics().stream().map(NewTopic::name);
  }

  private static List<NewTopic> topics() {
    return List.of(
        new NewTopic("input-high", Optional.of(2), Optional.empty())
            .configs(
                Map.of(
                    RETENTION_MS_CONFIG, "60000",
                    RETENTION_BYTES_CONFIG, "1024")),
        new NewTopic("input-low", Optional.of(2), Optional.empty()),
        new NewTopic("input-high.DLT", Optional.empty(), Optional.empty()),
        new NewTopic("input-low.DLT", Optional.empty(), Optional.empty()),
        new NewTopic("left", Optional.empty(), Optional.empty()),
        new NewTopic("right", Optional.empty(), Optional.empty()));
  }

  private static KafkaProducer<String, Object> createKafkaProducer(Environment env) {
    var kafkaProps = new Properties();
    kafkaProps.putAll(commonKafkaConfig(env));
    kafkaProps.putAll(
        Map.of(
            KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            VALUE_SERIALIZER_CLASS_CONFIG, JacksonObjectSerializer.class.getName()));
    return new KafkaProducer<>(kafkaProps);
  }

  public static void initializeTopics(Environment env) {
    createTopics(
        LOGGER,
        env.getProperty("recreate-topics", Boolean.class, false),
        AdminClient.create(commonKafkaConfig(env)),
        topics());

    produceMessages(
        List.of("input-high", "input-low"), 10, createKafkaProducer(env), loggingCallback());
  }

  public static void createTopics(
      Logger logger, boolean recreateTopics, AdminClient adminClient, List<NewTopic> topics) {
    // NOTE: deleting topics may partially fail, and topic deletion may take time to propagate. This
    // sometimes results in an incomplete list of topics which breaks producers/consumers. This is
    // currently a low-effort impl to speed up local development some.
    if (recreateTopics) {
      logger.info("Deleting topics");
      adminClient.deleteTopics(topics.stream().map(NewTopic::name).collect(Collectors.toList()));
    }

    logger.info("Creating topics");
    adminClient.createTopics(topics);
    logger.info("Topics created. Closing admin client.");
    adminClient.close(Duration.ofMinutes(1));
  }

  public static void produceMessages(
      List<String> topics,
      int numMessagesPerTopic,
      KafkaProducer<String, Object> producer,
      Callback producerCallback) {
    topics.forEach(
        topic -> {
          IntStream.range(0, numMessagesPerTopic)
              .mapToObj(
                  n -> {
                    if (n % 2 == 0) {
                      return "PASS-" + topic + "-" + n;
                    } else {
                      return "FAIL-" + topic + "-" + n;
                    }
                  })
              .peek(m -> LOGGER.info("Producing message={}", m))
              .map(new Event()::message)
              // We use the message as a key to distribute messages from our small batch among
              // partitions. The default round-robin doesn't mix messages up much on this scale.
              .map(event -> new ProducerRecord<String, Object>(topic, event.getMessage(), event))
              .forEach(
                  record -> {
                    try {
                      producer.send(record).get();
                    } catch (Exception e) {
                      LOGGER.error("Failed to produce message", e);
                    }
                  });
        });
  }

  private static Callback loggingCallback() {
    return (meta, e) -> {
      if (e != null) {
        LOGGER.error("Failed to produce message", e);
      }
    };
  }
}
