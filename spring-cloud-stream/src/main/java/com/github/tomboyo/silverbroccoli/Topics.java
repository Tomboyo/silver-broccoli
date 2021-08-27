package com.github.tomboyo.silverbroccoli;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * This is using Spring Kafka. The S.C.S. approach is to add properties to the
 * s.c.s.bindings.mybinder-x-y.consumer or producer, but topics tend to have both producers and
 * consumers, so you wind up with confusing asymmetric configuration. Use Spring Kafka.
 */
@Configuration
public class Topics {

  // Do not name your NewTopic bean the same as the topic it creates. Spring will produce errors
  // claiming that the bean named for the topic is not of type 'SubscribableChannel'. I append Topic
  // to every bean name for that reason.

  @Bean
  public NewTopic inputHighTopic() {
    return TopicBuilder.name("input-high")
        .partitions(3)
        .replicas(2)
        .config(TopicConfig.RETENTION_MS_CONFIG, "60000")
        .build();
  }

  @Bean
  public NewTopic inputLowTopic() {
    return TopicBuilder.name("input-low")
        .partitions(3)
        .replicas(2)
        .config(TopicConfig.RETENTION_MS_CONFIG, "60000")
        .build();
  }

  @Bean
  public NewTopic inputHighDltTopic() {
    return TopicBuilder.name("input-high.DLT")
        .partitions(3)
        .replicas(2)
        .config(TopicConfig.RETENTION_MS_CONFIG, "60000")
        .build();
  }

  @Bean
  public NewTopic inputLowDltTopic() {
    return TopicBuilder.name("input-low.DLT")
        .partitions(3)
        .replicas(2)
        .config(TopicConfig.RETENTION_MS_CONFIG, "60000")
        .build();
  }

  @Bean
  public NewTopic leftTopic() {
    return TopicBuilder.name("left")
        .partitions(1)
        .replicas(1)
        .config(TopicConfig.RETENTION_MS_CONFIG, "60000")
        .build();
  }

  @Bean
  public NewTopic rightTopic() {
    return TopicBuilder.name("right")
        .partitions(1)
        .replicas(1)
        .config(TopicConfig.RETENTION_MS_CONFIG, "60000")
        .build();
  }
}
