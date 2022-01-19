package com.github.tomboyo.silverbroccoli.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicsConfiguration {

  @Bean
  public NewTopic inputLow() {
    return TopicBuilder.name("input-low").config(TopicConfig.RETENTION_MS_CONFIG, "10000").build();
  }

  @Bean
  public NewTopic inputLowDlt() {
    return TopicBuilder.name("input-low.DLT")
        .config(TopicConfig.RETENTION_MS_CONFIG, "300000")
        .build();
  }

  @Bean
  public NewTopic inputHigh() {
    return TopicBuilder.name("input-high").config(TopicConfig.RETENTION_MS_CONFIG, "10000").build();
  }

  @Bean
  public NewTopic inputHighDlt() {
    return TopicBuilder.name("input-high.DLT")
        .config(TopicConfig.RETENTION_MS_CONFIG, "300000")
        .build();
  }

  @Bean
  public NewTopic outputLeft() {
    return TopicBuilder.name("output-left")
        .config(TopicConfig.RETENTION_MS_CONFIG, "10000")
        .build();
  }

  @Bean
  public NewTopic outputRight() {
    return TopicBuilder.name("output-right")
        .config(TopicConfig.RETENTION_MS_CONFIG, "10000")
        .build();
  }
}
