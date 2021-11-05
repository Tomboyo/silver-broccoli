package com.github.tomboyo.silverbroccoli.processors.loggers;

import com.github.tomboyo.silverbroccoli.kafka.BoundedRetryBatchConsumerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LoggersConfig {

  @Bean
  @LeftRightLoggers
  @ConfigurationProperties("sb.loggers.leftright")
  public BoundedRetryBatchConsumerProperties leftRightLoggers() {
    return new BoundedRetryBatchConsumerProperties();
  }

  @Bean
  @DltLoggers
  @ConfigurationProperties("sb.loggers.dlt")
  public BoundedRetryBatchConsumerProperties dltLoggers() {
    return new BoundedRetryBatchConsumerProperties();
  }
}
