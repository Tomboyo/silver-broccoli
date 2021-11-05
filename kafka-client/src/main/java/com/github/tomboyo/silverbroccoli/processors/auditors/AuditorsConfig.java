package com.github.tomboyo.silverbroccoli.processors.auditors;

import com.github.tomboyo.silverbroccoli.kafka.BoundedRetryBatchConsumerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AuditorsConfig {
  @Bean
  @HighPriorityAuditors
  @ConfigurationProperties(prefix = "sb.auditors.high.priority")
  public BoundedRetryBatchConsumerProperties highPriorityAuditors() {
    return new BoundedRetryBatchConsumerProperties();
  }

  @Bean
  @LowPriorityAuditors
  @ConfigurationProperties("sb.auditors.low.priority")
  public BoundedRetryBatchConsumerProperties lowPriorityAuditors() {
    return new BoundedRetryBatchConsumerProperties();
  }
}
