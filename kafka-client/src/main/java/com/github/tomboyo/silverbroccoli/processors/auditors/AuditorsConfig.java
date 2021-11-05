package com.github.tomboyo.silverbroccoli.processors.auditors;

import com.github.tomboyo.silverbroccoli.kafka.TransactionalBoundedRetryConsumerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AuditorsConfig {
  @Bean
  @HighPriorityAuditors
  @ConfigurationProperties(prefix = "sb.auditors.high.priority")
  public TransactionalBoundedRetryConsumerProperties highPriorityAuditors() {
    return new TransactionalBoundedRetryConsumerProperties();
  }

  @Bean
  @LowPriorityAuditors
  @ConfigurationProperties("sb.auditors.low.priority")
  public TransactionalBoundedRetryConsumerProperties lowPriorityAuditors() {
    return new TransactionalBoundedRetryConsumerProperties();
  }
}
