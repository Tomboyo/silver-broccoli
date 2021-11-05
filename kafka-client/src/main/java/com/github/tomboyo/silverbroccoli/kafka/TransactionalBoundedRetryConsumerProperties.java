package com.github.tomboyo.silverbroccoli.kafka;

import org.springframework.boot.context.properties.ConstructorBinding;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ConstructorBinding
public class TransactionalBoundedRetryConsumerProperties {
  private List<String> topics;
  private String dlt;
  private int maxAttempts = 3;
  private Duration pollTimeout = Duration.ofMillis(100);
  private Map<String, String> kafkaConsumer;
  private Map<String, String> kafkaProducer;

  public List<String> getTopics() {
    return topics;
  }

  public void setTopics(List<String> topics) {
    this.topics = topics;
  }

  public Optional<String> getDlt() {
    return Optional.ofNullable(dlt);
  }

  public void setDlt(String dlt) {
    this.dlt = dlt;
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public void setMaxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
  }

  public Duration getPollTimeout() {
    return pollTimeout;
  }

  public void setPollTimeout(Duration pollTimeout) {
    this.pollTimeout = pollTimeout;
  }

  public Map<String, Object> getKafkaConsumerConfig() {
    return new HashMap<>(kafkaConsumer);
  }

  public void setKafkaConsumer(Map<String, String> kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
  }

  public Map<String, Object> getKafkaProducerConfig() {
    return new HashMap<>(kafkaProducer);
  }

  public void setKafkaProducer(Map<String, String> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }

  @Override
  public String toString() {
    return "HighPriorityAuditorsConfig{"
        + " topics=\""
        + topics
        + "\" dlt=\""
        + dlt
        + "\" kafkaConsumer=\""
        + kafkaConsumer
        + "\" kafkaProducer=\""
        + kafkaProducer
        + "\" }";
  }
}
