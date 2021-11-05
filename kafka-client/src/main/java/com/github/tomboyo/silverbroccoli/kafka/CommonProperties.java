package com.github.tomboyo.silverbroccoli.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties("sb")
public class CommonProperties {
  private Map<String, String> kafkaCommon;

  public Map<String, Object> getKafkaCommon() {
    return new HashMap<>(kafkaCommon);
  }

  public void setKafkaCommon(Map<String, String> kafkaCommon) {
    this.kafkaCommon = kafkaCommon;
  }

  @Override
  public String toString() {
    return "CommonProperties{" + " kafkaCommon=\"" + kafkaCommon + "\" }";
  }
}
