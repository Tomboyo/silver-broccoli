package com.github.tomboyo.silverbroccoli;

import org.springframework.core.env.Environment;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaConfiguration {

  /** Map of configurations shared by producers and consumers. */
  public static Map<String, Object> commonKafkaConfig(Environment env) {
    return Stream.of(
            "bootstrap.servers",
            "security.protocol",
            "sasl.mechanism",
            "sasl.login.callback.handler.class",
            "sasl.jaas.config")
        .collect(
            Collectors.toMap(
                Function.identity(), key -> env.getRequiredProperty("sb.kafka-client." + key)));
  }
}
