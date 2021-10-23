package com.github.tomboyo.silverbroccoli;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.List;

import static com.github.tomboyo.silverbroccoli.ConfigurationSupport.composeConfigs;
import static com.github.tomboyo.silverbroccoli.ConfigurationSupport.extractMap;

@Configuration
public class KafkaConfiguration {

  @Bean
  public static AdminClient defaultAdminClient(Environment env) {
    return AdminClient.create(extractMap(env, "sb.kafka.common", kafkaPropertyNames()));
  }

  @Bean
  public static KafkaProducer<String, Object> defaultKafkaProducer(Environment env) {
    var common = extractMap(env, "sb.kafka.common", kafkaPropertyNames());
    var producer = extractMap(env, "sb.kafka.producer", kafkaPropertyNames());
    var combined = composeConfigs(List.of(common, producer));
    return new KafkaProducer<>(combined);
  }

  public static List<String> kafkaPropertyNames() {
    // Spring doesn't provide a safe way to enumerate all key-value property pairs, so we create a
    // list of all the properties we (might) use which can be used to extract properties.
    return List.of(
        "bootstrap.servers",
        "security.protocol",
        "sasl.mechanism",
        "sasl.login.callback.handler.class",
        "sasl.jaas.config",
        "key.serializer",
        "value.serializer",
        "auto.offset.reset",
        "enable.auto.commit",
        "group.id",
        "partition.assignment.strategy",
        "key.deserializer",
        "value.deserializer");
  }
}
