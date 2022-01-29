package com.github.tomboyo.silverbroccoli.messaging;

import org.apache.kafka.clients.admin.Admin;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableScheduling
@EnableKafka
public class KafkaConfiguration {

  @Bean
  public Admin admin(KafkaAdmin kafkaAdmin) {
    return Admin.create(kafkaAdmin.getConfigurationProperties());
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
      ConcurrentKafkaListenerContainerFactoryConfigurer bootConfigurer,
      ConsumerFactory<Object, Object> consumerFactory,
      KafkaTemplate<Object, Object> template) {
    var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();

    // Apply default spring-boot configuration.
    bootConfigurer.configure(factory, consumerFactory);

    factory.setContainerCustomizer(
        container -> {
          var groupId = container.getContainerProperties().getGroupId();
          if (groupId.equals("InputProcessorHigh") || groupId.equals("InputProcessorLow")) {
            container.setAfterRollbackProcessor(
                new DefaultAfterRollbackProcessor<>(
                    new DeadLetterPublishingRecoverer(template), new FixedBackOff(1_000, 2)));
          }
        });
    return factory;
  }
}
