package com.github.tomboyo.silverbroccoli;

import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.backoff.FixedBackOff;

import static java.util.Objects.requireNonNull;

@Configuration
public class Container {
  @Bean
  ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> customizer(
      BinderFactory binders) {
    return (container, dest, group) -> {
      // Configure the container to start kafka transactions for all producers. After three failed
      // attempts, deliver messages to a DLT.
      var transactionalTemplate =
          new KafkaTemplate<>(
              requireNonNull(
                  ((KafkaMessageChannelBinder) binders.getBinder(null, MessageChannel.class))
                      .getTransactionalProducerFactory()));
      container.setAfterRollbackProcessor(
          new DefaultAfterRollbackProcessor<>(
              new DeadLetterPublishingRecoverer(transactionalTemplate),
              new FixedBackOff(2000L, 2L)));
    };
  }
}
