package com.github.tomboyo.silverbroccoli;

import com.github.tomboyo.silverbroccoli.txn.AuditLogRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.backoff.FixedBackOff;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

@Profile("ft")
@Configuration
public class FaultTolerantHandlers {

  private static final Logger LOGGER = LoggerFactory.getLogger(FaultTolerantHandlers.class);

  @Bean
  public NewTopic inputTopic() {
    return TopicBuilder.name("input").partitions(1).replicas(1).build();
  }

  @Bean
  public NewTopic inputDltTopic() {
    return TopicBuilder.name("input.DLT").partitions(1).build();
  }

  @Bean
  public NewTopic leftTopic() {
    return TopicBuilder.name("left").partitions(1).build();
  }

  @Bean
  public NewTopic rightTopic() {
    return TopicBuilder.name("right").partitions(1).build();
  }

  @Bean
  public ApplicationRunner runner(KafkaTemplate<byte[], byte[]> template) {
    return args -> {
      LOGGER.info("Producing messages to input...");
      template.send("input", "pass-1".getBytes());
      template.send("input", "fail-1".getBytes());
      template.send("input", "pass-2".getBytes());
      template.send("input", "fail-2".getBytes());
      LOGGER.info("Produced input.");
    };
  }

  @Bean
  ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> customizer(
      BinderFactory binders) {
    return (container, dest, group) -> {
      ProducerFactory<byte[], byte[]> pf =
          ((KafkaMessageChannelBinder) binders.getBinder(null, MessageChannel.class))
              .getTransactionalProducerFactory();
      KafkaTemplate<byte[], byte[]> template = new KafkaTemplate<>(requireNonNull(pf));
      container.setAfterRollbackProcessor(
          new DefaultAfterRollbackProcessor<>(
              new DeadLetterPublishingRecoverer(template), new FixedBackOff(2000L, 2L)));
    };
  }

  @Bean
  public Consumer<String> persistAndSplit(TransactedUnitOfWork tx) {
    // Our Kafka transaction (started by the Kafka Listener Container automatically) will not roll
    // back our database operation; we need a database transaction. Therefore, we need to inject a
    // bean with our unit of work so that Spring has a chance to intercept that bean and wrap it
    // with a transactional interceptor. We can't just annotate this s.c.function bean definition
    // with @Transactional, because it won't wrap the consumer function.
    return tx::run;
  }

  @Component
  public static class TransactedUnitOfWork {

    private final StreamBridge bridge;
    private final AuditLogRepository repository;

    @Autowired
    public TransactedUnitOfWork(StreamBridge bridge, AuditLogRepository repository) {
      this.bridge = bridge;
      this.repository = repository;
    }

    @Transactional
    public void run(String input) {
      bridge.send("left", ("left-" + input).getBytes());
      repository.createIfNotExists(input);

      if (input.startsWith("fail")) {
        throw new RuntimeException("Simulated error");
      }

      bridge.send("right", ("right-" + input).getBytes());
    }
  }

  @Bean
  public Consumer<Message<String>> logger() {
    return message -> {
      var receivedTopic = message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC);
      LOGGER.info("Received on topic=" + receivedTopic + " payload=" + message.getPayload());
    };
  }
}
