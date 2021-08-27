package com.github.tomboyo.silverbroccoli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.function.Consumer;

@Configuration
public class Bindings {

  private static final Logger LOGGER = LoggerFactory.getLogger(Bindings.class);

  @Bean
  public ApplicationRunner runner(StreamBridge bridge) {
    return (_args) -> {
      LOGGER.info("Producing events...");
      bridge.send("input-low", new Event().message("FAIL-1"));
      bridge.send("input-low", new Event().message("PASS-1"));
      bridge.send("input-low", new Event().message("FAIL-2"));
      bridge.send("input-low", new Event().message("PASS-2"));

      bridge.send("input-high", new Event().message("FAIL-3"));
      bridge.send("input-high", new Event().message("PASS-3"));
      bridge.send("input-high", new Event().message("FAIL-4"));
      bridge.send("input-high", new Event().message("PASS-4"));
      LOGGER.info("Done producing events.");
    };
  }

  @Bean
  public Consumer<Event> consumer(TransactedUnitOfWork work) {
    // We inject the unit of work so that spring can wrap it in a transactional interceptor. We
    // can't just return a function (although a TransactionTemplate might work. In either case, this
    // doesn't look like idiomatic Spring.)
    return work::run;
  }

  @Bean
  public Consumer<Message<Event>> logger() {
    return message -> {
      var topic = message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC);
      LOGGER.info("Topic=" + topic + " Payload=" + message.getPayload());
    };
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

    /**
     * Persist a record to the database idempotently, then atomically send two messages to th left
     * and right topics. Both operations are transactional, but not XA, so the DB transaction may
     * commit even though the Kafka transaction fails.
     *
     * @param event The event to process.
     */
    @Transactional
    public void run(Event event) {
      bridge.send("left", new Event().message("LEFT: " + event.getMessage()));
      repository.createIfNotExists(event.getMessage());

      // Simulate a failure to exercise transactions.
      if (event.getMessage().startsWith("FAIL")) {
        throw new RuntimeException("Kaboom! Can't process event=" + event);
      }

      bridge.send("right", new Event().message("RIGHT: " + event.getMessage()));
    }
  }
}
