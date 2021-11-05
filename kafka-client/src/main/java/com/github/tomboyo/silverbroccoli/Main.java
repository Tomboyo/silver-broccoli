package com.github.tomboyo.silverbroccoli;

import com.github.tomboyo.silverbroccoli.kafka.BoundedRetryBatchConsumerProperties;
import com.github.tomboyo.silverbroccoli.kafka.CommonProperties;
import com.github.tomboyo.silverbroccoli.kafka.TransactionalBoundedRetryConsumerProperties;
import com.github.tomboyo.silverbroccoli.processors.auditors.Auditors;
import com.github.tomboyo.silverbroccoli.processors.auditors.HighPriorityAuditors;
import com.github.tomboyo.silverbroccoli.processors.auditors.LowPriorityAuditors;
import com.github.tomboyo.silverbroccoli.processors.loggers.DltLoggers;
import com.github.tomboyo.silverbroccoli.processors.loggers.LeftRightLoggers;
import com.github.tomboyo.silverbroccoli.processors.loggers.Loggers;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Main {
  public static void main(String[] args) {
    SpringApplication.run(Main.class, args);
  }

  @Bean
  public static ApplicationRunner runner(
      Environment env,
      AuditLogRepository repository,
      CommonProperties commonProperties,
      @HighPriorityAuditors TransactionalBoundedRetryConsumerProperties highPriorityAuditors,
      @LowPriorityAuditors TransactionalBoundedRetryConsumerProperties lowPriorityAuditors,
      @LeftRightLoggers BoundedRetryBatchConsumerProperties leftRightLoggers,
      @DltLoggers BoundedRetryBatchConsumerProperties dltLoggers) {
    return (_args) -> {
      Topics.initializeTopics(env, commonProperties);
      Loggers.initialize(commonProperties, leftRightLoggers);
      Loggers.initialize(commonProperties, dltLoggers);
      Auditors.initialize(commonProperties, highPriorityAuditors, lowPriorityAuditors, repository);
    };
  }
}
