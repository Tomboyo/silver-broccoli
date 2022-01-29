package com.github.tomboyo.silverbroccoli.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.MultiGauge.Row;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

/**
 * Monitors consumer lag across all application (non-kafka-internal) topics and consumer groups.
 * Data are exported via a Micrometer multi-gauge named sb.kafka.consumer.lag.
 */
@Service
public class ConsumerLagService {

  private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(ConsumerLagService.class);

  private final Admin admin;
  private final Logger logger;
  private final MultiGauge lagGauges;
  private final AtomicReference<HashMap<String, HashMap<String, Long>>> lagPerGroupPerTopic;

  @Autowired
  public ConsumerLagService(Admin admin, MeterRegistry meterRegistry) {
    this(admin, meterRegistry, DEFAULT_LOGGER);
  }

  public ConsumerLagService(Admin admin, MeterRegistry meterRegistry, Logger logger) {
    this.admin = admin;
    this.logger = logger;
    lagPerGroupPerTopic = new AtomicReference<>();
    lagGauges =
        MultiGauge.builder("sb.kafka.consumer.lag")
            .tags("topic", "consumer.group")
            .description(
                "Count of messages which have not been consumed by at least one consumer group")
            .baseUnit("messages")
            .register(meterRegistry);
  }

  @Scheduled(fixedRate = 5, timeUnit = TimeUnit.SECONDS)
  public void updateConsumerLagNow() throws IOException, ExecutionException, InterruptedException {
    logger.info("Updating consumer lag metrics");

    var update = getConsumerLagNow();
    this.lagPerGroupPerTopic.set(update);

    var rows =
        update.entrySet().stream()
            .flatMap(
                topicAndGroupLag -> {
                  var topic = topicAndGroupLag.getKey();
                  return topicAndGroupLag.getValue().entrySet().stream()
                      .map(
                          groupAndLag -> {
                            var group = groupAndLag.getKey();
                            var lag = groupAndLag.getValue();
                            return Row.of(
                                Tags.of(Tag.of("topic", topic), Tag.of("consumer-group", group)),
                                lag);
                          });
                })
            .collect(Collectors.toList());

    lagGauges.register((Iterable) rows, true);
  }

  /**
   * For every topic, for every group subscribed to that topic, update the sum total lag of all
   * consumers in that group.
   */
  private HashMap<String, HashMap<String, Long>> getConsumerLagNow()
      throws InterruptedException, ExecutionException, IOException {
    // Fetch group offsets before topic offsets so the latter is never older than the former. This
    // ensures topic offsets > consumer offsets.
    var latestConsumerGroupOffsets = getConsumerGroupOffsetsNow();
    var latestTopicOffsets = getTopicPartitionOffsetsNow();

    // Sum the lag of all consumers in the same group and topic.
    var lagPerGroupPerTopic = new HashMap<String, HashMap<String, Long>>();
    for (var topicPartitionAndOffset : latestTopicOffsets.entrySet()) {
      var topicPartition = topicPartitionAndOffset.getKey();
      var partitionLatestOffset = topicPartitionAndOffset.getValue();
      for (var consumerGroupAndOffset : latestConsumerGroupOffsets.get(topicPartition).entrySet()) {
        var group = consumerGroupAndOffset.getKey();
        var groupOffset = consumerGroupAndOffset.getValue();
        if (groupOffset > partitionLatestOffset) {
          logger.warn(
              "Consumer group offset {} greater than topic offset {}",
              groupOffset,
              topicPartitionAndOffset);
        }

        var consumerLagOnThisPartition = Math.max(partitionLatestOffset - groupOffset, 0);
        lagPerGroupPerTopic
            .computeIfAbsent(topicPartition.topic(), k -> new HashMap<>())
            .compute(group, (k, v) -> (v == null ? 0 : v) + consumerLagOnThisPartition);
      }
    }

    return lagPerGroupPerTopic;
  }

  /** Retrieve the latest offsets for all application (i.e. non-internal) topics */
  private Map<TopicPartition, Long> getTopicPartitionOffsetsNow()
      throws InterruptedException, ExecutionException {
    var topicNames = admin.listTopics(new ListTopicsOptions().listInternal(false)).names().get();
    var topicPartitions =
        admin.describeTopics(topicNames).all().get().values().stream()
            .flatMap(
                topicDescription ->
                    topicDescription.partitions().stream()
                        .map(p -> new TopicPartition(topicDescription.name(), p.partition())))
            .collect(Collectors.toSet());
    var requestSpec =
        topicPartitions.stream()
            .collect(toMap(Function.identity(), v -> (OffsetSpec) new OffsetSpec.LatestSpec()));
    return admin.listOffsets(requestSpec).all().get().entrySet().stream()
        .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
  }

  private Map<TopicPartition, Map<String, Long>> getConsumerGroupOffsetsNow()
      throws InterruptedException, ExecutionException, IOException {
    var listConsumerGroups = admin.listConsumerGroups();
    if (!listConsumerGroups.errors().get().isEmpty()) {
      var e = new IOException("Error while fetching consumer group listings");
      listConsumerGroups.errors().get().forEach(e::addSuppressed);
      throw e;
    }

    var groups =
        listConsumerGroups.valid().get().stream()
            .map(ConsumerGroupListing::groupId)
            .collect(Collectors.toSet());
    var offsets =
        groups.stream()
            .collect(
                toMap(
                    Function.identity(),
                    group ->
                        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()));

    HashMap<TopicPartition, Map<String, Long>> result = new HashMap<>();
    for (var group : groups) {
      for (var entry : offsets.get(group).get().entrySet()) {
        var topicPartition = entry.getKey();
        var offset = entry.getValue().offset();

        result.computeIfAbsent(topicPartition, k -> new HashMap<>()).put(group, offset);
      }
    }
    return result;
  }
}
