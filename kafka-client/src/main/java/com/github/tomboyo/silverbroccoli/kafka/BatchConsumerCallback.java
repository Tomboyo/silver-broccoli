package com.github.tomboyo.silverbroccoli.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

@FunctionalInterface
public interface BatchConsumerCallback<K, V> {
  void consume(KafkaProducer<K, Object> producer, ConsumerRecord<K, V> record);
}
