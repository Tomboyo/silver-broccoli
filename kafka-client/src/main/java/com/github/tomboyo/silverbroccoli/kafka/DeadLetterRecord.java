package com.github.tomboyo.silverbroccoli.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Json wrapper for an unprocessable Consumer Record. */
public class DeadLetterRecord<K, V> {

  private K key;
  private V value;
  private String topic;
  private Map<String, byte[]> headers;

  public DeadLetterRecord() {}

  public DeadLetterRecord(ConsumerRecord<K, V> undeliverableRecord) {
    key = undeliverableRecord.key();
    value = undeliverableRecord.value();
    topic = undeliverableRecord.topic();
    headers =
        StreamSupport.stream(undeliverableRecord.headers().spliterator(), false)
            .collect(Collectors.toMap(Header::key, Header::value));
  }

  @JsonProperty("key")
  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  @JsonProperty("value")
  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  @JsonProperty("topic")
  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  @JsonProperty("headers")
  public Map<String, byte[]> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, byte[]> headers) {
    this.headers = headers;
  }

  @Override
  public String toString() {
    return "DeadLetterRecord{"
        + " key=\""
        + key
        + "\" value=\""
        + value
        + "\" topic=\""
        + topic
        + "\" headers=\""
        + headers
        + "\" }";
  }
}
