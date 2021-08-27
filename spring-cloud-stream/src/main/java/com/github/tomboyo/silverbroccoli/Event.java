package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An arbitrary structured data type which we can communicate via Kafka.
 */
public class Event {
  private String message;

  @JsonProperty("message")
  public String getMessage() {
    return message;
  }

  public Event message(String message) {
    this.message = message;
    return this;
  }

  @Override
  public String toString() {
    return "Event{" +
        "message='" + message + '\'' +
        '}';
  }
}
