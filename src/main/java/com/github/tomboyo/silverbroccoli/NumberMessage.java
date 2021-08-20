package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** This is a simple message object that we can enqueue and dequeue with kafka. */
public class NumberMessage {
  private final int number;

  @JsonCreator
  public NumberMessage(@JsonProperty("number") int number) {
    this.number = number;
  }

  public int getNumber() {
    return number;
  }

  @Override
  public String toString() {
    return "NumberMessage{ number=" + number + " }";
  }
}
