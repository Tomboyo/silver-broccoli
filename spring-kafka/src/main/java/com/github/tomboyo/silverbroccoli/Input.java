package com.github.tomboyo.silverbroccoli;

public class Input {
  private int key;
  private String message;

  public int getKey() {
    return key;
  }

  public Input setKey(int key) {
    this.key = key;
    return this;
  }

  public String getMessage() {
    return message;
  }

  public Input setMessage(String message) {
    this.message = message;
    return this;
  }
}
