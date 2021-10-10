package com.github.tomboyo.silverbroccoli.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class JacksonObjectSerializer implements Serializer<Object> {

  private final ObjectMapper mapper;

  public JacksonObjectSerializer() {
    mapper = new ObjectMapper();
  }

  @Override
  public byte[] serialize(String s, Object o) {
    try {
      return mapper.writeValueAsBytes(o);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
