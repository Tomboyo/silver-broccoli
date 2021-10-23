package com.github.tomboyo.silverbroccoli;

import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public class ConfigurationSupport {

  public static Map<String, Object> composeConfigs(List<Map<String, Object>> configs) {
    var acc = new HashMap<String, Object>();
    for (var config : configs) {
      acc.putAll(config);
    }
    return acc;
  }

  public static Map<String, Object> extractMap(
      Environment env, String prefix, List<String> properties) {
    return stripPrefix(asMap(env, prefix, properties), prefix);
  }

  public static Map<String, Object> stripPrefix(Map<String, Object> config, String prefix) {
    var acc = new HashMap<String, Object>();
    for (var entry : config.entrySet()) {
      acc.put(entry.getKey().replaceFirst("^" + prefix + "\\.?", ""), entry.getValue());
    }
    return acc;
  }

  public static Map<String, Object> asMap(Environment env, String prefix, List<String> properties) {
    return properties.stream()
        .map(x -> String.join(".", prefix, x))
        .filter(key -> env.getProperty(key) != null)
        .collect(toMap(Function.identity(), env::getRequiredProperty));
  }
}
