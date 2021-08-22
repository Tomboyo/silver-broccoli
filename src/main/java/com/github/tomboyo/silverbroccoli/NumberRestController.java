package com.github.tomboyo.silverbroccoli;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/number")
@Profile("numbers")
public class NumberRestController {

  public static final class EnqueueBody {
    public final int n;

    @JsonCreator
    public EnqueueBody(
        @JsonProperty("n") int n
    ) {
      this.n = n;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(NumberRestController.class);

  private final StreamBridge bridge;

  @Autowired public NumberRestController(
      StreamBridge bridge
  ) {
    this.bridge = bridge;
  }

  // tag::generate-message-with-streambridge[]
  @PostMapping("/")
  public ResponseEntity<Void> enqueue(@RequestBody EnqueueBody body) {
    // This is re-using an existing producer binding.
    bridge.send("producer-out-0", body.n);
    LOGGER.info("Enqueued in response to REST API request: n=" + body.n);
    return ResponseEntity.ok().build();
  }
  // end::generate-message-with-streambridge[]
}
