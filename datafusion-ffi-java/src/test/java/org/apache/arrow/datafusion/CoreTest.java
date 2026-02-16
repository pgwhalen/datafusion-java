package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Core functionality tests for the DataFusion FFI Java bindings. */
public class CoreTest {

  @Test
  void testContextCreation() {
    try (SessionContext ctx = new SessionContext()) {
      assertNotNull(ctx);
    }
  }

  @Test
  void testInvalidSqlThrowsException() {
    try (SessionContext ctx = new SessionContext()) {
      assertThrows(RuntimeException.class, () -> ctx.sql("SELECT * FROM nonexistent_table"));
    }
  }

  @Test
  void testSessionId() {
    try (SessionContext ctx = new SessionContext()) {
      String id = ctx.sessionId();
      assertNotNull(id);
      assertFalse(id.isEmpty());
    }
  }

  @Test
  void testSessionStartTime() {
    Instant before = Instant.now();
    try (SessionContext ctx = new SessionContext()) {
      Instant startTime = ctx.sessionStartTime();
      Instant after = Instant.now();

      assertNotNull(startTime);
      // Session start time should be between 'before' and 'after' (with some tolerance)
      assertTrue(
          Duration.between(before, startTime).toSeconds() >= -1,
          "Session start time should not be more than 1 second before creation");
      assertTrue(
          Duration.between(startTime, after).toSeconds() >= -1,
          "Session start time should not be more than 1 second after test");
    }
  }
}
