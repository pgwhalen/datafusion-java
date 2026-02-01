package org.apache.arrow.datafusion.ffi;

import static org.junit.jupiter.api.Assertions.*;

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
}
