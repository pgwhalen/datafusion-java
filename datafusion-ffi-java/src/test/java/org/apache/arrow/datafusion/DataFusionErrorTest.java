package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.datafusion.common.DataFusionError;
import org.junit.jupiter.api.Test;

/** Tests for {@link DataFusionError}. */
public class DataFusionErrorTest {

  @Test
  void testConstructorWithCause() {
    RuntimeException cause = new RuntimeException("root cause");
    DataFusionError error = new DataFusionError("something failed", cause);
    assertEquals("something failed", error.getMessage());
    assertSame(cause, error.getCause());
  }
}
