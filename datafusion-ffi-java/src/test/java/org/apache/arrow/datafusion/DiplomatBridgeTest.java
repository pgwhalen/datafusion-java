package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class DiplomatBridgeTest {

  @Test
  void testCreateAndDestroyContext() {
    try (DfSessionContext ctx = DfSessionContext.new_()) {
      assertNotNull(ctx);
    }
  }

  @Test
  void testSessionId() {
    try (DfSessionContext ctx = DfSessionContext.new_()) {
      String id = ctx.sessionId();
      assertNotNull(id);
      assertFalse(id.isEmpty());
    }
  }

  @Test
  void testSessionStartTimeMillis() {
    long before = System.currentTimeMillis();
    try (DfSessionContext ctx = DfSessionContext.new_()) {
      long startTime = ctx.sessionStartTimeMillis();
      long after = System.currentTimeMillis();
      assertTrue(startTime >= before - 1000);
      assertTrue(startTime <= after + 1000);
    }
  }

  @Test
  void testSqlSuccess() {
    try (DfSessionContext ctx = DfSessionContext.new_()) {
      try (DfDataFrame df = ctx.sql("SELECT 1 + 1 AS result")) {
        assertNotNull(df);
      }
    }
  }

  @Test
  void testSqlError() {
    try (DfSessionContext ctx = DfSessionContext.new_()) {
      DfError error =
          assertThrows(DfError.class, () -> ctx.sql("SELECT * FROM nonexistent_table_xyz"));
      String msg = error.toDisplay();
      assertTrue(msg.contains("nonexistent_table_xyz"), "Error: " + msg);
      error.close();
    }
  }

  @Test
  void testSessionIdStableAcrossOperations() {
    try (DfSessionContext ctx = DfSessionContext.new_()) {
      String id1 = ctx.sessionId();
      try (DfDataFrame df = ctx.sql("SELECT 42 AS answer")) {
        assertNotNull(df);
      }
      String id2 = ctx.sessionId();
      assertEquals(id1, id2);
    }
  }
}
