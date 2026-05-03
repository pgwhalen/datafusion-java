package org.apache.arrow.datafusion.providers.flight;

import static org.apache.arrow.datafusion.providers.ProviderTestSupport.readPhysicalPlan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.generated.DfArrowBatch;
import org.apache.arrow.datafusion.generated.DfFlightSqlTestServer;
import org.apache.arrow.datafusion.generated.DfStringArray;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * End-to-end federation test. Stands up two in-process Flight SQL test servers, points a
 * federation-enabled client {@link SessionContext} at both via {@link FlightSqlFederatedCatalog},
 * and asserts both result data and predicate pushdown.
 */
class FlightSqlFederationTest {

  @Test
  void joinAcrossTwoRemotesWithPushdown() {
    try (BufferAllocator allocator = new RootAllocator();
        DfFlightSqlTestServer usersServer = new DfFlightSqlTestServer();
        DfFlightSqlTestServer ordersServer = new DfFlightSqlTestServer()) {

      // Seed the "users" remote.
      Schema usersSchema =
          new Schema(
              List.of(
                  intField("id"),
                  new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
      try (VectorSchemaRoot users = VectorSchemaRoot.create(usersSchema, allocator)) {
        IntVector id = (IntVector) users.getVector("id");
        VarCharVector name = (VarCharVector) users.getVector("name");
        id.allocateNew(2);
        name.allocateNew(2);
        id.set(0, 1);
        name.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
        id.set(1, 2);
        name.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
        users.setRowCount(2);
        registerOnServer(usersServer, "users", users, allocator);
      }

      // Seed the "orders" remote.
      Schema ordersSchema =
          new Schema(List.of(intField("id"), intField("user_id"), intField("amount")));
      try (VectorSchemaRoot orders = VectorSchemaRoot.create(ordersSchema, allocator)) {
        IntVector id = (IntVector) orders.getVector("id");
        IntVector userId = (IntVector) orders.getVector("user_id");
        IntVector amount = (IntVector) orders.getVector("amount");
        id.allocateNew(3);
        userId.allocateNew(3);
        amount.allocateNew(3);
        id.set(0, 101);
        userId.set(0, 1);
        amount.set(0, 50);
        id.set(1, 102);
        userId.set(1, 1);
        amount.set(1, 150);
        id.set(2, 103);
        userId.set(2, 2);
        amount.set(2, 200);
        orders.setRowCount(3);
        registerOnServer(ordersServer, "orders", orders, allocator);
      }

      // Build the federating client SessionContext.
      String usersEndpoint = "http://127.0.0.1:" + portOf(usersServer);
      String ordersEndpoint = "http://127.0.0.1:" + portOf(ordersServer);

      try (SessionContext ctx = SessionContext.newWithFederation(ConfigOptions.defaults());
          FlightSqlFederatedCatalog usersCatalog =
              FlightSqlFederatedCatalog.builder()
                  .endpoint(usersEndpoint)
                  .computeContext("users_remote")
                  .table("users")
                  .build();
          FlightSqlFederatedCatalog ordersCatalog =
              FlightSqlFederatedCatalog.builder()
                  .endpoint(ordersEndpoint)
                  .computeContext("orders_remote")
                  .table("orders")
                  .build()) {

        ctx.registerCatalog("users_remote", usersCatalog);
        ctx.registerCatalog("orders_remote", ordersCatalog);

        // Aliasing each table to its own (unqualified) name sidesteps a DataFusion
        // unparser quirk where SubqueryAlias above Filter emits the original table name inside
        // the WHERE clause instead of the alias. With alias == table name, the generated SQL is
        // syntactically valid regardless of which one the unparser emits.
        String query =
            "SELECT users.name, orders.amount"
                + " FROM users_remote.public.users AS users"
                + " JOIN orders_remote.public.orders AS orders"
                + "   ON users.id = orders.user_id"
                + " WHERE orders.amount > 100"
                + " ORDER BY users.name";

        // Assert predicate pushdown at the physical plan level: the orders_remote
        // VirtualExecutionPlan's base_sql must contain the "amount > 100" WHERE clause, and
        // the users_remote VirtualExecutionPlan's base_sql must not reference amount at all.
        String explain = explainPhysical(ctx, query, allocator);
        String usersVep = findVepLine(explain, "users_remote");
        String ordersVep = findVepLine(explain, "orders_remote");
        assertTrue(
            ordersVep.contains("amount > 100"),
            "expected the > 100 predicate inlined in orders_remote base_sql:\n" + ordersVep);
        assertFalse(
            usersVep.contains("amount"),
            "users_remote base_sql should not reference the amount column:\n" + usersVep);

        List<String[]> rows = collectNameAmount(ctx, query, allocator);
        assertEquals(2, rows.size(), "expected two joined rows");
        rows.sort(Comparator.comparing(r -> r[0]));
        assertArrayEqualsStr(new String[] {"Alice", "150"}, rows.get(0));
        assertArrayEqualsStr(new String[] {"Bob", "200"}, rows.get(1));

        // Corroborate pushdown at the wire level: inspect the SQL each test server received.
        List<String> ordersSql = toList(ordersServer.receivedSql());
        List<String> usersSql = toList(usersServer.receivedSql());

        assertTrue(
            ordersSql.stream().anyMatch(s -> s.contains("amount > 100")),
            "expected the > 100 predicate in a SQL statement received by orders_remote: "
                + ordersSql);
        assertFalse(
            usersSql.stream().anyMatch(s -> s.contains("amount")),
            "users_remote should never receive a SQL statement referencing the amount column: "
                + usersSql);
      }
    }
  }

  // -------- helpers --------

  private static Field intField(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  private static int portOf(DfFlightSqlTestServer server) {
    // The generated method returns JAVA_SHORT; widen to an unsigned port.
    return Short.toUnsignedInt(server.port());
  }

  private static void registerOnServer(
      DfFlightSqlTestServer server, String name, VectorSchemaRoot root, BufferAllocator allocator) {
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);
      try (DfArrowBatch batch =
          DfArrowBatch.fromAddresses(ffiSchema.memoryAddress(), ffiArray.memoryAddress())) {
        server.registerMemtable(name, batch);
      }
    }
  }

  private static List<String> toList(DfStringArray arr) {
    try (arr) {
      int len = (int) arr.len();
      List<String> out = new ArrayList<>(len);
      for (int i = 0; i < len; i++) {
        out.add(arr.get(i));
      }
      return out;
    }
  }

  private static String findVepLine(String plan, String computeContext) {
    String marker = "compute_context=" + computeContext;
    int idx = plan.indexOf(marker);
    if (idx < 0) {
      throw new AssertionError(
          "No VirtualExecutionPlan for compute_context=" + computeContext + " in:\n" + plan);
    }
    int start = plan.lastIndexOf('\n', idx) + 1;
    int end = plan.indexOf('\n', idx);
    if (end < 0) end = plan.length();
    return plan.substring(start, end);
  }

  private static String explainPhysical(SessionContext ctx, String sql, BufferAllocator allocator) {
    try (DataFrame df = ctx.sql("EXPLAIN " + sql);
        SendableRecordBatchStream stream = df.executeStream(allocator)) {
      String physical = readPhysicalPlan(stream);
      return physical == null ? "" : physical;
    }
  }

  private static List<String[]> collectNameAmount(
      SessionContext ctx, String sql, BufferAllocator allocator) {
    List<String[]> all = new ArrayList<>();
    try (DataFrame df = ctx.sql(sql);
        SendableRecordBatchStream stream = df.executeStream(allocator)) {
      while (stream.loadNextBatch()) {
        VectorSchemaRoot r = stream.getVectorSchemaRoot();
        VarCharVector name = (VarCharVector) r.getVector("name");
        IntVector amount = (IntVector) r.getVector("amount");
        for (int i = 0; i < r.getRowCount(); i++) {
          all.add(new String[] {name.getObject(i).toString(), Integer.toString(amount.get(i))});
        }
      }
    }
    return all;
  }

  private static void assertArrayEqualsStr(String[] expected, String[] actual) {
    assertTrue(
        Arrays.equals(expected, actual),
        "expected=" + Arrays.toString(expected) + " actual=" + Arrays.toString(actual));
  }
}
