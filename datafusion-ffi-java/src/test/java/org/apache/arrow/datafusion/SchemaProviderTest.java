package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import org.apache.arrow.datafusion.catalog.SchemaProvider;
import org.apache.arrow.datafusion.catalog.Session;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.datafusion.physical_plan.PlanProperties;
import org.apache.arrow.datafusion.physical_plan.RecordBatchReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Tests for {@link SchemaProvider} default methods. */
public class SchemaProviderTest {

  @Test
  void testRegisterTableThrows() {
    SchemaProvider provider = createSimpleSchemaProvider();
    assertThrows(UnsupportedOperationException.class, () -> provider.registerTable("t", null));
  }

  @Test
  void testDeregisterTableThrows() {
    SchemaProvider provider = createSimpleSchemaProvider();
    assertThrows(UnsupportedOperationException.class, () -> provider.deregisterTable("t"));
  }

  @Test
  void testTableExists() {
    SchemaProvider provider = createSimpleSchemaProvider();
    assertTrue(provider.tableExists("existing"));
    assertFalse(provider.tableExists("nonexistent"));
  }

  private static SchemaProvider createSimpleSchemaProvider() {
    return new SchemaProvider() {
      @Override
      public List<String> tableNames() {
        return List.of("existing");
      }

      @Override
      public Optional<TableProvider> table(String name) {
        if ("existing".equals(name)) {
          return Optional.of(createMinimalTableProvider());
        }
        return Optional.empty();
      }
    };
  }

  private static TableProvider createMinimalTableProvider() {
    return new TableProvider() {
      @Override
      public Schema schema() {
        return new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));
      }

      @Override
      public ExecutionPlan scan(
          Session session, List<Expr> filters, List<Integer> projection, Long limit) {
        return new ExecutionPlan() {
          @Override
          public Schema schema() {
            return new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));
          }

          @Override
          public PlanProperties properties() {
            return new PlanProperties();
          }

          @Override
          public RecordBatchReader execute(int partition, BufferAllocator allocator) {
            throw new UnsupportedOperationException("not implemented");
          }
        };
      }
    };
  }
}
