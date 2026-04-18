package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.datafusion.catalog.ScanArgs;
import org.apache.arrow.datafusion.catalog.Session;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.datafusion.execution.TaskContext;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.TableProviderFilterPushDown;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.datafusion.physical_plan.PlanProperties;
import org.apache.arrow.datafusion.physical_plan.RecordBatchReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Tests for {@link TableProvider} default methods. */
public class TableProviderTest {

  @Test
  void testSupportsFiltersPushdownDefault() {
    TableProvider provider = createMinimalTableProvider();
    List<TableProviderFilterPushDown> result =
        provider.supportsFiltersPushdown(
            List.of(
                new Expr.LiteralExpr(new ScalarValue.Int64(1L)),
                new Expr.LiteralExpr(new ScalarValue.Int64(2L))));
    assertEquals(2, result.size());
    assertEquals(TableProviderFilterPushDown.INEXACT, result.get(0));
    assertEquals(TableProviderFilterPushDown.INEXACT, result.get(1));
  }

  private static TableProvider createMinimalTableProvider() {
    return new TableProvider() {
      @Override
      public Schema schema() {
        return new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));
      }

      @Override
      public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
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
          public RecordBatchReader execute(
              int partition, TaskContext taskContext, BufferAllocator allocator) {
            throw new UnsupportedOperationException("not implemented");
          }
        };
      }
    };
  }
}
