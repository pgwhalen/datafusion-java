package org.apache.arrow.datafusion;

import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between the public {@link PhysicalExpr} API and Diplomat-generated {@link DfPhysicalExpr}.
 */
final class PhysicalExprBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(PhysicalExprBridge.class);

  private final DfPhysicalExpr dfExpr;
  private volatile boolean closed = false;

  private PhysicalExprBridge(DfPhysicalExpr dfExpr) {
    this.dfExpr = dfExpr;
  }

  /**
   * Create a physical expression from filter expressions using a default session state.
   *
   * @param allocator buffer allocator for Arrow schema export
   * @param tableSchema the table schema
   * @param filters the filter expressions to compile
   * @return a new PhysicalExprBridge
   */
  static PhysicalExprBridge fromProtoFilters(
      BufferAllocator allocator, Schema tableSchema, List<Expr> filters) {
    byte[] filterBytes = ExprProtoConverter.toProtoBytes(filters);

    ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
    try {
      Data.exportSchema(allocator, tableSchema, null, ffiSchema);
      long schemaAddr = ffiSchema.memoryAddress();

      try {
        DfPhysicalExpr dfExpr = DfPhysicalExpr.fromProtoFilters(filterBytes, schemaAddr);
        return new PhysicalExprBridge(dfExpr);
      } catch (DfError e) {
        throw new NativeDataFusionException(e);
      }
    } finally {
      ffiSchema.release();
      ffiSchema.close();
    }
  }

  /** Returns the underlying Diplomat opaque for use by LiteralGuaranteeBridge. */
  DfPhysicalExpr dfExpr() {
    return dfExpr;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      dfExpr.close();
      logger.debug("Closed PhysicalExpr");
    }
  }
}
