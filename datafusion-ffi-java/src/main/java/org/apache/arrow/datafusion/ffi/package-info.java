/**
 * DataFusion bindings using Java's Foreign Function and Memory (FFM) API.
 *
 * <p>This package provides a way to interact with Apache DataFusion through Java's FFM API (Project
 * Panama), enabling zero-copy data transfer via the Arrow C Data Interface.
 *
 * <p>Main classes:
 *
 * <ul>
 *   <li>{@link org.apache.arrow.datafusion.ffi.SessionContext} - The main entry point for creating
 *       sessions and executing queries
 *   <li>{@link org.apache.arrow.datafusion.ffi.DataFrame} - Represents a DataFusion DataFrame
 *   <li>{@link org.apache.arrow.datafusion.ffi.RecordBatchStream} - Stream of Arrow record batches
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * try (BufferAllocator allocator = new RootAllocator();
 *      SessionContext ctx = new SessionContext()) {
 *     // Register data
 *     VectorSchemaRoot data = createData(allocator);
 *     ctx.registerTable("test", data, allocator);
 *
 *     // Execute query
 *     try (DataFrame df = ctx.sql("SELECT * FROM test");
 *          RecordBatchStream stream = df.executeStream(allocator)) {
 *         VectorSchemaRoot root = stream.getVectorSchemaRoot();
 *         while (stream.loadNextBatch()) {
 *             // Process data
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p>Note: This API requires Java 21+ with preview features enabled.
 */
package org.apache.arrow.datafusion.ffi;
