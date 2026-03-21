import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class JarValidationTest {
    public static void main(String[] args) {
        System.out.println("Starting JAR validation test...");

        try (BufferAllocator allocator = new RootAllocator();
             SessionContext ctx = new SessionContext()) {

            System.out.println("Created SessionContext successfully");

            // Execute a simple SQL query
            DataFrame df = ctx.sql("SELECT 1 + 1 AS result, 'hello' AS greeting");
            System.out.println("Executed SQL successfully");

            // Execute and stream results
            try (SendableRecordBatchStream stream = df.executeStream(allocator)) {
                VectorSchemaRoot root = stream.getVectorSchemaRoot();
                System.out.println("Schema: " + root.getSchema());

                while (stream.loadNextBatch()) {
                    System.out.println("Got batch with " + root.getRowCount() + " rows");

                    // Print first row
                    if (root.getRowCount() > 0) {
                        System.out.println("  result: " + root.getVector(0).getObject(0));
                        System.out.println("  greeting: " + root.getVector(1).getObject(0));
                    }
                }
            }

            System.out.println("\n[SUCCESS] JAR validation test passed!");

        } catch (Exception e) {
            System.err.println("\n[FAILURE] JAR validation test failed!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
