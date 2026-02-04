#
# Validate the datafusion-ffi-java JAR package (Windows)
# This script verifies that the JAR contains native libraries and works correctly
#
# Requires Java 22+ to be available in PATH
#

$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $RootDir) {
    $RootDir = (Get-Location).Path
}
$JarDir = Join-Path $RootDir "datafusion-ffi-java/build/libs"
$TestDir = Join-Path $RootDir "build/jar-validation"

# Check Java version
$javaVersion = (java -version 2>&1 | Select-Object -First 1) -replace '.*"(\d+).*"', '$1'
if ([int]$javaVersion -lt 22) {
    Write-Error "Java 22+ is required but found version $javaVersion"
    exit 1
}
Write-Host "[INFO] Using Java version: $javaVersion"

# Find the main JAR
$jar = Get-ChildItem -Path $JarDir -Filter "datafusion-ffi-java-*.jar" |
    Where-Object { $_.Name -notlike "*-sources*" -and $_.Name -notlike "*-javadoc*" } |
    Select-Object -First 1

if (-not $jar) {
    Write-Error "No JAR file found in $JarDir"
    Write-Host "[INFO] Run './gradlew :datafusion-ffi-java:jarWithLocalLib' or ':jarWithAllLibs' first"
    exit 1
}

Write-Host "[INFO] DataFusion FFI Java JAR Validation"
Write-Host "========================================"
Write-Host "[INFO] Found JAR: $($jar.FullName)"

# List natives in JAR
Write-Host "[INFO] Native libraries in JAR:"
jar tf $jar.FullName | Select-String "natives/"

# Clean up previous test
if (Test-Path $TestDir) {
    Remove-Item -Recurse -Force $TestDir
}
New-Item -ItemType Directory -Force -Path "$TestDir/src" | Out-Null
New-Item -ItemType Directory -Force -Path "$TestDir/classes" | Out-Null

# Download dependencies
$depDir = Join-Path $TestDir "deps"
New-Item -ItemType Directory -Force -Path $depDir | Out-Null

Write-Host "[INFO] Downloading test dependencies..."

$deps = @(
    "https://repo1.maven.org/maven2/org/apache/arrow/arrow-vector/18.1.0/arrow-vector-18.1.0.jar",
    "https://repo1.maven.org/maven2/org/apache/arrow/arrow-format/18.1.0/arrow-format-18.1.0.jar",
    "https://repo1.maven.org/maven2/org/apache/arrow/arrow-memory-core/18.1.0/arrow-memory-core-18.1.0.jar",
    "https://repo1.maven.org/maven2/org/apache/arrow/arrow-memory-unsafe/18.1.0/arrow-memory-unsafe-18.1.0.jar",
    "https://repo1.maven.org/maven2/org/apache/arrow/arrow-c-data/18.1.0/arrow-c-data-18.1.0.jar",
    "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar",
    "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.16/slf4j-simple-2.0.16.jar",
    "https://repo1.maven.org/maven2/org/scijava/native-lib-loader/2.5.0/native-lib-loader-2.5.0.jar",
    "https://repo1.maven.org/maven2/com/google/flatbuffers/flatbuffers-java/24.3.25/flatbuffers-java-24.3.25.jar",
    "https://repo1.maven.org/maven2/io/netty/netty-common/4.1.115.Final/netty-common-4.1.115.Final.jar",
    "https://repo1.maven.org/maven2/io/netty/netty-buffer/4.1.115.Final/netty-buffer-4.1.115.Final.jar",
    "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.17.2/jackson-core-2.17.2.jar",
    "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.17.2/jackson-databind-2.17.2.jar",
    "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.17.2/jackson-annotations-2.17.2.jar"
)

foreach ($url in $deps) {
    $filename = Split-Path $url -Leaf
    $dest = Join-Path $depDir $filename
    if (-not (Test-Path $dest)) {
        Invoke-WebRequest -Uri $url -OutFile $dest
    }
}

# Create test program
Write-Host "[INFO] Creating test program..."

@'
import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.RecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class JarValidationTest {
    public static void main(String[] args) {
        System.out.println("Starting JAR validation test...");
        try (BufferAllocator allocator = new RootAllocator();
             SessionContext ctx = new SessionContext()) {
            System.out.println("Created SessionContext successfully");
            DataFrame df = ctx.sql("SELECT 1 + 1 AS result");
            System.out.println("Executed SQL successfully");
            try (RecordBatchStream stream = df.executeStream(allocator)) {
                VectorSchemaRoot root = stream.getVectorSchemaRoot();
                while (stream.loadNextBatch()) {
                    System.out.println("Got " + root.getRowCount() + " rows");
                }
            }
            System.out.println("[SUCCESS] JAR validation test passed!");
        } catch (Exception e) {
            System.err.println("[FAILURE] JAR validation test failed!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
'@ | Out-File -FilePath "$TestDir/src/JarValidationTest.java" -Encoding UTF8

# Build classpath
$depJars = Get-ChildItem -Path $depDir -Filter "*.jar" | ForEach-Object { $_.FullName }
$classpath = @($jar.FullName) + $depJars -join ";"

# Compile and run
Write-Host "[INFO] Compiling test program..."
javac -cp $classpath -d "$TestDir/classes" "$TestDir/src/JarValidationTest.java"

Write-Host "[INFO] Running test program..."
java -cp "$TestDir/classes;$classpath" `
    --enable-native-access=ALL-UNNAMED `
    --add-opens=java.base/java.nio=ALL-UNNAMED `
    JarValidationTest

Write-Host "[INFO] Validation complete!"
