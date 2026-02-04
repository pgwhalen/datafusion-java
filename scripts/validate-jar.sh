#!/bin/bash
#
# Validate the datafusion-ffi-java JAR package
# This script verifies that the JAR contains native libraries and works correctly
#
# Requires Java 22+ to be available via JAVA_HOME or in PATH

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/platforms.sh"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR_DIR="$ROOT_DIR/datafusion-ffi-java/build/libs"
TEST_DIR="$ROOT_DIR/build/jar-validation"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Find Java 22+ - check JAVA_HOME first, then common locations
find_java22() {
    # Check JAVA_HOME
    if [ -n "$JAVA_HOME" ]; then
        local version=$("$JAVA_HOME/bin/java" -version 2>&1 | head -1 | grep -oE '[0-9]+' | head -1)
        if [ "$version" -ge 22 ] 2>/dev/null; then
            echo "$JAVA_HOME/bin"
            return 0
        fi
    fi

    # Check common locations on macOS
    for jdk in /Library/Java/JavaVirtualMachines/temurin-22*/Contents/Home \
               /Library/Java/JavaVirtualMachines/jdk-22*/Contents/Home \
               /Users/*/Library/Java/JavaVirtualMachines/temurin-22*/Contents/Home \
               /opt/homebrew/opt/openjdk@22 \
               /usr/local/opt/openjdk@22; do
        if [ -d "$jdk/bin" ]; then
            local version=$("$jdk/bin/java" -version 2>&1 | head -1 | grep -oE '[0-9]+' | head -1)
            if [ "$version" -ge 22 ] 2>/dev/null; then
                echo "$jdk/bin"
                return 0
            fi
        fi
    done

    # Fallback to system Java (may not work if < 22)
    echo ""
    return 1
}

JAVA_BIN=$(find_java22 || true)
if [ -n "$JAVA_BIN" ]; then
    JAVA="$JAVA_BIN/java"
    JAVAC="$JAVA_BIN/javac"
else
    # Check if system Java is 22+
    SYS_VERSION=$(java -version 2>&1 | head -1 | grep -oE '[0-9]+' | head -1)
    if [ "$SYS_VERSION" -ge 22 ] 2>/dev/null; then
        JAVA="java"
        JAVAC="javac"
    else
        log_error "Java 22+ is required but not found."
        log_error "System Java version: $SYS_VERSION"
        log_info "Set JAVA_HOME to a Java 22+ installation and try again."
        log_info "On macOS: brew install openjdk@22"
        log_info "On Ubuntu: sudo apt install openjdk-22-jdk"
        exit 1
    fi
fi

log_info "Using Java: $JAVA"
"$JAVA" -version 2>&1 | head -1

# Find the JAR file
find_jar() {
    # Look for the main JAR (not sources or javadoc)
    local jar=$(find "$JAR_DIR" -name "datafusion-ffi-java-*.jar" \
        ! -name "*-sources.jar" \
        ! -name "*-javadoc.jar" \
        -type f 2>/dev/null | head -1)

    if [ -z "$jar" ]; then
        log_error "No JAR file found in $JAR_DIR"
        log_info "Run './gradlew :datafusion-ffi-java:jarWithLocalLib' or ':jarWithAllLibs' first"
        exit 1
    fi

    echo "$jar"
}

# List native libraries in JAR
list_natives() {
    local jar="$1"
    log_info "Listing native libraries in JAR:"

    local natives=$(unzip -l "$jar" | grep "natives/" | grep -v "/$" || true)

    if [ -z "$natives" ]; then
        log_warn "No native libraries found in JAR"
        return 1
    fi

    echo "$natives" | while read line; do
        echo "  $line"
    done

    return 0
}

# Check expected platform directories
check_platforms() {
    local jar="$1"
    local expected_platforms=("${PLATFORMS[@]}")
    local found=0
    local missing=0

    log_info "Checking for platform directories:"

    for platform in "${expected_platforms[@]}"; do
        if unzip -l "$jar" | grep -q "natives/$platform/"; then
            echo "  [OK] $platform"
            ((found++))
        else
            echo "  [MISSING] $platform"
            ((missing++))
        fi
    done

    log_info "Found $found of ${#expected_platforms[@]} platforms"

    if [ $found -eq 0 ]; then
        log_error "No platform libraries found!"
        return 1
    fi

    return 0
}

# Download dependencies for testing
download_dependencies() {
    local dep_dir="$TEST_DIR/deps"
    mkdir -p "$dep_dir"

    log_info "Downloading test dependencies..." >&2

    # Arrow 18.1.0 dependencies and their transitive deps
    local deps=(
        "https://repo1.maven.org/maven2/org/apache/arrow/arrow-vector/18.1.0/arrow-vector-18.1.0.jar"
        "https://repo1.maven.org/maven2/org/apache/arrow/arrow-format/18.1.0/arrow-format-18.1.0.jar"
        "https://repo1.maven.org/maven2/org/apache/arrow/arrow-memory-core/18.1.0/arrow-memory-core-18.1.0.jar"
        "https://repo1.maven.org/maven2/org/apache/arrow/arrow-memory-unsafe/18.1.0/arrow-memory-unsafe-18.1.0.jar"
        "https://repo1.maven.org/maven2/org/apache/arrow/arrow-c-data/18.1.0/arrow-c-data-18.1.0.jar"
        "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar"
        "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.16/slf4j-simple-2.0.16.jar"
        "https://repo1.maven.org/maven2/org/scijava/native-lib-loader/2.5.0/native-lib-loader-2.5.0.jar"
        "https://repo1.maven.org/maven2/com/google/flatbuffers/flatbuffers-java/24.3.25/flatbuffers-java-24.3.25.jar"
        "https://repo1.maven.org/maven2/io/netty/netty-common/4.1.115.Final/netty-common-4.1.115.Final.jar"
        "https://repo1.maven.org/maven2/io/netty/netty-buffer/4.1.115.Final/netty-buffer-4.1.115.Final.jar"
        "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.17.2/jackson-core-2.17.2.jar"
        "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.17.2/jackson-databind-2.17.2.jar"
        "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.17.2/jackson-annotations-2.17.2.jar"
    )

    for url in "${deps[@]}"; do
        local filename=$(basename "$url")
        if [ ! -f "$dep_dir/$filename" ]; then
            curl -sL -o "$dep_dir/$filename" "$url"
        fi
    done

    echo "$dep_dir"
}

# Create and run test program
run_test() {
    local jar="$1"
    local dep_dir="$2"

    log_info "Creating test program..."

    mkdir -p "$TEST_DIR/src"

    cat > "$TEST_DIR/src/JarValidationTest.java" << 'EOF'
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

            // Execute a simple SQL query
            DataFrame df = ctx.sql("SELECT 1 + 1 AS result, 'hello' AS greeting");
            System.out.println("Executed SQL successfully");

            // Execute and stream results
            try (RecordBatchStream stream = df.executeStream(allocator)) {
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
EOF

    log_info "Compiling test program..."
    log_info "Using Java: $JAVA"

    # Build classpath
    local classpath="$jar"
    for dep in "$dep_dir"/*.jar; do
        classpath="$classpath:$dep"
    done

    # Compile
    "$JAVAC" -cp "$classpath" \
        -d "$TEST_DIR/classes" \
        "$TEST_DIR/src/JarValidationTest.java"

    log_info "Running test program..."

    # Run without java.library.path to test JAR extraction
    "$JAVA" -cp "$TEST_DIR/classes:$classpath" \
        --enable-native-access=ALL-UNNAMED \
        --add-opens=java.base/java.nio=ALL-UNNAMED \
        JarValidationTest
}

# Main
main() {
    log_info "DataFusion FFI Java JAR Validation"
    echo "========================================"

    # Clean up previous test
    rm -rf "$TEST_DIR"
    mkdir -p "$TEST_DIR/classes"

    # Find JAR
    local jar=$(find_jar)
    log_info "Found JAR: $jar"

    # List natives
    list_natives "$jar" || true

    # Check platforms
    check_platforms "$jar" || true

    # Download deps and run test
    local dep_dir=$(download_dependencies)
    run_test "$jar" "$dep_dir"

    log_info "Validation complete!"
}

main "$@"
