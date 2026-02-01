#!/bin/bash

echo "DataFusion Panama FFM Build and Run Script"
echo "==========================================="

# Check Java version
echo "Checking Java version..."
java --version

# Build the native library first
echo ""
echo "Building native DataFusion FFI library..."
cd ../datafusion-panama-ffi
cargo build --release
if [ $? -ne 0 ]; then
    echo "Failed to build native library"
    exit 1
fi

# Return to Java project
cd ../datafusion-panama-java

# Set library path for the native library
export DYLD_LIBRARY_PATH="../datafusion-panama-ffi/target/release:$DYLD_LIBRARY_PATH"
export LD_LIBRARY_PATH="../datafusion-panama-ffi/target/release:$LD_LIBRARY_PATH"

echo ""
echo "Building Java project..."
./gradlew build
if [ $? -ne 0 ]; then
    echo "Failed to build Java project"
    exit 1
fi

echo ""
echo "Running DataFusion Hello World..."
./gradlew run