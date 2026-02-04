#!/bin/bash
#
# Organize native library artifacts downloaded from CI into the expected directory structure.
# Called during the build-jar CI job after downloading per-platform artifacts.
#
# Expected input:  datafusion-ffi-java/build/native-artifacts/native-{platform}/lib*
# Expected output: datafusion-ffi-java/build/native-artifacts/{platform}/lib*
#

set -e

ARTIFACTS_DIR="${1:-datafusion-ffi-java/build/native-artifacts}"
PLATFORMS=("osx_64" "osx_arm64" "linux_64" "linux_arm64" "windows_64")

mkdir -p "$ARTIFACTS_DIR"

for platform in "${PLATFORMS[@]}"; do
    mkdir -p "$ARTIFACTS_DIR/$platform"
    src="$ARTIFACTS_DIR/native-$platform"
    if [ -d "$src" ]; then
        mv "$src"/* "$ARTIFACTS_DIR/$platform/"
        rm -rf "$src"
    fi
done

echo "Native artifacts structure:"
find "$ARTIFACTS_DIR" -type f
