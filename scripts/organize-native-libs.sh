#!/bin/bash
#
# Organize native library artifacts downloaded from CI into the expected directory structure.
# Called during the build-jar CI job after downloading per-platform artifacts.
#
# Expected input:  datafusion-ffi-java/build/native-artifacts/native-{platform}/lib*
# Expected output: datafusion-ffi-java/build/native-artifacts/{platform}/lib*
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/platforms.sh"

ARTIFACTS_DIR="${1:-datafusion-ffi-java/build/native-artifacts}"

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
