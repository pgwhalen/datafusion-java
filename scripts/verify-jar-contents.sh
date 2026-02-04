#!/bin/bash
#
# Verify that the built JAR contains native libraries.
# Called during the build-jar CI job after packaging.
#

set -e

JAR_DIR="${1:-datafusion-ffi-java/build/libs}"

JAR=$(find "$JAR_DIR" -name "datafusion-ffi-java-*.jar" \
    ! -name "*-sources.jar" ! -name "*-javadoc.jar" -type f | head -1)

if [ -z "$JAR" ]; then
    echo "ERROR: No JAR file found in $JAR_DIR"
    exit 1
fi

echo "JAR: $JAR"
echo "Native libraries in JAR:"
unzip -l "$JAR" | grep natives/ || echo "No natives found!"
