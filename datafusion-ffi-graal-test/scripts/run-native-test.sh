#!/usr/bin/env bash
#
# Build and run the GraalVM native image test for datafusion-ffi-java.
#
# The native library is statically linked into the binary, so no
# DYLD_LIBRARY_PATH, LD_LIBRARY_PATH, or java.library.path is needed.
#
# Prerequisites:
#   - GraalVM JDK 25+ installed (set GRAALVM_HOME or auto-detected)
#   - Rust toolchain (the native library must be built first)
#   - JDK 21+ for Gradle daemon (set JAVA_HOME or auto-detected)
#
# Usage:
#   ./scripts/run-native-test.sh [--skip-build]
#
# Options:
#   --skip-build  Skip the native image build step and run the existing binary
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$PROJECT_DIR/.." && pwd)"

# ── Detect JAVA_HOME (for Gradle daemon) ─────────────────────────────────
if [ -z "${JAVA_HOME:-}" ]; then
  # Try common locations for JDK 21
  for candidate in \
    "$HOME/Library/Java/JavaVirtualMachines/temurin-21"*/Contents/Home \
    /usr/lib/jvm/java-21-* \
    /usr/local/opt/openjdk@21; do
    if [ -d "$candidate" ]; then
      export JAVA_HOME="$candidate"
      break
    fi
  done
fi
echo "JAVA_HOME: ${JAVA_HOME:-<not set>}"

# ── Detect GRAALVM_HOME ──────────────────────────────────────────────────
if [ -z "${GRAALVM_HOME:-}" ]; then
  for candidate in \
    "$HOME/Library/Java/JavaVirtualMachines/graalvm-25"*/Contents/Home \
    /usr/lib/jvm/graalvm-* \
    /usr/local/opt/graalvm; do
    if [ -d "$candidate" ]; then
      export GRAALVM_HOME="$candidate"
      break
    fi
  done
fi
echo "GRAALVM_HOME: ${GRAALVM_HOME:-<not set>}"

# ── Build native image (unless --skip-build) ──────────────────────────────
SKIP_BUILD=false
for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=true ;;
  esac
done

BINARY="$PROJECT_DIR/build/native/nativeCompile/datafusion-ffi-graal-test"

if [ "$SKIP_BUILD" = false ]; then
  echo ""
  echo "=== Building native image ==="
  "$ROOT_DIR/gradlew" -p "$ROOT_DIR" :datafusion-ffi-graal-test:nativeCompile
fi

if [ ! -f "$BINARY" ]; then
  echo "ERROR: Native binary not found at $BINARY"
  echo "Run without --skip-build to build it."
  exit 1
fi

# ── Run native image ─────────────────────────────────────────────────────
echo ""
echo "=== Running native image test ==="

# No library path needed — native library is statically linked into the binary
"$BINARY"
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
  echo "=== Native image test PASSED ==="
else
  echo "=== Native image test FAILED (exit code: $EXIT_CODE) ==="
fi

exit $EXIT_CODE