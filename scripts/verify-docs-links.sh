#!/usr/bin/env bash
# Verifies all docs.rs @see links in public Java files:
#   1. Every URL returns HTTP 200
#   2. The Rust type name in the link label matches the Java class/interface/enum/record name
#      (unless listed in EXCEPTIONS below)
#
# Usage: ./scripts/verify-docs-links.sh
# Exit code: 0 if all links are valid, 1 if any failures

set -eo pipefail

JAVA_SRC="datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion"

# Exceptions: Java name|expected Rust label (when they intentionally differ)
# Add entries here when the Java class name intentionally differs from the Rust type name.
EXCEPTIONS="
"

get_expected_rust_name() {
  local java_name="$1"
  local match
  match=$(echo "$EXCEPTIONS" | grep "^${java_name}|" | head -1 | cut -d'|' -f2)
  if [ -n "$match" ]; then
    echo "$match"
  else
    echo "$java_name"
  fi
}

FAIL=0
CHECKED=0

# Find all Java files with docs.rs links (recursively, including config/)
while IFS= read -r file; do
  # Extract the Java type name from the file (public class/interface/enum/record)
  java_type=$(perl -ne 'if (/^public\s+(?:final\s+)?(?:class|interface|enum|record)\s+(\w+)/) { print $1; exit }' "$file")
  if [ -z "$java_type" ]; then
    continue
  fi

  # Extract the docs.rs URL (may span multiple lines due to spotless formatting)
  url=$(perl -0777 -ne 'if (m{(https://docs\.rs/[^"]+)}) { print $1 }' "$file")
  if [ -z "$url" ]; then
    continue
  fi
  # Remove any whitespace/newlines that spotless may have inserted
  url=$(echo "$url" | tr -d '[:space:]')

  # Extract the Rust type name from the link label: "Rust DataFusion: TypeName"
  # The label may be split across lines with * continuations
  rust_label=$(perl -0777 -ne '
    if (/\@see\s+<a[^>]*>\s*Rust\s*\n?\s*\*?\s*DataFusion:\s*(\w+)/s) {
      print $1;
    }
  ' "$file")

  CHECKED=$((CHECKED + 1))

  # 1. Check URL returns 200
  http_code=$(curl -s -o /dev/null -w "%{http_code}" "$url")
  if [ "$http_code" != "200" ]; then
    echo "FAIL [HTTP $http_code] $file"
    echo "  URL: $url"
    FAIL=$((FAIL + 1))
    continue
  fi

  # 2. Check name match
  if [ -n "$rust_label" ]; then
    expected_rust=$(get_expected_rust_name "$java_type")
    if [ "$rust_label" != "$expected_rust" ]; then
      echo "FAIL [NAME MISMATCH] $file"
      echo "  Java type:  $java_type"
      echo "  Rust label: $rust_label"
      echo "  Expected:   $expected_rust"
      echo "  (Add to EXCEPTIONS in scripts/verify-docs-links.sh if intentional)"
      FAIL=$((FAIL + 1))
    fi
  fi
done < <(grep -rl 'docs\.rs/datafusion' "$JAVA_SRC")

echo ""
echo "Checked $CHECKED files, $FAIL failures."
if [ "$FAIL" -gt 0 ]; then
  exit 1
else
  echo "All docs.rs links are valid."
  exit 0
fi
