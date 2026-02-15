package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Verifies the cross-language file naming rule from ffi.md: every Java *Ffi or *Handle class that
 * makes downcalls into Rust must have a matching Rust source file (class name minus suffix,
 * CamelCase→snake_case, .rs) that contains all the #[no_mangle] functions it references.
 */
class CrossLanguageNamingTest {

  private static final Path JAVA_SRC_DIR = Paths.get("src/main/java/org/apache/arrow/datafusion");
  private static final Path RUST_SRC_DIR = Paths.get("../datafusion-ffi-native/src");

  /**
   * Regex to extract downcall function names from Java source. Matches both:
   *
   * <ul>
   *   <li>{@code NativeUtil.downcall("datafusion_xxx", ...)}
   *   <li>{@code .find("datafusion_xxx")}
   * </ul>
   */
  private static final Pattern DOWNCALL_PATTERN = Pattern.compile("\"(datafusion_[a-z_0-9]+)\"");

  /** Regex to extract #[no_mangle] function definitions from Rust source. */
  private static final Pattern NO_MANGLE_FN_PATTERN =
      Pattern.compile("pub\\s+(unsafe\\s+)?extern\\s+\"C\"\\s+fn\\s+(datafusion_[a-z_0-9]+)");

  @Test
  void everyFfiClassHasMatchingRustFile() throws IOException {
    assertTrue(Files.isDirectory(JAVA_SRC_DIR), "Java source directory not found: " + JAVA_SRC_DIR);
    assertTrue(Files.isDirectory(RUST_SRC_DIR), "Rust source directory not found: " + RUST_SRC_DIR);

    List<String> violations = new ArrayList<>();

    // Find all *Ffi.java and *Handle.java files
    List<Path> javaFiles;
    try (Stream<Path> stream = Files.list(JAVA_SRC_DIR)) {
      javaFiles =
          stream
              .filter(p -> p.getFileName().toString().endsWith(".java"))
              .filter(
                  p -> {
                    String name = p.getFileName().toString().replace(".java", "");
                    return name.endsWith("Ffi")
                        || name.endsWith("Handle")
                        || name.equals("NativeUtil");
                  })
              .sorted()
              .toList();
    }

    assertFalse(javaFiles.isEmpty(), "No *Ffi.java or *Handle.java files found");

    // Track mappings for reporting
    Map<String, Set<String>> classFunctions = new LinkedHashMap<>();

    for (Path javaFile : javaFiles) {
      String className = javaFile.getFileName().toString().replace(".java", "");

      // Compute expected Rust file name
      String baseName;
      if (className.endsWith("Ffi")) {
        baseName = className.substring(0, className.length() - 3);
      } else if (className.endsWith("Handle")) {
        baseName = className.substring(0, className.length() - 6);
      } else {
        baseName = className; // e.g., NativeUtil → native_util.rs
      }
      String rustFileName = camelToSnake(baseName) + ".rs";

      // Extract downcall function names from Java source
      String javaSource = Files.readString(javaFile);
      Set<String> functionNames = new LinkedHashSet<>();
      Matcher matcher = DOWNCALL_PATTERN.matcher(javaSource);
      while (matcher.find()) {
        functionNames.add(matcher.group(1));
      }

      if (functionNames.isEmpty()) {
        continue; // Pure wrapper, no downcalls
      }

      classFunctions.put(className, functionNames);

      // Check that the Rust file exists
      Path rustFile = RUST_SRC_DIR.resolve(rustFileName);
      if (!Files.exists(rustFile)) {
        violations.add(
            String.format(
                "%s: expected Rust file %s does not exist (functions: %s)",
                className, rustFileName, functionNames));
        continue;
      }

      // Extract #[no_mangle] function names from Rust file
      String rustSource = Files.readString(rustFile);
      Set<String> rustFunctions = new LinkedHashSet<>();
      Matcher rustMatcher = NO_MANGLE_FN_PATTERN.matcher(rustSource);
      while (rustMatcher.find()) {
        rustFunctions.add(rustMatcher.group(2));
      }

      // Verify every Java-referenced function exists in the Rust file
      for (String fn : functionNames) {
        if (!rustFunctions.contains(fn)) {
          violations.add(
              String.format(
                  "%s: function '%s' not found in %s (found: %s)",
                  className, fn, rustFileName, rustFunctions));
        }
      }
    }

    if (!violations.isEmpty()) {
      fail(
          "Cross-language naming violations:\n  - "
              + String.join("\n  - ", violations)
              + "\n\nChecked "
              + classFunctions.size()
              + " classes");
    }
  }

  /** Convert CamelCase to snake_case. */
  private static String camelToSnake(String camel) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < camel.length(); i++) {
      char c = camel.charAt(i);
      if (Character.isUpperCase(c)) {
        if (i > 0) {
          sb.append('_');
        }
        sb.append(Character.toLowerCase(c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
