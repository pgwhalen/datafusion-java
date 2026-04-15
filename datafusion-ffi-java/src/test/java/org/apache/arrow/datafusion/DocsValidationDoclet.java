package org.apache.arrow.datafusion;

import com.sun.source.doctree.*;
import com.sun.source.util.DocTrees;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;

/**
 * Custom Doclet that validates public Javadoc: {@code @see} docs.rs links and {@code @snippet} code
 * examples.
 *
 * <p>Uses the Doclet API for compiler-backed access to types, methods, and parsed Javadoc tags
 * instead of fragile regex-based source file parsing.
 *
 * <p>By default, only checks presence and format (offline, fast). Set system property {@code
 * verifyHttp=true} (via {@code -J-DverifyHttp=true}) to also validate each URL returns HTTP 200.
 */
public class DocsValidationDoclet implements Doclet {

  private Reporter reporter;
  private final List<String> failures = new ArrayList<>();

  // Valid docs.rs URL pattern - allows struct, trait, enum, fn, type, and index.html.
  // Accepts any crate (datafusion-*, object_store, etc.) to support cross-crate references.
  private static final Pattern VALID_DOCS_URL =
      Pattern.compile(
          "https://docs\\.rs/[\\w-]+/\\d+\\.\\d+\\.\\d+/.*"
              + "(?:(?:struct|trait|enum|fn|type)\\.[\\w]+\\.html|index\\.html)"
              + "(?:(?:#method|#structfield|#tymethod)\\.[\\w_]+)?");

  /** The base package — used as prefix for validated packages. */
  private static final String VALIDATED_PACKAGE = "org.apache.arrow.datafusion";

  /** Validated subpackages (in addition to the base package). */
  private static final Set<String> VALIDATED_SUBPACKAGES =
      Set.of(
          "common",
          "logical_expr",
          "physical_plan",
          "physical_expr",
          "catalog",
          "datasource",
          "dataframe",
          "execution");

  /** Classes whose methods map to Rust free functions (fn.X.html), not struct/trait methods. */
  private static final Set<String> FREE_FUNCTION_CLASSES = Set.of("Functions");

  /** Checks if a package should be validated (base package or a listed subpackage). */
  private static boolean isValidatedPackage(String packageName) {
    if (packageName.equals(VALIDATED_PACKAGE)) return true;
    for (String sub : VALIDATED_SUBPACKAGES) {
      if (packageName.equals(VALIDATED_PACKAGE + "." + sub)) return true;
    }
    return false;
  }

  /** Internal class name patterns - these never need @see links or snippets. */
  private static boolean isInternalClass(String name) {
    return name.endsWith("Bridge")
        || name.endsWith("Adapter")
        || name.endsWith("Converter")
        || name.endsWith("Ffi")
        || name.endsWith("Handle")
        || name.startsWith("Native")
        || name.equals("Errors");
  }

  /**
   * Public types that are pure Java helpers with no Rust counterpart, or map to non-DataFusion
   * types.
   */
  private static final Set<String> NO_SEE_LINK_REQUIRED =
      Set.of(
          "WhenThen",
          // RecordBatchReader maps to arrow::record_batch::RecordBatch (Arrow, not DataFusion)
          "RecordBatchReader",
          // ColumnAssignment maps to Rust's (String, Expr) tuple — no standalone Rust type
          "ColumnAssignment");

  /** Name mismatch exceptions: Java name -> expected Rust label in @see. */
  private static final Map<String, String> NAME_EXCEPTIONS =
      Map.of("Functions", "datafusion::prelude");

  /** Method names that never need @see or snippets (Java infrastructure, no Rust equivalent). */
  private static final Set<String> EXCLUDED_METHODS =
      Set.of(
          "close",
          "toString",
          "hashCode",
          "equals",
          "compareTo",
          "valueOf",
          "values",
          "iterator",
          "spliterator",
          "stream",
          "builder",
          "of",
          "bridge",
          "fromBridge");

  /** Per-class method exclusions for methods with no Rust equivalent. */
  private static final Map<String, Set<String>> CLASS_METHOD_EXCLUSIONS =
      Map.ofEntries(
          Map.entry("ScalarValue", Set.of("getObject")),
          Map.entry(
              "SendableRecordBatchStream",
              Set.of(
                  // Java Arrow iteration patterns, no Rust equivalent
                  "getVectorSchemaRoot", "loadNextBatch", "lookup", "getDictionaryIds")),
          Map.entry(
              "SimpleScalarUDF",
              Set.of(
                  // Inherited from ScalarUDF interface, documented there
                  "name", "signature", "returnField", "invoke", "coerceTypes")),
          Map.entry(
              "FileScanConfig",
              Set.of(
                  // No Rust equivalent
                  "projection", "partition")),
          Map.entry(
              "Spans",
              Set.of(
                  // Rust Spans is a newtype wrapper, no named field
                  "spans")),
          Map.entry(
              "CsvReadOptions",
              Set.of(
                  // Java-only proto serialization helper
                  "encodeOptions")),
          Map.entry(
              "ParquetReadOptions",
              Set.of(
                  // Java-only proto serialization helper
                  "encodeOptions")),
          Map.entry(
              "NdJsonReadOptions",
              Set.of(
                  // Java-only proto serialization helper
                  "encodeOptions")),
          Map.entry(
              "ArrowReadOptions",
              Set.of(
                  // Java-only proto serialization helper
                  "encodeOptions")),
          Map.entry(
              "CsvOptions",
              Set.of(
                  // Java-only proto serialization helper
                  "encodeOptions")),
          Map.entry(
              "JsonOptions",
              Set.of(
                  // Java-only proto serialization helper
                  "encodeOptions")),
          Map.entry(
              "LogicalPlan",
              Set.of(
                  // Java-only bridge accessor
                  "bridge")),
          Map.entry(
              "PhysicalExpr",
              Set.of(
                  // Java-only bridge accessor
                  "bridge")),
          Map.entry(
              "ParquetOptions",
              Set.of(
                  // Java-only proto serialization helper
                  "encodeOptions")),
          Map.entry(
              "SessionContext",
              Set.of(
                  // Java-only bridge accessor
                  "bridge")));

  /** Types whose method-level @see validation is deferred (links not yet added). */
  private static final Set<String> METHOD_VALIDATION_DEFERRED = Set.of();

  // ── Snippet validation constants ──

  /** Types that do not require class-level or method-level {@code @snippet} examples. */
  private static final Set<String> NO_SNIPPET_REQUIRED =
      Set.of(
          // Pure Java helper record, created via CaseBuilder
          "WhenThen",
          // Simple (column, expr) pair used in TableProvider.update()
          "ColumnAssignment");

  /**
   * Per-class method exclusions for snippet validation. Methods listed here do not need a
   * {@code @snippet} or {@code @link} cross-reference in their Javadoc. Reasons include: overload
   * secondaries that duplicate the primary overload's example, methods fully documented by the
   * class-level snippet, or methods with no meaningful standalone example.
   */
  private static final Map<String, Set<String>> CLASS_SNIPPET_EXCLUSIONS =
      Map.ofEntries(
          // --- SessionContext: overload secondaries and simple accessors ---
          Map.entry(
              "SessionContext",
              Set.of(
                  // Java-only bridge accessor
                  "bridge")),
          // --- DataFrame: overload secondaries ---
          Map.entry("DataFrame", Set.of()),
          // --- Functions: lit() overloads cross-reference lit(int) ---
          Map.entry("Functions", Set.of()),
          // --- Provider interfaces: methods shown in class-level example ---
          Map.entry("CatalogProvider", Set.of()),
          Map.entry("SchemaProvider", Set.of()),
          // --- SendableRecordBatchStream: Java Arrow iteration, no Rust equivalent ---
          Map.entry(
              "SendableRecordBatchStream",
              Set.of("getVectorSchemaRoot", "loadNextBatch", "lookup", "getDictionaryIds")),
          // --- SimpleScalarUDF: inherited from ScalarUDF, documented there ---
          Map.entry(
              "SimpleScalarUDF",
              Set.of("name", "signature", "returnField", "invoke", "coerceTypes")),
          // --- Misc: proto helpers and bridge accessors ---
          Map.entry("CsvReadOptions", Set.of("encodeOptions")),
          Map.entry("ParquetReadOptions", Set.of("encodeOptions")),
          Map.entry("NdJsonReadOptions", Set.of("encodeOptions")),
          Map.entry("ArrowReadOptions", Set.of("encodeOptions")),
          Map.entry("CsvOptions", Set.of("encodeOptions")),
          Map.entry("JsonOptions", Set.of("encodeOptions")),
          Map.entry("ParquetOptions", Set.of("encodeOptions")),
          Map.entry("LogicalPlan", Set.of("bridge")),
          Map.entry("PhysicalExpr", Set.of("bridge")),
          Map.entry("ScalarValue", Set.of("getObject")),
          Map.entry("FileScanConfig", Set.of("projection", "partition")),
          Map.entry("Spans", Set.of("spans")));

  /** Types whose snippet validation is deferred (snippets not yet added). */
  private static final Set<String> SNIPPET_VALIDATION_DEFERRED = Set.of();

  @Override
  public void init(Locale locale, Reporter reporter) {
    this.reporter = reporter;
  }

  @Override
  public String getName() {
    return "DocsValidationDoclet";
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.latest();
  }

  @Override
  public Set<? extends Option> getSupportedOptions() {
    // Gradle's Javadoc task passes standard doclet options (-d, -doctitle, etc.)
    // that are not recognized by custom doclets. Declare them here so they are
    // silently accepted and ignored.
    return Set.of(
        ignoredOption("-d", 1),
        ignoredOption("-doctitle", 1),
        ignoredOption("-windowtitle", 1),
        ignoredOption("--no-timestamp", 0),
        ignoredOption("-notimestamp", 0),
        ignoredOption("-quiet", 0));
  }

  private static Option ignoredOption(String name, int argCount) {
    return new Option() {
      @Override
      public int getArgumentCount() {
        return argCount;
      }

      @Override
      public String getDescription() {
        return "Ignored (passed by Gradle)";
      }

      @Override
      public Kind getKind() {
        return Kind.STANDARD;
      }

      @Override
      public List<String> getNames() {
        return List.of(name);
      }

      @Override
      public String getParameters() {
        return argCount > 0 ? "<value>" : "";
      }

      @Override
      public boolean process(String option, List<String> arguments) {
        return true;
      }
    };
  }

  @Override
  public boolean run(DocletEnvironment docEnv) {
    DocTrees docTrees = docEnv.getDocTrees();
    boolean verifyHttp = "true".equalsIgnoreCase(System.getProperty("verifyHttp"));
    Set<String> urlsToCheck = new LinkedHashSet<>();

    // Collect all top-level, non-internal type names and their public methods for staleness checks.
    Set<String> allTypeNames = new LinkedHashSet<>();
    Map<String, Set<String>> allPublicMethods = new LinkedHashMap<>();

    for (TypeElement type : ElementFilter.typesIn(docEnv.getIncludedElements())) {
      String packageName =
          docEnv.getElementUtils().getPackageOf(type).getQualifiedName().toString();
      if (!isValidatedPackage(packageName)) continue;
      if (type.getEnclosingElement().getKind() != ElementKind.PACKAGE) continue;

      String typeName = type.getSimpleName().toString();
      if (isInternalClass(typeName)) continue;

      allTypeNames.add(typeName);

      Set<String> methods = new LinkedHashSet<>();
      for (ExecutableElement method : ElementFilter.methodsIn(type.getEnclosedElements())) {
        if (method.getModifiers().contains(Modifier.PUBLIC)) {
          methods.add(method.getSimpleName().toString());
        }
      }
      allPublicMethods.put(typeName, methods);

      validateType(type, docTrees, urlsToCheck);
    }

    validateExclusionStaleness(allTypeNames, allPublicMethods);

    if (verifyHttp && !urlsToCheck.isEmpty()) {
      validateHttpUrls(urlsToCheck);
    }

    if (!failures.isEmpty()) {
      reporter.print(
          Diagnostic.Kind.ERROR,
          failures.size()
              + " documentation validation failure(s):\n"
              + String.join("\n", failures));
    }

    return failures.isEmpty();
  }

  /**
   * Checks that every entry in the exclusion sets/maps references a real type or method. Reports
   * failures for stale entries that no longer match the codebase.
   */
  private void validateExclusionStaleness(
      Set<String> allTypeNames, Map<String, Set<String>> allPublicMethods) {

    for (String name : NO_SEE_LINK_REQUIRED) {
      if (!allTypeNames.contains(name)) {
        failures.add("Stale NO_SEE_LINK_REQUIRED entry: \"" + name + "\" is not a known type");
      }
    }

    for (String name : FREE_FUNCTION_CLASSES) {
      if (!allTypeNames.contains(name)) {
        failures.add("Stale FREE_FUNCTION_CLASSES entry: \"" + name + "\" is not a known type");
      }
    }

    for (String name : NAME_EXCEPTIONS.keySet()) {
      if (!allTypeNames.contains(name)) {
        failures.add("Stale NAME_EXCEPTIONS key: \"" + name + "\" is not a known type");
      }
    }

    for (String name : METHOD_VALIDATION_DEFERRED) {
      if (!allTypeNames.contains(name)) {
        failures.add(
            "Stale METHOD_VALIDATION_DEFERRED entry: \"" + name + "\" is not a known type");
      }
    }

    for (Map.Entry<String, Set<String>> entry : CLASS_METHOD_EXCLUSIONS.entrySet()) {
      String typeName = entry.getKey();
      if (!allTypeNames.contains(typeName)) {
        failures.add("Stale CLASS_METHOD_EXCLUSIONS key: \"" + typeName + "\" is not a known type");
        continue;
      }
      Set<String> actualMethods = allPublicMethods.getOrDefault(typeName, Set.of());
      for (String methodName : entry.getValue()) {
        if (!actualMethods.contains(methodName)) {
          failures.add(
              "Stale CLASS_METHOD_EXCLUSIONS entry: \""
                  + typeName
                  + "."
                  + methodName
                  + "\" is not a public method on "
                  + typeName);
        }
      }
    }

    // Snippet exclusion staleness checks
    for (String name : NO_SNIPPET_REQUIRED) {
      if (!allTypeNames.contains(name)) {
        failures.add("Stale NO_SNIPPET_REQUIRED entry: \"" + name + "\" is not a known type");
      }
    }

    for (String name : SNIPPET_VALIDATION_DEFERRED) {
      if (!allTypeNames.contains(name)) {
        failures.add(
            "Stale SNIPPET_VALIDATION_DEFERRED entry: \"" + name + "\" is not a known type");
      }
    }

    for (Map.Entry<String, Set<String>> entry : CLASS_SNIPPET_EXCLUSIONS.entrySet()) {
      String typeName = entry.getKey();
      if (!allTypeNames.contains(typeName)) {
        failures.add(
            "Stale CLASS_SNIPPET_EXCLUSIONS key: \"" + typeName + "\" is not a known type");
        continue;
      }
      Set<String> actualMethods = allPublicMethods.getOrDefault(typeName, Set.of());
      for (String methodName : entry.getValue()) {
        if (!actualMethods.contains(methodName)) {
          failures.add(
              "Stale CLASS_SNIPPET_EXCLUSIONS entry: \""
                  + typeName
                  + "."
                  + methodName
                  + "\" is not a public method on "
                  + typeName);
        }
      }
    }
  }

  private void validateType(TypeElement type, DocTrees docTrees, Set<String> urls) {
    String typeName = type.getSimpleName().toString();

    if (isInternalClass(typeName)) return;

    DocCommentTree docComment = docTrees.getDocCommentTree(type);

    // --- @see link validation ---
    if (!NO_SEE_LINK_REQUIRED.contains(typeName)) {
      validateSeeLinks(type, typeName, docComment, docTrees, urls);
    }

    // --- Snippet validation ---
    if (!NO_SNIPPET_REQUIRED.contains(typeName)
        && !SNIPPET_VALIDATION_DEFERRED.contains(typeName)) {
      validateSnippets(type, typeName, docComment, docTrees);
    }
  }

  private void validateSeeLinks(
      TypeElement type,
      String typeName,
      DocCommentTree docComment,
      DocTrees docTrees,
      Set<String> urls) {

    SeeLink classLink = extractDocsRsSeeLink(docComment);

    // --- Class-level @see presence ---
    if (classLink == null) {
      failures.add(typeName + ": public type missing @see docs.rs link");
      return; // No point checking methods if class link is missing
    }

    // --- Class-level @see format ---
    if (!VALID_DOCS_URL.matcher(classLink.url).matches()) {
      failures.add(typeName + ": invalid docs.rs URL format: " + classLink.url);
    }

    // --- Class-level name match ---
    String expected = NAME_EXCEPTIONS.getOrDefault(typeName, typeName);
    if (!classLink.rustLabel.equals(expected)) {
      failures.add(
          typeName
              + ": name mismatch - Java '"
              + typeName
              + "' vs Rust label '"
              + classLink.rustLabel
              + "' (expected '"
              + expected
              + "')");
    }

    urls.add(classLink.url.replaceFirst("#.*", ""));

    // --- Method-level @see validation ---
    if (METHOD_VALIDATION_DEFERRED.contains(typeName)) return;

    Set<String> classExclusions = CLASS_METHOD_EXCLUSIONS.getOrDefault(typeName, Set.of());

    for (ExecutableElement method : ElementFilter.methodsIn(type.getEnclosedElements())) {
      if (!method.getModifiers().contains(Modifier.PUBLIC)) continue;

      String methodName = method.getSimpleName().toString();
      if (EXCLUDED_METHODS.contains(methodName)) continue;
      if (classExclusions.contains(methodName)) continue;

      DocCommentTree methodDoc = docTrees.getDocCommentTree(method);
      SeeLink methodLink = extractDocsRsSeeLink(methodDoc);

      if (methodLink == null) {
        failures.add(typeName + "." + methodName + ": method missing @see docs.rs link");
      } else {
        if (!FREE_FUNCTION_CLASSES.contains(typeName)
            && !methodLink.url.contains("#method.")
            && !methodLink.url.contains("#structfield.")
            && !methodLink.url.contains("#tymethod.")) {
          failures.add(
              typeName
                  + "."
                  + methodName
                  + ": @see URL missing #method./#structfield./#tymethod. anchor: "
                  + methodLink.url);
        }
        if (!VALID_DOCS_URL.matcher(methodLink.url).matches()) {
          failures.add(
              typeName + "." + methodName + ": invalid docs.rs URL format: " + methodLink.url);
        }
        urls.add(methodLink.url.replaceFirst("#.*", ""));
      }
    }
  }

  private void validateSnippets(
      TypeElement type, String typeName, DocCommentTree docComment, DocTrees docTrees) {

    // --- Class-level snippet presence ---
    if (!hasSnippetOrLink(docComment)) {
      failures.add(typeName + ": public type missing {@snippet} example in class-level Javadoc");
    }

    // --- Method-level snippet presence ---
    Set<String> snippetExclusions = CLASS_SNIPPET_EXCLUSIONS.getOrDefault(typeName, Set.of());
    Set<String> recordComponents = getRecordComponentNames(type);

    for (ExecutableElement method : ElementFilter.methodsIn(type.getEnclosedElements())) {
      if (!method.getModifiers().contains(Modifier.PUBLIC)) continue;

      String methodName = method.getSimpleName().toString();
      if (EXCLUDED_METHODS.contains(methodName)) continue;
      if (snippetExclusions.contains(methodName)) continue;
      if (recordComponents.contains(methodName)) continue;

      DocCommentTree methodDoc = docTrees.getDocCommentTree(method);
      if (!hasSnippetOrLink(methodDoc)) {
        failures.add(
            typeName
                + "."
                + methodName
                + ": method missing {@snippet} example or {@link} cross-reference");
      }
    }
  }

  /**
   * Checks if a doc comment body contains an {@code @snippet} inline tag or an {@code @link}
   * cross-reference (used by overload secondaries to reference the primary overload's snippet).
   *
   * <p>Uses both parsed tree inspection and text-based fallback because google-java-format may
   * collapse {@code @snippet} blocks to a single line that some JDK versions parse differently.
   */
  private boolean hasSnippetOrLink(DocCommentTree docComment) {
    if (docComment == null) return false;
    // Check parsed tree nodes (works when the Javadoc parser produces SNIPPET/LINK nodes)
    for (DocTree node : docComment.getFullBody()) {
      if (node.getKind() == DocTree.Kind.SNIPPET || node.getKind() == DocTree.Kind.LINK) {
        return true;
      }
    }
    // Fallback: check raw text for {@snippet or {@link (handles collapsed single-line format)
    String text = docComment.toString();
    return text.contains("{@snippet") || text.contains("{@link");
  }

  /** Returns the names of record components for a record type, or empty set for non-records. */
  private Set<String> getRecordComponentNames(TypeElement type) {
    if (type.getKind() != ElementKind.RECORD) return Set.of();
    Set<String> names = new LinkedHashSet<>();
    for (RecordComponentElement comp : type.getRecordComponents()) {
      names.add(comp.getSimpleName().toString());
    }
    return names;
  }

  /**
   * Extracts a docs.rs {@code @see} link from a Javadoc comment.
   *
   * <p>Looks for {@code @see <a href="...docs.rs/...">Rust DataFusion: Label</a>} tags.
   */
  private SeeLink extractDocsRsSeeLink(DocCommentTree docComment) {
    if (docComment == null) return null;

    for (DocTree blockTag : docComment.getBlockTags()) {
      if (blockTag.getKind() != DocTree.Kind.SEE) continue;

      SeeTree seeTree = (SeeTree) blockTag;
      SeeLink link = parseSeeTree(seeTree);
      if (link != null && link.url.contains("docs.rs")) {
        return link;
      }
    }
    return null;
  }

  /**
   * Parses an {@code @see} tag into a URL and Rust label.
   *
   * <p>Handles multi-line {@code @see <a href="...">Rust DataFusion: Name</a>} by walking the
   * DocTree nodes (StartElement, Text, EndElement) instead of regex on raw source text.
   */
  private SeeLink parseSeeTree(SeeTree seeTree) {
    String url = null;
    StringBuilder labelBuilder = new StringBuilder();
    boolean inAnchor = false;

    for (DocTree node : seeTree.getReference()) {
      switch (node.getKind()) {
        case START_ELEMENT -> {
          StartElementTree start = (StartElementTree) node;
          if ("a".equalsIgnoreCase(start.getName().toString())) {
            inAnchor = true;
            for (DocTree attr : start.getAttributes()) {
              if (attr instanceof AttributeTree at
                  && "href".equalsIgnoreCase(at.getName().toString())) {
                url = at.getValue().stream().map(Object::toString).collect(Collectors.joining());
              }
            }
          }
        }
        case END_ELEMENT -> {
          EndElementTree end = (EndElementTree) node;
          if ("a".equalsIgnoreCase(end.getName().toString())) {
            inAnchor = false;
          }
        }
        case TEXT -> {
          if (inAnchor) {
            labelBuilder.append(((TextTree) node).getBody());
          }
        }
        default -> {
          // Ignore other node types (e.g., entity references)
        }
      }
    }

    if (url == null) return null;

    String label = labelBuilder.toString().replaceAll("\\s+", " ").trim();
    // Parse "Rust {crate}: Label" from the anchor text (e.g., "Rust DataFusion: Foo" or
    // "Rust object_store: Bar"). The label after the colon is used for name matching.
    String prefix = "Rust ";
    if (label.startsWith(prefix)) {
      int colonIdx = label.indexOf(": ", prefix.length());
      if (colonIdx < 0) return null;
      String rustName = label.substring(colonIdx + 2).trim();
      // For method links like "Type::method", extract just the type part for class-level,
      // but keep the full label for the caller to interpret.
      return new SeeLink(url, rustName);
    }

    return null;
  }

  private void validateHttpUrls(Set<String> urls) {
    try (HttpClient client = HttpClient.newHttpClient()) {
      for (String url : urls) {
        try {
          HttpRequest request =
              HttpRequest.newBuilder()
                  .uri(URI.create(url))
                  .method("HEAD", HttpRequest.BodyPublishers.noBody())
                  .build();
          HttpResponse<Void> response =
              client.send(request, HttpResponse.BodyHandlers.discarding());
          if (response.statusCode() != 200) {
            failures.add("HTTP " + response.statusCode() + ": " + url);
          }
        } catch (Exception e) {
          failures.add("HTTP error for " + url + ": " + e.getMessage());
        }
      }
    }
  }

  private record SeeLink(String url, String rustLabel) {}
}
