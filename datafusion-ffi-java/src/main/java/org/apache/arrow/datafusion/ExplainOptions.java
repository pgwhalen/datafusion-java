package org.apache.arrow.datafusion;

import java.util.Map;

/**
 * EXPLAIN statement configuration options. Maps to DataFusion's {@code ExplainOptions}.
 *
 * <p>All fields are nullable. A null value means the DataFusion default is used.
 *
 * @param logicalPlanOnly Whether EXPLAIN should only show the logical plan
 * @param physicalPlanOnly Whether EXPLAIN should only show the physical plan
 * @param showStatistics Whether to show statistics in EXPLAIN output
 * @param showSizes Whether to show sizes in EXPLAIN output
 * @param showSchema Whether to show schema information in EXPLAIN output
 * @param format Output format for EXPLAIN statements
 * @param treeMaximumRenderWidth Maximum render width for tree-style EXPLAIN output
 * @param analyzeLevel Detail level for EXPLAIN ANALYZE output
 */
public record ExplainOptions(
    Boolean logicalPlanOnly,
    Boolean physicalPlanOnly,
    Boolean showStatistics,
    Boolean showSizes,
    Boolean showSchema,
    ExplainFormat format,
    Integer treeMaximumRenderWidth,
    ExplainAnalyzeLevel analyzeLevel) {

  private static final String PREFIX = "datafusion.explain.";

  /** Returns a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Writes non-null options to the map with proper dotted keys. */
  void writeTo(Map<String, String> map) {
    putIfPresent(map, PREFIX + "logical_plan_only", logicalPlanOnly);
    putIfPresent(map, PREFIX + "physical_plan_only", physicalPlanOnly);
    putIfPresent(map, PREFIX + "show_statistics", showStatistics);
    putIfPresent(map, PREFIX + "show_sizes", showSizes);
    putIfPresent(map, PREFIX + "show_schema", showSchema);
    if (format != null) {
      map.put(PREFIX + "format", format.toConfigValue());
    }
    putIfPresent(map, PREFIX + "tree_maximum_render_width", treeMaximumRenderWidth);
    if (analyzeLevel != null) {
      map.put(PREFIX + "analyze_level", analyzeLevel.toConfigValue());
    }
  }

  private static void putIfPresent(Map<String, String> map, String key, Object value) {
    if (value != null) {
      map.put(key, value.toString());
    }
  }

  /** Builder for {@link ExplainOptions}. */
  public static final class Builder {
    private Boolean logicalPlanOnly;
    private Boolean physicalPlanOnly;
    private Boolean showStatistics;
    private Boolean showSizes;
    private Boolean showSchema;
    private ExplainFormat format;
    private Integer treeMaximumRenderWidth;
    private ExplainAnalyzeLevel analyzeLevel;

    private Builder() {}

    /** Whether EXPLAIN should only show the logical plan. */
    public Builder logicalPlanOnly(boolean value) {
      this.logicalPlanOnly = value;
      return this;
    }

    /** Whether EXPLAIN should only show the physical plan. */
    public Builder physicalPlanOnly(boolean value) {
      this.physicalPlanOnly = value;
      return this;
    }

    /** Whether to show statistics in EXPLAIN output. */
    public Builder showStatistics(boolean value) {
      this.showStatistics = value;
      return this;
    }

    /** Whether to show sizes in EXPLAIN output. */
    public Builder showSizes(boolean value) {
      this.showSizes = value;
      return this;
    }

    /** Whether to show schema information in EXPLAIN output. */
    public Builder showSchema(boolean value) {
      this.showSchema = value;
      return this;
    }

    /** Output format for EXPLAIN statements. */
    public Builder format(ExplainFormat value) {
      this.format = value;
      return this;
    }

    /** Maximum render width for tree-style EXPLAIN output. */
    public Builder treeMaximumRenderWidth(int value) {
      this.treeMaximumRenderWidth = value;
      return this;
    }

    /** Detail level for EXPLAIN ANALYZE output. */
    public Builder analyzeLevel(ExplainAnalyzeLevel value) {
      this.analyzeLevel = value;
      return this;
    }

    /** Builds the {@link ExplainOptions}. */
    public ExplainOptions build() {
      return new ExplainOptions(
          logicalPlanOnly,
          physicalPlanOnly,
          showStatistics,
          showSizes,
          showSchema,
          format,
          treeMaximumRenderWidth,
          analyzeLevel);
    }
  }
}
