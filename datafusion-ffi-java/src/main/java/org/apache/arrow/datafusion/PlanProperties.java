package org.apache.arrow.datafusion;

/**
 * Properties of an execution plan that describe its output characteristics.
 *
 * @param outputPartitioning The number of output partitions
 * @param emissionType How the plan produces output
 * @param boundedness Whether the plan's input data is finite or infinite
 */
public record PlanProperties(
    int outputPartitioning, EmissionType emissionType, Boundedness boundedness) {

  /**
   * Creates a PlanProperties with default values: 1 partition, incremental emission, bounded.
   *
   * @return The default PlanProperties
   */
  public static PlanProperties defaults() {
    return new PlanProperties(1, EmissionType.INCREMENTAL, Boundedness.BOUNDED);
  }
}
