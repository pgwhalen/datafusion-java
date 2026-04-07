package org.apache.arrow.datafusion.physical_plan;

/**
 * Properties of an execution plan that describe its output characteristics.
 *
 * <p>Example:
 *
 * {@snippet :
 * PlanProperties props = new PlanProperties(4, EmissionType.INCREMENTAL, Boundedness.BOUNDED);
 * // or use defaults: 1 partition, incremental, bounded
 * PlanProperties defaults = new PlanProperties();
 * }
 *
 * @param outputPartitioning The number of output partitions
 * @param emissionType How the plan produces output
 * @param boundedness Whether the plan's input data is finite or infinite
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/physical_plan/struct.PlanProperties.html">Rust
 *     DataFusion: PlanProperties</a>
 */
public record PlanProperties(
    int outputPartitioning, EmissionType emissionType, Boundedness boundedness) {

  /**
   * Returns the number of output partitions.
   *
   * @return the output partitioning count
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/physical_plan/struct.PlanProperties.html#method.output_partitioning">Rust
   *     DataFusion: PlanProperties::output_partitioning</a>
   */
  public int outputPartitioning() {
    return outputPartitioning;
  }

  /**
   * Returns how the plan produces output.
   *
   * @return the emission type
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/physical_plan/struct.PlanProperties.html#structfield.emission_type">Rust
   *     DataFusion: PlanProperties::emission_type</a>
   */
  public EmissionType emissionType() {
    return emissionType;
  }

  /**
   * Returns whether the plan's input data is finite or infinite.
   *
   * @return the boundedness
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/physical_plan/struct.PlanProperties.html#structfield.boundedness">Rust
   *     DataFusion: PlanProperties::boundedness</a>
   */
  public Boundedness boundedness() {
    return boundedness;
  }

  /** Creates a PlanProperties with default values: 1 partition, incremental emission, bounded. */
  public PlanProperties() {
    this(1, EmissionType.INCREMENTAL, Boundedness.BOUNDED);
  }
}
