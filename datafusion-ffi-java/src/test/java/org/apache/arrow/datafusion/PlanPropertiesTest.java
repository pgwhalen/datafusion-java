package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.datafusion.physical_plan.Boundedness;
import org.apache.arrow.datafusion.physical_plan.EmissionType;
import org.apache.arrow.datafusion.physical_plan.PlanProperties;
import org.junit.jupiter.api.Test;

/** Tests for {@link PlanProperties}. */
public class PlanPropertiesTest {

  @Test
  void testDefaultConstructor() {
    PlanProperties props = new PlanProperties();
    assertEquals(1, props.outputPartitioning());
    assertEquals(EmissionType.INCREMENTAL, props.emissionType());
    assertEquals(Boundedness.BOUNDED, props.boundedness());
  }
}
