package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.datafusion.logical_expr.WindowFrame;
import org.apache.arrow.datafusion.logical_expr.WindowFrameBound;
import org.apache.arrow.datafusion.logical_expr.WindowFrameUnits;
import org.junit.jupiter.api.Test;

/** Tests for {@link WindowFrame} and {@link WindowFrameBound}. */
public class WindowFrameTest {

  @Test
  void testWindowFrameAccessors() {
    WindowFrame frame =
        new WindowFrame(
            WindowFrameUnits.ROWS,
            new WindowFrameBound.CurrentRow(),
            new WindowFrameBound.Following(null));
    assertEquals(WindowFrameUnits.ROWS, frame.frameType());
    assertInstanceOf(WindowFrameBound.CurrentRow.class, frame.startBound());
    assertInstanceOf(WindowFrameBound.Following.class, frame.endBound());
  }

  @Test
  void testWindowFrameBoundVariants() {
    var currentRow = new WindowFrameBound.CurrentRow();
    assertNotNull(currentRow);

    var preceding = new WindowFrameBound.Preceding(null);
    assertNull(preceding.value());

    var following = new WindowFrameBound.Following(null);
    assertNull(following.value());
  }
}
