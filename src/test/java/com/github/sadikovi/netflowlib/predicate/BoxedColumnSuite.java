package com.github.sadikovi.netflowlib.predicate;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.github.sadikovi.netflowlib.predicate.BoxedColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;

public class BoxedColumnSuite {
  @Test
  public void testEquality() {
    BoxedColumn bcol1 = new BoxedColumn(new IntColumn((byte) 1, 0), (byte) 0);

    BoxedColumn bcol2 = new BoxedColumn(new IntColumn((byte) 1, 0), (byte) 0);
    assertTrue(bcol1.equals(bcol2));

    BoxedColumn bcol3 = new BoxedColumn(new IntColumn((byte) 1, 0), (byte) 3);
    assertTrue(bcol1.equals(bcol3));

    BoxedColumn bcol4 = new BoxedColumn(new IntColumn((byte) 2, 0), (byte) 0);
    assertFalse(bcol1.equals(bcol4));

    BoxedColumn bcol5 = new BoxedColumn(new ShortColumn((byte) 2, 0), (byte) 0);
    assertFalse(bcol1.equals(bcol5));
  }

  @Test
  public void testByteFlags() {
    IntColumn col = new IntColumn((byte) 1, 0);
    BoxedColumn bcol = new BoxedColumn(col);

    assertFalse(bcol.hasFlag(BoxedColumn.FLAG_PRUNED));
    assertFalse(bcol.hasFlag(BoxedColumn.FLAG_FILTERED));

    bcol.setFlag(BoxedColumn.FLAG_PRUNED);
    assertTrue(bcol.hasFlag(BoxedColumn.FLAG_PRUNED));

    bcol.setFlag(BoxedColumn.FLAG_FILTERED);
    assertTrue(bcol.hasFlag(BoxedColumn.FLAG_PRUNED));
    assertTrue(bcol.hasFlag(BoxedColumn.FLAG_FILTERED));
  }
}
