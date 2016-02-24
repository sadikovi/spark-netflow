package com.github.sadikovi.netflowlib.predicate;

import static org.hamcrest.CoreMatchers.containsString;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.github.sadikovi.netflowlib.predicate.Columns.ByteColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;

public class JavaColumnSuite {
  @Test
  public void testColumnInit() {
    byte name = (byte) 1;

    // initialize byte column and check min, max
    ByteColumn col1 = new ByteColumn(name, 1);
    assertEquals(col1.getColumnName(), name);
    assertEquals(col1.getColumnOffset(), 1);
    assertEquals(col1.getColumnType(), Byte.class);
    assertSame(col1.getMinValue(), (byte) 0);
    assertSame(col1.getMaxValue() - Byte.MAX_VALUE, 0);

    ByteColumn col2 = new ByteColumn(name, 1, (byte) 1, (byte) 2);
    assertEquals(col2.getColumnName(), name);
    assertEquals(col2.getColumnOffset(), 1);
    assertEquals(col2.getColumnType(), Byte.class);
    assertSame(col2.getMinValue(), (byte) 1);
    assertSame(col2.getMaxValue(), (byte) 2);

    // initialize short column and check min, max
    ShortColumn col3 = new ShortColumn(name, 2);
    assertEquals(col3.getColumnName(), name);
    assertEquals(col3.getColumnOffset(), 2);
    assertEquals(col3.getColumnType(), Short.class);
    assertSame(col3.getMinValue(), (short) 0);
    assertSame(col3.getMaxValue() - Short.MAX_VALUE, 0);

    ShortColumn col4 = new ShortColumn(name, 2, (short) 10, (short) 100);
    assertEquals(col4.getColumnName(), name);
    assertEquals(col4.getColumnOffset(), 2);
    assertEquals(col4.getColumnType(), Short.class);
    assertSame(col4.getMinValue(), (short) 10);
    assertSame(col4.getMaxValue(), (short) 100);

    IntColumn col5 = new IntColumn(name, 3);
    assertEquals(col5.getColumnName(), name);
    assertEquals(col5.getColumnOffset(), 3);
    assertEquals(col5.getColumnType(), Integer.class);
    assertSame(col5.getMinValue(), 0);
    assertSame(col5.getMaxValue() - Integer.MAX_VALUE, 0);

    IntColumn col6 = new IntColumn(name, 3, 10, 100);
    assertEquals(col6.getColumnName(), name);
    assertEquals(col6.getColumnOffset(), 3);
    assertEquals(col6.getColumnType(), Integer.class);
    assertSame(col6.getMinValue(), 10);
    assertSame(col6.getMaxValue(), 100);

    LongColumn col7 = new LongColumn(name, 4);
    assertEquals(col7.getColumnName(), name);
    assertEquals(col7.getColumnOffset(), 4);
    assertEquals(col7.getColumnType(), Long.class);
    assertSame(col7.getMinValue(), (long) 0);
    assertSame(col7.getMaxValue() - Long.MAX_VALUE, (long) 0);

    LongColumn col8 = new LongColumn(name, 4, 10, 100);
    assertEquals(col8.getColumnName(), name);
    assertEquals(col8.getColumnOffset(), 4);
    assertEquals(col8.getColumnType(), Long.class);
    assertSame(col8.getMinValue(), (long) 10);
    assertSame(col8.getMaxValue(), (long) 100);
  }

  @Test
  public void testWrongOffset() {
    try {
      new ByteColumn((byte) 1, -1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("Wrong offset"));
    }

    try {
      new ShortColumn((byte) 1, -1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("Wrong offset"));
    }

    try {
      new IntColumn((byte) 1, -1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("Wrong offset"));
    }

    try {
      new LongColumn((byte) 1, -1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("Wrong offset"));
    }
  }

  @Test
  public void testWrongMinMax() {
    try {
      new ByteColumn((byte) 1, 1, (byte) 2, (byte) 1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("is greater than"));
    }

    try {
      new ShortColumn((byte) 1, 1, (byte) 2, (byte) 1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("is greater than"));
    }

    try {
      new IntColumn((byte) 1, 1, (byte) 2, (byte) 1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("is greater than"));
    }

    try {
      new LongColumn((byte) 1, 1, (byte) 2, (byte) 1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("is greater than"));
    }
  }

  @Test
  public void testEquality() {
    IntColumn col1 = new IntColumn((byte) 1, 0);

    IntColumn col2 = new IntColumn((byte) 1, 0);
    assertTrue(col1.equals(col2));

    IntColumn col3 = new IntColumn((byte) 2, 0);
    assertFalse(col1.equals(col3));

    IntColumn col4 = new IntColumn((byte) 1, 1);
    assertFalse(col1.equals(col4));

    ShortColumn col5 = new ShortColumn((byte) 1, 0);
    assertFalse(col1.equals(col5));

    IntColumn col6 = new IntColumn((byte) 1, 0, 10, 100);
    assertTrue(col1.equals(col6));
  }
}
