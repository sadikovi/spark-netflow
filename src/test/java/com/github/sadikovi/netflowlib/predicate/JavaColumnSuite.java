/*
 * Copyright 2016 sadikovi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    // initialize byte column and check min, max
    ByteColumn col1 = new ByteColumn("name", 1);
    assertEquals(col1.getColumnName(), "name");
    assertEquals(col1.getColumnOffset(), 1);
    assertEquals(col1.getColumnType(), Byte.class);
    assertSame(Byte.class.cast(col1.getMin()), (byte) 0);
    assertSame(Byte.class.cast(col1.getMax()) - Byte.MAX_VALUE, 0);

    ByteColumn col2 = new ByteColumn("name", 1, (byte) 1, (byte) 2);
    assertEquals(col2.getColumnName(), "name");
    assertEquals(col2.getColumnOffset(), 1);
    assertEquals(col2.getColumnType(), Byte.class);
    assertSame(Byte.class.cast(col2.getMin()), (byte) 1);
    assertSame(Byte.class.cast(col2.getMax()), (byte) 2);

    // initialize short column and check min, max
    ShortColumn col3 = new ShortColumn("name", 2);
    assertEquals(col3.getColumnName(), "name");
    assertEquals(col3.getColumnOffset(), 2);
    assertEquals(col3.getColumnType(), Short.class);
    assertSame(Short.class.cast(col3.getMin()), (short) 0);
    assertSame(Short.class.cast(col3.getMax()) - Short.MAX_VALUE, 0);

    ShortColumn col4 = new ShortColumn("name", 2, (short) 10, (short) 100);
    assertEquals(col4.getColumnName(), "name");
    assertEquals(col4.getColumnOffset(), 2);
    assertEquals(col4.getColumnType(), Short.class);
    assertSame(Short.class.cast(col4.getMin()), (short) 10);
    assertSame(Short.class.cast(col4.getMax()), (short) 100);

    IntColumn col5 = new IntColumn("name", 3);
    assertEquals(col5.getColumnName(), "name");
    assertEquals(col5.getColumnOffset(), 3);
    assertEquals(col5.getColumnType(), Integer.class);
    assertSame(Integer.class.cast(col5.getMin()), 0);
    assertSame(Integer.class.cast(col5.getMax()) - Integer.MAX_VALUE, 0);

    IntColumn col6 = new IntColumn("name", 3, 10, 100);
    assertEquals(col6.getColumnName(), "name");
    assertEquals(col6.getColumnOffset(), 3);
    assertEquals(col6.getColumnType(), Integer.class);
    assertSame(Integer.class.cast(col6.getMin()), 10);
    assertSame(Integer.class.cast(col6.getMax()), 100);

    LongColumn col7 = new LongColumn("name", 4);
    assertEquals(col7.getColumnName(), "name");
    assertEquals(col7.getColumnOffset(), 4);
    assertEquals(col7.getColumnType(), Long.class);
    assertSame(Long.class.cast(col7.getMin()), (long) 0);
    assertSame(Long.class.cast(col7.getMax()) - Long.MAX_VALUE, (long) 0);

    LongColumn col8 = new LongColumn("name", 4, 10, 100);
    assertEquals(col8.getColumnName(), "name");
    assertEquals(col8.getColumnOffset(), 4);
    assertEquals(col8.getColumnType(), Long.class);
    assertSame(Long.class.cast(col8.getMin()), (long) 10);
    assertSame(Long.class.cast(col8.getMax()), (long) 100);
  }

  @Test
  public void testWrongOffset() {
    try {
      new ByteColumn("name", -1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("Wrong offset"));
    }

    try {
      new ShortColumn("name", -1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("Wrong offset"));
    }

    try {
      new IntColumn("name", -1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("Wrong offset"));
    }

    try {
      new LongColumn("name", -1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("Wrong offset"));
    }
  }

  @Test
  public void testWrongMinMax() {
    try {
      new ByteColumn("name", 1, (byte) 2, (byte) 1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("is greater than"));
    }

    try {
      new ShortColumn("name", 1, (byte) 2, (byte) 1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("is greater than"));
    }

    try {
      new IntColumn("name", 1, (byte) 2, (byte) 1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("is greater than"));
    }

    try {
      new LongColumn("name", 1, (byte) 2, (byte) 1);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), containsString("is greater than"));
    }
  }

  @Test
  public void testEquality() {
    IntColumn col1 = new IntColumn("col1", 0);

    IntColumn col2 = new IntColumn("col1", 0);
    assertTrue(col1.equals(col2));

    IntColumn col3 = new IntColumn("col2", 0);
    assertFalse(col1.equals(col3));

    IntColumn col4 = new IntColumn("col1", 1);
    assertFalse(col1.equals(col4));

    ShortColumn col5 = new ShortColumn("col1", 0);
    assertFalse(col1.equals(col5));

    IntColumn col6 = new IntColumn("col1", 0, 10, 100);
    assertTrue(col1.equals(col6));
  }
}
