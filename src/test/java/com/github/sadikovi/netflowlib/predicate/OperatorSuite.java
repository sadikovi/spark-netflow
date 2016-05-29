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

import java.util.HashSet;

import static org.hamcrest.CoreMatchers.containsString;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.github.sadikovi.netflowlib.predicate.Columns.ByteColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;
import com.github.sadikovi.netflowlib.predicate.FilterApi;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
import com.github.sadikovi.netflowlib.predicate.Operators.ColumnPredicate;
import com.github.sadikovi.netflowlib.predicate.Operators.Eq;
import com.github.sadikovi.netflowlib.predicate.Operators.Gt;
import com.github.sadikovi.netflowlib.predicate.Operators.In;
import com.github.sadikovi.netflowlib.predicate.Operators.And;

public class OperatorSuite {
  @Test
  public void testEquality() {
    IntColumn col = new IntColumn("col1", 0);

    Eq pr1 = FilterApi.eq(col, 10);
    assertTrue(pr1.equals(pr1));

    Eq pr2 = FilterApi.eq(col, 10);
    assertTrue(pr1.equals(pr2));

    Gt pr3 = FilterApi.gt(col, 10);
    assertFalse(pr1.equals(pr3));

    HashSet<Integer> set = new HashSet<Integer>();
    set.add(10);
    In pr4 = FilterApi.in(col, set);
    assertFalse(pr1.equals(pr4));

    And pr5 = FilterApi.and(pr1, pr2);
    assertFalse(pr1.equals(pr5));
  }

  @Test
  public void testNullColumn() {
    boolean fetchedException = false;
    try {
      FilterApi.eq(null, 10);
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);

    fetchedException = false;
    try {
      FilterApi.gt(null, 10);
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);

    fetchedException = false;
    try {
      FilterApi.lt(null, 10);
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testNullValue() {
    // no exception should be thrown
    FilterApi.eq(new IntColumn("col1", 0), 10);
    FilterApi.gt(new IntColumn("col1", 0), 10);
    FilterApi.lt(new IntColumn("col1", 0), 10);
  }

  @Test
  public void testInValues1() {
    // `In` predicate has value as null, but also has `values` set which can be empty
    HashSet<Integer> values = new HashSet<Integer>();
    In in = FilterApi.in(new IntColumn("col1", 0), values);
    assertTrue(in.getValues().isEmpty());
    assertEquals(in.getValues(), values);
    assertNotSame(in.getValues(), values);
  }

  @Test
  public void testInValues2() {
    HashSet<Integer> values = new HashSet<Integer>();
    values.add(1);
    values.add(2);
    In in = FilterApi.in(new IntColumn("col1", 0), values);
    assertSame(in.getValues().size(), 2);
  }

  @Test
  public void testBinaryNull() {
    boolean fetchedException;
    IntColumn col = new IntColumn("col", 0);
    // check that left and right nodes are null
    fetchedException = false;
    try {
      FilterApi.and(null, null);
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);

    // check that left node is null
    fetchedException = false;
    try {
      FilterApi.or(null, FilterApi.eq(col, 1));
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);

    // check that right node is null
    fetchedException = false;
    try {
      FilterApi.and(FilterApi.eq(col, 1), null);
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  @Test
  public void testUnaryNull() {
    boolean fetchedException;
    // check that left and right nodes are null
    fetchedException = false;
    try {
      FilterApi.not(null);
    } catch (IllegalArgumentException iae) {
      fetchedException = true;
    }
    assertTrue(fetchedException);
  }

  // Test predicate update
  private boolean checkResult(ColumnPredicate predicate, byte value) {
    ValueInspector inspector = predicate.inspector();
    inspector.update(value);
    return inspector.getResult();
  }

  private boolean checkResult(ColumnPredicate predicate, short value) {
    ValueInspector inspector = predicate.inspector();
    inspector.update(value);
    return inspector.getResult();
  }

  private boolean checkResult(ColumnPredicate predicate, int value) {
    ValueInspector inspector = predicate.inspector();
    inspector.update(value);
    return inspector.getResult();
  }

  private boolean checkResult(ColumnPredicate predicate, long value) {
    ValueInspector inspector = predicate.inspector();
    inspector.update(value);
    return inspector.getResult();
  }

  private final ByteColumn byteCol = new ByteColumn("col", 0);
  private final ShortColumn shortCol = new ShortColumn("col", 0);
  private final IntColumn intCol = new IntColumn("col", 0);
  private final LongColumn longCol = new LongColumn("col", 0);

  @Test
  public void testEqInspector() {
    assertFalse(checkResult(FilterApi.eq(byteCol, (byte) 10), (byte) 9));
    assertTrue(checkResult(FilterApi.eq(byteCol, (byte) 10), (byte) 10));
    assertFalse(checkResult(FilterApi.eq(byteCol, (byte) 10), (byte) 11));

    assertFalse(checkResult(FilterApi.eq(shortCol, (short) 10), (short) 9));
    assertTrue(checkResult(FilterApi.eq(shortCol, (short) 10), (short) 10));
    assertFalse(checkResult(FilterApi.eq(shortCol, (short) 10), (short) 11));

    assertFalse(checkResult(FilterApi.eq(intCol, 10), 9));
    assertTrue(checkResult(FilterApi.eq(intCol, 10), 10));
    assertFalse(checkResult(FilterApi.eq(intCol, 10), 11));

    assertFalse(checkResult(FilterApi.eq(longCol, 10L), 9L));
    assertTrue(checkResult(FilterApi.eq(longCol, 10L), 10L));
    assertFalse(checkResult(FilterApi.eq(longCol, 10L), 11L));
  }

  @Test
  public void testGtInspector() {
    assertFalse(checkResult(FilterApi.gt(byteCol, (byte) 10), (byte) 9));
    assertFalse(checkResult(FilterApi.gt(byteCol, (byte) 10), (byte) 10));
    assertTrue(checkResult(FilterApi.gt(byteCol, (byte) 10), (byte) 11));

    assertFalse(checkResult(FilterApi.gt(shortCol, (short) 10), (short) 9));
    assertFalse(checkResult(FilterApi.gt(shortCol, (short) 10), (short) 10));
    assertTrue(checkResult(FilterApi.gt(shortCol, (short) 10), (short) 11));

    assertFalse(checkResult(FilterApi.gt(intCol, 10), 9));
    assertFalse(checkResult(FilterApi.gt(intCol, 10), 10));
    assertTrue(checkResult(FilterApi.gt(intCol, 10), 11));

    assertFalse(checkResult(FilterApi.gt(longCol, 10L), 9L));
    assertFalse(checkResult(FilterApi.gt(longCol, 10L), 10L));
    assertTrue(checkResult(FilterApi.gt(longCol, 10L), 11L));
  }

  @Test
  public void testGeInspector() {
    assertFalse(checkResult(FilterApi.ge(byteCol, (byte) 10), (byte) 9));
    assertTrue(checkResult(FilterApi.ge(byteCol, (byte) 10), (byte) 10));
    assertTrue(checkResult(FilterApi.ge(byteCol, (byte) 10), (byte) 11));

    assertFalse(checkResult(FilterApi.ge(shortCol, (short) 10), (short) 9));
    assertTrue(checkResult(FilterApi.ge(shortCol, (short) 10), (short) 10));
    assertTrue(checkResult(FilterApi.ge(shortCol, (short) 10), (short) 11));

    assertFalse(checkResult(FilterApi.ge(intCol, 10), 9));
    assertTrue(checkResult(FilterApi.ge(intCol, 10), 10));
    assertTrue(checkResult(FilterApi.ge(intCol, 10), 11));

    assertFalse(checkResult(FilterApi.ge(longCol, 10L), 9L));
    assertTrue(checkResult(FilterApi.ge(longCol, 10L), 10L));
    assertTrue(checkResult(FilterApi.ge(longCol, 10L), 11L));
  }

  @Test
  public void testLtInspector() {
    assertTrue(checkResult(FilterApi.lt(byteCol, (byte) 10), (byte) 9));
    assertFalse(checkResult(FilterApi.lt(byteCol, (byte) 10), (byte) 10));
    assertFalse(checkResult(FilterApi.lt(byteCol, (byte) 10), (byte) 11));

    assertTrue(checkResult(FilterApi.lt(shortCol, (short) 10), (short) 9));
    assertFalse(checkResult(FilterApi.lt(shortCol, (short) 10), (short) 10));
    assertFalse(checkResult(FilterApi.lt(shortCol, (short) 10), (short) 11));

    assertTrue(checkResult(FilterApi.lt(intCol, 10), 9));
    assertFalse(checkResult(FilterApi.lt(intCol, 10), 10));
    assertFalse(checkResult(FilterApi.lt(intCol, 10), 11));

    assertTrue(checkResult(FilterApi.lt(longCol, 10L), 9L));
    assertFalse(checkResult(FilterApi.lt(longCol, 10L), 10L));
    assertFalse(checkResult(FilterApi.lt(longCol, 10L), 11L));
  }

  @Test
  public void testLeInspector() {
    assertTrue(checkResult(FilterApi.le(byteCol, (byte) 10), (byte) 9));
    assertTrue(checkResult(FilterApi.le(byteCol, (byte) 10), (byte) 10));
    assertFalse(checkResult(FilterApi.le(byteCol, (byte) 10), (byte) 11));

    assertTrue(checkResult(FilterApi.le(shortCol, (short) 10), (short) 9));
    assertTrue(checkResult(FilterApi.le(shortCol, (short) 10), (short) 10));
    assertFalse(checkResult(FilterApi.le(shortCol, (short) 10), (short) 11));

    assertTrue(checkResult(FilterApi.le(intCol, 10), 9));
    assertTrue(checkResult(FilterApi.le(intCol, 10), 10));
    assertFalse(checkResult(FilterApi.le(intCol, 10), 11));

    assertTrue(checkResult(FilterApi.le(longCol, 10L), 9L));
    assertTrue(checkResult(FilterApi.le(longCol, 10L), 10L));
    assertFalse(checkResult(FilterApi.le(longCol, 10L), 11L));
  }

  @Test
  public void testInInspector() {
    HashSet<Number> values = new HashSet<Number>();
    values.add((byte) 10);
    values.add((short) 10);
    values.add(10);
    values.add(10L);

    assertFalse(checkResult(FilterApi.in(byteCol, values), (byte) 9));
    assertTrue(checkResult(FilterApi.in(byteCol, values), (byte) 10));
    assertFalse(checkResult(FilterApi.in(byteCol, values), (byte) 11));

    assertFalse(checkResult(FilterApi.in(shortCol, values), (short) 9));
    assertTrue(checkResult(FilterApi.in(shortCol, values), (short) 10));
    assertFalse(checkResult(FilterApi.in(shortCol, values), (short) 11));

    assertFalse(checkResult(FilterApi.in(intCol, values), 9));
    assertTrue(checkResult(FilterApi.in(intCol, values), 10));
    assertFalse(checkResult(FilterApi.in(intCol, values), 11));

    assertFalse(checkResult(FilterApi.in(longCol, values), 9L));
    assertTrue(checkResult(FilterApi.in(longCol, values), 10L));
    assertFalse(checkResult(FilterApi.in(longCol, values), 11L));
  }
}
