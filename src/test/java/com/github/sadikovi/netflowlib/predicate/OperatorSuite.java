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

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import com.github.sadikovi.netflowlib.codegen.CodeGenContext;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;
import com.github.sadikovi.netflowlib.predicate.FilterApi;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
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
  public void testInPredicate() {
    IntColumn col = new IntColumn("col1", 0);
    HashSet<Integer> set = new HashSet<Integer>();
    In predicate = FilterApi.in(col, set);
    assertTrue(predicate.getValues().isEmpty());
    ValueInspector inspector = predicate.getValueInspector(Integer.class);
    inspector.update(0);
    assertFalse(inspector.getResult());
  }

  @Test
  public void testCodeGenEqInteger() {
    CodeGenContext.reset();
    String code = FilterApi.eq(new IntColumn("col", 0), 10).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "col0 == 10");
  }

  @Test
  public void testCodeGenEqLong() {
    CodeGenContext.reset();
    String code = FilterApi.eq(new LongColumn("col", 0), 10L).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "col0 == 10L");
  }

  @Test
  public void testCodeGenEqShort() {
    CodeGenContext.reset();
    String code = FilterApi.eq(new ShortColumn("col", 0), (short) 10).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "col0 == (short) 10");
  }

  @Test
  public void testCodeGenGtInteger() {
    CodeGenContext.reset();
    String code = FilterApi.gt(new IntColumn("col", 0), 10).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "col0 > 10");
  }

  @Test
  public void testCodeGenGeInteger() {
    CodeGenContext.reset();
    String code = FilterApi.ge(new IntColumn("col", 0), 10).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "col0 >= 10");
  }

  @Test
  public void testCodeGenLtInteger() {
    CodeGenContext.reset();
    String code = FilterApi.lt(new IntColumn("col", 0), 10).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "col0 < 10");
  }

  @Test
  public void testCodeGenLeInteger() {
    CodeGenContext.reset();
    String code = FilterApi.le(new IntColumn("col", 0), 10).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "col0 <= 10");
  }

  @Test
  public void testCodeGenInInteger1() {
    CodeGenContext.reset();
    HashSet<Integer> values = new HashSet<Integer>();
    String code = FilterApi.in(new IntColumn("col", 0), values).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "false");
  }

  @Test
  public void testCodeGenInInteger2() {
    CodeGenContext.reset();
    HashSet<Integer> values = new HashSet<Integer>();
    values.add(10);
    String code = FilterApi.in(new IntColumn("col", 0), values).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "col0 == 10");
  }

  @Test
  public void testCodeGenInInteger3() {
    CodeGenContext.reset();
    HashSet<Integer> values = new HashSet<Integer>();
    values.add(10);
    values.add(11);
    values.add(12);
    String code = FilterApi.in(new IntColumn("col", 0), values).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "col0 == 10 || col0 == 11 || col0 == 12");
  }

  @Test
  public void testCodeGenAndInteger1() {
    CodeGenContext.reset();
    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 4);
    String code =
      FilterApi.and(
        FilterApi.eq(col1, 1),
        FilterApi.gt(col2, 2)
      ).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "(col10 == 1) && (col20 > 2)");
  }

  @Test
  public void testCodeGenAndInteger2() {
    CodeGenContext.reset();
    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 4);
    String code =
      FilterApi.and(
        FilterApi.and(
          FilterApi.ge(col1, 1),
          FilterApi.le(col1, 3)
        ),
        FilterApi.gt(col2, 2)
      ).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "((col10 >= 1) && (col10 <= 3)) && (col20 > 2)");
  }

  @Test
  public void testCodeGenAndInteger3() {
    CodeGenContext.reset();
    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 4);
    String code = FilterApi.and(FilterApi.trivial(false), FilterApi.trivial(true)).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "(false) && (true)");
  }

  @Test
  public void testCodeGenOrInteger1() {
    CodeGenContext.reset();
    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 4);
    String code =
      FilterApi.or(
        FilterApi.eq(col1, 1),
        FilterApi.gt(col2, 2)
      ).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "(col10 == 1) || (col20 > 2)");
  }

  @Test
  public void testCodeGenOrInteger2() {
    CodeGenContext.reset();
    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 4);
    HashSet<Integer> values = new HashSet<Integer>();
    values.add(10);
    values.add(11);
    String code =
      FilterApi.or(
        FilterApi.and(
          FilterApi.ge(col1, 1),
          FilterApi.le(col1, 3)
        ),
        FilterApi.in(col2, values)
      ).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "((col10 >= 1) && (col10 <= 3)) || (col20 == 10 || col20 == 11)");
  }

  @Test
  public void testCodeGenNotInteger() {
    CodeGenContext.reset();
    IntColumn col1 = new IntColumn("col1", 0);
    String code = FilterApi.not(FilterApi.gt(col1, 1)).
      generate(CodeGenContext.getOrCreate());
    assertEquals(code, "!(col10 > 1)");
  }

  @Test
  public void testCodeGenTrivialInteger() {
    CodeGenContext.reset();
    assertEquals(FilterApi.trivial(false).generate(CodeGenContext.getOrCreate()), "false");
    assertEquals(FilterApi.trivial(true).generate(CodeGenContext.getOrCreate()), "true");
  }
}
