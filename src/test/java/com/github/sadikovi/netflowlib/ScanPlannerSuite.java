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

package com.github.sadikovi.netflowlib;

import java.util.HashMap;
import java.util.HashSet;

import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.github.sadikovi.netflowlib.ScanPlanner;
import com.github.sadikovi.netflowlib.Strategies.ScanStrategy;
import com.github.sadikovi.netflowlib.Strategies.SkipScan;
import com.github.sadikovi.netflowlib.Strategies.FullScan;
import com.github.sadikovi.netflowlib.Strategies.FilterScan;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;

import com.github.sadikovi.netflowlib.predicate.FilterApi;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.eq;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.gt;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.ge;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.lt;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.le;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.in;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.and;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.or;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.trivial;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;

import com.github.sadikovi.netflowlib.statistics.Statistics;
import com.github.sadikovi.netflowlib.statistics.StatisticsTypes.IntStatistics;

public class ScanPlannerSuite {
  @Test(expected = IllegalArgumentException.class)
  public void testColumnsNullFailure() {
    Column[] cols = null;
    FilterPredicate tree = null;

    ScanPlanner.buildStrategy(cols, tree, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColumnsEmptyFailure() {
    IntColumn[] cols = new IntColumn[0];
    FilterPredicate tree = null;

    ScanPlanner.buildStrategy(cols, tree, null);
  }

  @Test
  public void testSkipScan1() {
    IntColumn[] cols = new IntColumn[] {new IntColumn("col1", 0), new IntColumn("col2", 4)};
    FilterPredicate tree = trivial(false);

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, null);
    assertSame(ss.getClass(), SkipScan.class);
    assertTrue(ss.skipScan());
  }

  @Test
  public void testSkipScan2() {
    IntColumn[] cols = new IntColumn[] {new IntColumn("col1", 0), new IntColumn("col2", 4)};
    FilterPredicate tree = and(trivial(false), eq(new IntColumn("col1", 0), 10));

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, null);
    assertSame(ss.getClass(), SkipScan.class);
    assertTrue(ss.skipScan());
  }

  @Test
  public void testSkipScan3() {
    IntColumn[] cols = new IntColumn[] {new IntColumn("col1", 0), new IntColumn("col2", 4)};

    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 0);
    IntColumn col3 = new IntColumn("col3", 0);
    FilterPredicate tree = or(and(lt(col1, 0), eq(col2, 10)), and(ge(col3, 10), le(col3, 19)));

    // statistics on column 3, even when it is a different object
    HashMap<Column, Statistics> stats = new HashMap<Column, Statistics>();
    stats.put(col3, new IntStatistics(21, 25));

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), SkipScan.class);
    assertTrue(ss.skipScan());
  }

  @Test
  public void testFullScan1() {
    IntColumn[] cols = new IntColumn[] {new IntColumn("col1", 0), new IntColumn("col2", 4)};
    FilterPredicate tree = null;

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, null);
    assertSame(ss.getClass(), FullScan.class);
    assertFalse(ss.skipScan());

    // it is an equivalent of having trivial predicate "true"
    tree = trivial(true);
    ss = ScanPlanner.buildStrategy(cols, tree, null);
    assertSame(ss.getClass(), FullScan.class);
    assertFalse(ss.skipScan());
  }

  @Test
  public void testFullScan2() {
    IntColumn[] cols = new IntColumn[] {new IntColumn("col1", 0), new IntColumn("col2", 4)};

    IntColumn col1 = new IntColumn("col1", 0);
    FilterPredicate tree = and(trivial(true), ge(col1, 0));

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, null);
    assertSame(ss.getClass(), FullScan.class);
    assertFalse(ss.skipScan());
  }

  @Test
  public void testFullScan3() {
    IntColumn[] cols = new IntColumn[] {new IntColumn("col1", 0), new IntColumn("col2", 4)};

    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 0);
    IntColumn col3 = new IntColumn("col3", 0);
    FilterPredicate tree = and(
      and(ge(col1, 10), le(col1, 20)),
      or(
        eq(col2, 100),
        or(le(col3, -1), ge(col3, 0))
      )
    );

    HashMap<Column, Statistics> stats = new HashMap<Column, Statistics>();
    stats.put(col1, new IntStatistics(12, 18));

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), FullScan.class);
    assertFalse(ss.skipScan());
  }

  @Test
  public void testFullScan4() {
    IntColumn[] cols = new IntColumn[] {new IntColumn("col1", 0), new IntColumn("col2", 4)};

    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 0);
    IntColumn col3 = new IntColumn("col3", 0);
    FilterPredicate tree = or(
      and(ge(col1, 10), le(col1, 20)),
      or(
        eq(col2, 100),
        or(le(col3, -1), ge(col3, 0))
      )
    );

    HashMap<Column, Statistics> stats = new HashMap<Column, Statistics>();

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), FullScan.class);
    assertFalse(ss.skipScan());
  }

  @Test
  public void testPredicateScan1() {
    IntColumn[] cols = new IntColumn[] {new IntColumn("col1", 0), new IntColumn("col2", 4)};

    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 0);
    IntColumn col3 = new IntColumn("col3", 0);
    FilterPredicate tree = and(
      and(ge(col1, 10), le(col1, 20)),
      or(
        eq(col2, 100),
        or(le(col3, -1), ge(col3, 0))
      )
    );

    HashMap<Column, Statistics> stats = new HashMap<Column, Statistics>();

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), FilterScan.class);
    assertFalse(ss.skipScan());
  }

  @Test
  public void testPredicateScan2() {
    IntColumn[] cols = new IntColumn[] {new IntColumn("col1", 0), new IntColumn("col2", 4),
      new IntColumn("col3", 8), new IntColumn("col4", 12), new IntColumn("col5", 16),
      new IntColumn("col6", 20), new IntColumn("col7", 24)};

    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 4);
    FilterPredicate tree = and(
      and(ge(col1, 10), le(col1, 20)),
      eq(col2, 100)
    );

    HashMap<Column, Statistics> stats = new HashMap<Column, Statistics>();
    stats.put(col1, new IntStatistics(12, 18));

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), FilterScan.class);
    assertFalse(ss.skipScan());
  }

  @Test
  public void testPredicateScan3() {
    // Test of "In" predicate optimization. Currently, if "In" set contains only one element we
    // still use set, instead of converting it into equality predicate which is supposed to be
    // faster. We also test empty set optimization.
    IntColumn col1 = new IntColumn("col1", 0);
    IntColumn col2 = new IntColumn("col2", 4);
    IntColumn[] cols = new IntColumn[] {col1, col2};

    FilterPredicate tree = null;
    ScanStrategy ss = null;
    HashMap<Column, Statistics> stats = null;
    HashSet<Integer> values = null;

    // Case 1: set is empty, we should skip scan
    tree = in(col1, new HashSet<Integer>());
    stats = new HashMap<Column, Statistics>();
    ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), SkipScan.class);
    assertTrue(ss.skipScan());

    // Case 2: set contains only one value and no statistics
    values = new HashSet<Integer>();
    values.add(10);
    tree = in(col1, values);

    stats = new HashMap<Column, Statistics>();
    ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), FilterScan.class);
    assertFalse(ss.skipScan());

    // Case 3: set contains only one value and we have statistics on a column
    values = new HashSet<Integer>();
    values.add(10);
    tree = in(col1, values);

    stats = new HashMap<Column, Statistics>();
    stats.put(col1, new IntStatistics(1, 3));
    ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), SkipScan.class);
    assertTrue(ss.skipScan());

    // Case 4: set contains more than one value and we do not have any statistics
    values = new HashSet<Integer>();
    values.add(10);
    values.add(20);
    tree = in(col1, values);

    stats = new HashMap<Column, Statistics>();
    ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), FilterScan.class);
    assertFalse(ss.skipScan());
  }
}
