package com.github.sadikovi.netflowlib;

import java.util.HashMap;

import org.junit.Test;
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
import static com.github.sadikovi.netflowlib.predicate.FilterApi.and;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.or;
import static com.github.sadikovi.netflowlib.predicate.FilterApi.trivial;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;

import com.github.sadikovi.netflowlib.statistics.StatisticsTypes.IntStatistics;
import com.github.sadikovi.netflowlib.statistics.StatisticsTypes.GenericStatistics;

public class ScanPlannerSuite {
  @Test(expected = IllegalArgumentException.class)
  public <T extends Comparable<T>> void testColumnsNullFailure() {
    Column<T>[] cols = null;
    FilterPredicate tree = null;

    ScanPlanner.buildStrategy(cols, tree, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public <T extends Comparable<T>> void testColumnsEmptyFailure() {
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
    HashMap<Column<Integer>, GenericStatistics<Integer>> stats =
      new HashMap<Column<Integer>, GenericStatistics<Integer>>();
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

    HashMap<Column<Integer>, GenericStatistics<Integer>> stats =
      new HashMap<Column<Integer>, GenericStatistics<Integer>>();
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

    HashMap<Column<Integer>, GenericStatistics<Integer>> stats =
      new HashMap<Column<Integer>, GenericStatistics<Integer>>();

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

    HashMap<Column<Integer>, GenericStatistics<Integer>> stats =
      new HashMap<Column<Integer>, GenericStatistics<Integer>>();

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

    HashMap<Column<Integer>, GenericStatistics<Integer>> stats =
      new HashMap<Column<Integer>, GenericStatistics<Integer>>();
    stats.put(col1, new IntStatistics(12, 18));

    ScanStrategy ss = ScanPlanner.buildStrategy(cols, tree, stats);
    assertSame(ss.getClass(), FilterScan.class);
    assertFalse(ss.skipScan());
  }
}
