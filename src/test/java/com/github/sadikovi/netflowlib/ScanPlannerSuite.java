package com.github.sadikovi.netflowlib;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.github.sadikovi.netflowlib.ScanPlanner;
import com.github.sadikovi.netflowlib.ScanStrategies.SkipScan;
import com.github.sadikovi.netflowlib.ScanStrategies.FullScan;
import com.github.sadikovi.netflowlib.ScanStrategies.PredicateScan;
import com.github.sadikovi.netflowlib.ScanStrategies.PredicateFirstScan;

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

import com.github.sadikovi.netflowlib.statistics.ColumnStats;

public class ScanPlannerSuite {
  @Test(expected = IllegalArgumentException.class)
  public void testColumnsNullFailure() {
    Column[] cols = null;
    FilterPredicate tree = null;
    ColumnStats[] stats = null;

    new ScanPlanner(cols, tree, stats);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColumnsEmptyFailure() {
    Column[] cols = new Column[0];
    FilterPredicate tree = null;
    ColumnStats[] stats = null;

    new ScanPlanner(cols, tree, stats);
  }

  @Test
  public void testSkipScan1() {
    Column[] cols = new Column[] {new IntColumn((byte) 1, 0), new IntColumn((byte) 2, 4)};
    FilterPredicate tree = trivial(false);
    ColumnStats[] stats = null;

    ScanPlanner sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), SkipScan.class);
    assertTrue(sp.getStrategy().skip());
  }

  @Test
  public void testSkipScan2() {
    Column[] cols = new Column[] {new IntColumn((byte) 1, 0), new IntColumn((byte) 2, 4)};
    FilterPredicate tree = and(trivial(false), eq(new IntColumn((byte) 1, 0), 10));
    ColumnStats[] stats = null;

    ScanPlanner sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), SkipScan.class);
    assertTrue(sp.getStrategy().skip());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSkipScan3() {
    Column[] cols = new Column[] {new IntColumn((byte) 1, 0), new IntColumn((byte) 2, 4)};

    IntColumn col1 = new IntColumn((byte) 1, 0);
    IntColumn col2 = new IntColumn((byte) 2, 0);
    IntColumn col3 = new IntColumn((byte) 3, 0);
    FilterPredicate tree = or(and(lt(col1, 0), eq(col2, 10)), and(ge(col3, 10), le(col3, 19)));

    // statistics on column 3, even when it is a different object
    ColumnStats<Integer> colStats3 = new ColumnStats(new IntColumn((byte) 3, 0));
    colStats3.setMin(21);
    colStats3.setMax(25);
    ColumnStats[] stats = new ColumnStats[] {colStats3};

    ScanPlanner sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), SkipScan.class);
    assertTrue(sp.getStrategy().skip());
  }

  @Test
  public void testFullScan1() {
    Column[] cols = new Column[] {new IntColumn((byte) 1, 0), new IntColumn((byte) 2, 4)};
    FilterPredicate tree = null;
    ColumnStats[] stats = null;

    ScanPlanner sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), FullScan.class);
    assertFalse(sp.getStrategy().skip());

    // it is an equivalent of having trivial predicate "true"
    tree = trivial(true);
    sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), FullScan.class);
    assertFalse(sp.getStrategy().skip());
  }

  @Test
  public void testFullScan2() {
    Column[] cols = new Column[] {new IntColumn((byte) 1, 0), new IntColumn((byte) 2, 4)};

    IntColumn col1 = new IntColumn((byte) 1, 0);
    FilterPredicate tree = and(trivial(true), ge(col1, 0));

    ColumnStats[] stats = null;

    ScanPlanner sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), FullScan.class);
    assertFalse(sp.getStrategy().skip());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFullScan3() {
    Column[] cols = new Column[] {new IntColumn((byte) 1, 0), new IntColumn((byte) 2, 4)};

    IntColumn col1 = new IntColumn((byte) 1, 0);
    IntColumn col2 = new IntColumn((byte) 2, 0);
    IntColumn col3 = new IntColumn((byte) 3, 0);
    FilterPredicate tree = and(
      and(ge(col1, 10), le(col1, 20)),
      or(
        eq(col2, 100),
        or(le(col3, -1), ge(col3, 0))
      )
    );

    ColumnStats colStats1 = new ColumnStats(col1);
    colStats1.setMin(12);
    colStats1.setMax(18);
    ColumnStats[] stats = new ColumnStats[] {colStats1};

    ScanPlanner sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), FullScan.class);
    assertFalse(sp.getStrategy().skip());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFullScan4() {
    Column[] cols = new Column[] {new IntColumn((byte) 1, 0), new IntColumn((byte) 2, 4)};

    IntColumn col1 = new IntColumn((byte) 1, 0);
    IntColumn col2 = new IntColumn((byte) 2, 0);
    IntColumn col3 = new IntColumn((byte) 3, 0);
    FilterPredicate tree = or(
      and(ge(col1, 10), le(col1, 20)),
      or(
        eq(col2, 100),
        or(le(col3, -1), ge(col3, 0))
      )
    );

    ColumnStats[] stats = new ColumnStats[] {};

    ScanPlanner sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), FullScan.class);
    assertFalse(sp.getStrategy().skip());
  }

  @Test
  public void testPredicateScan1() {
    Column[] cols = new Column[] {new IntColumn((byte) 1, 0), new IntColumn((byte) 2, 4)};

    IntColumn col1 = new IntColumn((byte) 1, 0);
    IntColumn col2 = new IntColumn((byte) 2, 0);
    IntColumn col3 = new IntColumn((byte) 3, 0);
    FilterPredicate tree = and(
      and(ge(col1, 10), le(col1, 20)),
      or(
        eq(col2, 100),
        or(le(col3, -1), ge(col3, 0))
      )
    );

    ColumnStats[] stats = new ColumnStats[] {};

    ScanPlanner sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), PredicateScan.class);
    assertFalse(sp.getStrategy().skip());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPredicateFilterScan1() {
    Column[] cols = new Column[] {new IntColumn((byte) 1, 0), new IntColumn((byte) 2, 4),
      new IntColumn((byte) 3, 4), new IntColumn((byte) 4, 4), new IntColumn((byte) 5, 4),
      new IntColumn((byte) 6, 4), new IntColumn((byte) 7, 4)};

    IntColumn col1 = new IntColumn((byte) 1, 0);
    IntColumn col2 = new IntColumn((byte) 2, 0);
    FilterPredicate tree = and(
      and(ge(col1, 10), le(col1, 20)),
      eq(col2, 100)
    );

    ColumnStats colStats1 = new ColumnStats(col1);
    colStats1.setMin(12);
    colStats1.setMax(18);
    ColumnStats[] stats = new ColumnStats[] {colStats1};

    ScanPlanner sp = new ScanPlanner(cols, tree, stats);
    assertSame(sp.getStrategy().getClass(), PredicateFirstScan.class);
    assertFalse(sp.getStrategy().skip());
  }
}
