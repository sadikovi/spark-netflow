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

import java.util.ArrayList;
import java.util.HashMap;

import com.github.sadikovi.netflowlib.Strategies.ScanStrategy;
import com.github.sadikovi.netflowlib.Strategies.FullScan;
import com.github.sadikovi.netflowlib.Strategies.FilterScan;
import com.github.sadikovi.netflowlib.Strategies.SkipScan;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.FilterApi;
import com.github.sadikovi.netflowlib.predicate.Inspectors.Inspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
import com.github.sadikovi.netflowlib.predicate.PredicateTransform;

import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;
import com.github.sadikovi.netflowlib.predicate.Operators.Eq;
import com.github.sadikovi.netflowlib.predicate.Operators.Gt;
import com.github.sadikovi.netflowlib.predicate.Operators.Ge;
import com.github.sadikovi.netflowlib.predicate.Operators.Lt;
import com.github.sadikovi.netflowlib.predicate.Operators.Le;
import com.github.sadikovi.netflowlib.predicate.Operators.In;
import com.github.sadikovi.netflowlib.predicate.Operators.And;
import com.github.sadikovi.netflowlib.predicate.Operators.Or;
import com.github.sadikovi.netflowlib.predicate.Operators.Not;
import com.github.sadikovi.netflowlib.predicate.Operators.TrivialPredicate;

import com.github.sadikovi.netflowlib.statistics.Statistics;
import com.github.sadikovi.netflowlib.statistics.StatisticsTypes.GenericStatistics;

/**
 * [[ScanPlanner]] interface defines strategies for parsing a record and resolving predicate tree.
 */
public final class ScanPlanner implements PredicateTransform {
  /** Build appropriate strategy based on pruned columns, predicate tree and statistics */
  public static <T extends Comparable<T>> ScanStrategy buildStrategy(
      Column<T>[] columns,
      FilterPredicate predicate,
      HashMap<Column<T>, GenericStatistics<T>> stats) {
    ScanPlanner planner = new ScanPlanner(columns, predicate, stats);
    return planner.getStrategy();
  }

  public static <T extends Comparable<T>> ScanStrategy buildStrategy(Column<T>[] columns) {
    return buildStrategy(columns, null, null);
  }

  public static <T extends Comparable<T>> ScanStrategy buildStrategy(
      Column<T>[] columns,
      FilterPredicate predicate) {
    return buildStrategy(columns, predicate, null);
  }

  private <T extends Comparable<T>> ScanPlanner(
      Column<T>[] columns,
      FilterPredicate predicate,
      HashMap<Column<T>, GenericStatistics<T>> stats) {
    // Building strategy involves several steps, such as variables check, making sure that we can
    // prune columns, predicate is defined, and statistics can be resolved; next is folding
    // predicate tree and applying statistics to modified predicate; next step is converting
    // predicate tree into inspector tree + mapping of columns to [[ValueInspector]], so it can be
    // used in [[RecordMaterializer]].
    if (columns == null || columns.length == 0) {
      throw new IllegalArgumentException("Expected columns to select, got " + columns +
        ". Make sure that you provide correct Column<T> instances when requesting a scan");
    }

    // Resolve statistics, if available. Note that we manage statistics as map of column name and
    // [[Statistics]] instance, so internally we are dealing with abstract interface without support
    // of parameterized type, so we just rely on correct statistics type provided for a column in
    // constructor.
    HashMap<String, Statistics> internalStats = new HashMap<String, Statistics>();
    if (stats != null) {
      for (Column<T> col: stats.keySet()) {
        internalStats.put(col.getColumnName(), stats.get(col));
      }
    }

    // Resolve predicate, if predicate is not defined we automatically assign positive trivial
    // predicate which will result in full scan of the file, otherwise fold it with statistics. Make
    // sure that there are no `TrivialPredicate` instances in the tree.
    FilterPredicate internalFilter = predicate;
    if (internalFilter == null) {
      internalFilter = FilterApi.trivial(true);
    } else {
      internalFilter = predicate.update(this, internalStats);
    }

    // Flags for predicate, whether it is known or not, whether result is known or not
    boolean known = (internalFilter instanceof TrivialPredicate);
    boolean result = (known) ? ((TrivialPredicate) internalFilter).getResult() : false;

    // Choose strategy based on result
    if (known && result) {
      // Do full scan
      strategy = new FullScan(columns);
    } else if (known && !result) {
      // Skip scan
      strategy = new SkipScan();
    } else {
      // Predicate scan, convert `internalFilter` into inspector tree, extract columns that are
      // filtered
      Inspector inspectorTree = internalFilter.convert();
      strategy = new FilterScan(columns, inspectorTree, inspectors);
    }

    if (strategy == null) {
      throw new IllegalStateException("Cannot find suitable strategy for scan. Make sure you " +
        "provide correct arguments in constructor");
    }
  }

  private ScanStrategy getStrategy() {
    return strategy;
  }

  @SuppressWarnings("unchecked")
  protected <T extends Comparable<T>> T resolveMin(Column<T> column, Statistics stats) {
    T min = column.getMinValue();
    if (stats != null) {
      min = (T) stats.getMin();
    }

    return min;
  }

  @SuppressWarnings("unchecked")
  protected <T extends Comparable<T>> T resolveMax(Column<T> column, Statistics stats) {
    T max = column.getMaxValue();
    if (stats != null) {
      max = (T) stats.getMax();
    }

    return max;
  }

  protected <T extends Comparable<T>> void addInspector(
      Column<T> column,
      ValueInspector inspector) {
    if (!inspectors.containsKey(column)) {
      inspectors.put(column, new ArrayList<ValueInspector>());
    }

    inspectors.get(column).add(inspector);
  }

  //////////////////////////////////////////////////////////////
  // PredicateTransform API (ColumnPredicate)
  //////////////////////////////////////////////////////////////

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Eq<T> predicate,
      HashMap<String, Statistics> stats) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col, stats.get(col.getColumnName()));
    T max = resolveMax(col, stats.get(col.getColumnName()));

    if (predicate.getValue().compareTo(min) < 0) {
      return FilterApi.trivial(false);
    }

    if (predicate.getValue().compareTo(max) > 0) {
      return FilterApi.trivial(false);
    }

    addInspector(col, predicate.inspector());

    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Gt<T> predicate,
      HashMap<String, Statistics> stats) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col, stats.get(col.getColumnName()));
    T max = resolveMax(col, stats.get(col.getColumnName()));

    // If predicate value is less than minimum value then predicate is trivial. Note, if predicate
    // greater than minimum value, we still have to scan, since values can be minimal.
    if (predicate.getValue().compareTo(min) < 0) {
      return FilterApi.trivial(true);
    }

    // If predicate value is greater or equals than maximum value then predicate "Greater than" is
    // trivial, since there are no such values greater than upper bound.
    if (predicate.getValue().compareTo(max) >= 0) {
      return FilterApi.trivial(false);
    }

    addInspector(col, predicate.inspector());

    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Ge<T> predicate,
      HashMap<String, Statistics> stats) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col, stats.get(col.getColumnName()));
    T max = resolveMax(col, stats.get(col.getColumnName()));

    // If predicate value is less than or equals minimum value, then predicate is trivial, since it
    // would mean that we scan entire range anyway.
    if (predicate.getValue().compareTo(min) <= 0) {
      return FilterApi.trivial(true);
    }

    // If predicate value is greater than maximum value, then predicate is trivial, since there are
    // no such values in range.
    if (predicate.getValue().compareTo(max) > 0) {
      return FilterApi.trivial(false);
    }

    // If "GreaterThanOrEqual" value is a maximum value, then this simply becomes equality
    // predicate, since there are no values greater than maximum value.
    // Note since we return new equality predicate we have to add it to the list of inspectors.
    if (predicate.getValue().compareTo(max) == 0) {
      Eq<T> equalityPredicate = FilterApi.eq(predicate.getColumn(), max);

      addInspector(col, equalityPredicate.inspector());
      return equalityPredicate;
    }

    addInspector(col, predicate.inspector());
    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Lt<T> predicate,
      HashMap<String, Statistics> stats) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col, stats.get(col.getColumnName()));
    T max = resolveMax(col, stats.get(col.getColumnName()));

    // If predicate value is less than or equal minimum value, then predicate is trivial, since
    // there are no values less than minimum value.
    if (predicate.getValue().compareTo(min) <= 0) {
      return FilterApi.trivial(false);
    }

    // If predicate value is greater than maximum value, then predicate is trivial, since all
    // values fall into range (< value which is larger than maximum value).
    if (predicate.getValue().compareTo(max) > 0) {
      return FilterApi.trivial(true);
    }

    addInspector(col, predicate.inspector());

    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Le<T> predicate,
      HashMap<String, Statistics> stats) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col, stats.get(col.getColumnName()));
    T max = resolveMax(col, stats.get(col.getColumnName()));

    // If predicate value is less than minimum value, then predicate is trivial, see above for more
    // information.
    if (predicate.getValue().compareTo(min) < 0) {
      return FilterApi.trivial(false);
    }

    // If predicate value is greater than or equal to maximum value, then predicate is trivial, and
    // full scan is performed.
    if (predicate.getValue().compareTo(max) >= 0) {
      return FilterApi.trivial(true);
    }

    // If predicate value equals minimum value, then predicate becomes equality operator.
    // Note since we return new equality predicate we have to add it to the list of inspectors.
    if (predicate.getValue().compareTo(min) == 0) {
      Eq<T> equalityPredicate = FilterApi.eq(predicate.getColumn(), min);

      addInspector(col, equalityPredicate.inspector());
      return equalityPredicate;
    }

    addInspector(col, predicate.inspector());
    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(In<T> predicate,
      HashMap<String, Statistics> stats) {
    // TODO: we do not do any updates for "In", since there is very low chance of being out of
    // range {min, max}.
    addInspector(predicate.getColumn(), predicate.inspector());

    return predicate;
  }

  //////////////////////////////////////////////////////////////
  // PredicateTransform API (Unary/BinaryLogicalPredicate)
  //////////////////////////////////////////////////////////////

  @Override
  public FilterPredicate transform(And predicate) {
    // both children are trivial
    if (predicate.getLeft() instanceof TrivialPredicate &&
        predicate.getRight() instanceof TrivialPredicate) {
      TrivialPredicate left = (TrivialPredicate)(predicate.getLeft());
      TrivialPredicate right = (TrivialPredicate)(predicate.getRight());

      return FilterApi.trivial(left.getResult() && right.getResult());
    }

    // either left or right is trivial, but not both
    if (predicate.getLeft() instanceof TrivialPredicate) {
      TrivialPredicate left = (TrivialPredicate)(predicate.getLeft());
      if (left.getResult()) {
        return predicate.getRight();
      } else {
        return left;
      }
    }

    if (predicate.getRight() instanceof TrivialPredicate) {
      TrivialPredicate right = (TrivialPredicate)(predicate.getRight());
      if (right.getResult()) {
        return predicate.getLeft();
      } else {
        return right;
      }
    }

    // otherwise return unmodified predicate
    return predicate;
  }

  @Override
  public FilterPredicate transform(Or predicate) {
    // both children are trivial
    if (predicate.getLeft() instanceof TrivialPredicate &&
        predicate.getRight() instanceof TrivialPredicate) {
      TrivialPredicate left = (TrivialPredicate)(predicate.getLeft());
      TrivialPredicate right = (TrivialPredicate)(predicate.getRight());

      return FilterApi.trivial(left.getResult() || right.getResult());
    }

    // either left or right is trivial, but not both
    if (predicate.getLeft() instanceof TrivialPredicate) {
      TrivialPredicate left = (TrivialPredicate)(predicate.getLeft());
      if (left.getResult()) {
        return left;
      } else {
        return predicate.getRight();
      }
    }

    if (predicate.getRight() instanceof TrivialPredicate) {
      TrivialPredicate right = (TrivialPredicate)(predicate.getRight());
      if (right.getResult()) {
        return right;
      } else {
        return predicate.getLeft();
      }
    }

    // otherwise return itself
    return predicate;
  }

  @Override
  public FilterPredicate transform(Not predicate) {
    // transform trivial predicate directly to minimize recursion depth.
    if (predicate.getChild() instanceof TrivialPredicate) {
      TrivialPredicate child = (TrivialPredicate)(predicate.getChild());
      return FilterApi.trivial(!child.getResult());
    }

    return predicate;
  }

  private ScanStrategy strategy = null;
  private final HashMap<Column<?>, ArrayList<ValueInspector>> inspectors =
    new HashMap<Column<?>, ArrayList<ValueInspector>>();
}
