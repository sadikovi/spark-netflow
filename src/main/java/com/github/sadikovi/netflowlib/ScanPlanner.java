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
import java.util.Iterator;

import com.github.sadikovi.netflowlib.predicate.BoxedColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.FilterApi;

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

import com.github.sadikovi.netflowlib.predicate.PredicateTransform;
import com.github.sadikovi.netflowlib.predicate.Visitor;

import com.github.sadikovi.netflowlib.ScanStrategies.ScanStrategy;
import com.github.sadikovi.netflowlib.ScanStrategies.SkipScan;
import com.github.sadikovi.netflowlib.ScanStrategies.FullScan;

import com.github.sadikovi.netflowlib.statistics.ColumnStats;

/**
 * [[ScanPlanner]] interface defines strategies for parsing a record and resolving predicate tree.
 * Potentially can apply different strategies of scanning record and pushing down filters. Note
 * that original predicate tree is not modified.
 */
public final class ScanPlanner implements PredicateTransform, Visitor {
  public ScanPlanner(
      Column<?>[] selectedColumns,
      FilterPredicate predicateTree,
      ColumnStats<?>[] stats) {
    if (selectedColumns == null || selectedColumns.length == 0) {
      throw new IllegalArgumentException("Columns to select are not specified");
    }

    statistics = new HashMap<Column<?>, ColumnStats<?>>();
    filteredColumnsSet = new HashSet<Column<?>>();

    // process statistics
    if (stats != null) {
      for (ColumnStats aStats: stats) {
        statistics.put(aStats.getColumn(), aStats);
      }
    }

    // update predicate with statistics collected. If predicate tree is null, we will eventually
    // resolve it to full scan.
    FilterPredicate updatedTree = FilterApi.trivial(true);
    if (predicateTree != null) {
      updatedTree = predicateTree.update(this);
    }

    // after we update predicate tree, we need to collect information about unresolved predicates
    updatedTree.visit(this);

    // choose strategy based on columns and predicate
    // if predicate is resolved to trivial then we apply either full scan or skip file altogether
    if (updatedTree instanceof TrivialPredicate) {
      TrivialPredicate trivial = (TrivialPredicate) updatedTree;
      if (trivial.getResult()) {
        strategy = new FullScan(selectedColumns);
      } else {
        strategy = new SkipScan();
      }
    } else {
      // this branch means that we have at least one unresolved predicate, therefore plan will
      // include predicate pushdown for each file
      if (filteredColumnsSet.isEmpty()) {
        throw new IllegalStateException("Predicate tree is unresolved, but there is no filtered " +
          "columns. Make sure you define leaf predicate nodes properly");
      }

      // create array of filtered columns
      int numFilteredCols = 0;
      Column[] filteredColumns = new Column[filteredColumnsSet.size()];
      for (Column item: filteredColumnsSet) {
        filteredColumns[numFilteredCols++] = item;
      }

      // choose appropriate plan to scan based on selected and filtered columns
      if (filteredColumns.length <= 3 && filteredColumns.length <= selectedColumns.length / 3) {
        // filter-first scan
      } else {
        // full filter scan
      }
    }

    if (strategy == null) {
      throw new IllegalStateException("Strategy could not be resolved");
    }
  }

  public ScanStrategy getStrategy() {
    return strategy;
  }

  // Resolve minimum value for column looking up statistics for that column
  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> T resolveMin(Column<T> col) {
    if (statistics.containsKey(col)) {
      return (T) statistics.get(col).getMin();
    } else {
      return col.getMinValue();
    }
  }

  // Resolve maximum value for column looking up statistics for that column
  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> T resolveMax(Column<T> col) {
    if (statistics.containsKey(col)) {
      return (T) statistics.get(col).getMax();
    } else {
      return col.getMaxValue();
    }
  }

  //////////////////////////////////////////////////////////////
  // Transform API
  //////////////////////////////////////////////////////////////

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Eq<T> predicate) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col);
    T max = resolveMax(col);

    // if predicate value is less than minimum value then predicate is trivial.
    if (predicate.getValue().compareTo(min) < 0) {
      return FilterApi.trivial(false);
    }

    // if predicate value is greater than maximum value then predicate is trivial.
    if (predicate.getValue().compareTo(max) > 0) {
      return FilterApi.trivial(false);
    }

    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Gt<T> predicate) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col);
    T max = resolveMax(col);

    // if predicate value is less than minimum value then predicate is trivial. Note, if predicate
    // greater than minimum value, we still have to scan, since values can be minimal.
    if (predicate.getValue().compareTo(min) < 0) {
      return FilterApi.trivial(true);
    }

    // if predicate value is greater or equals than maximum value then predicate "Greater than" is
    // trivial, since there are no such values greater than upper bound.
    if (predicate.getValue().compareTo(max) >= 0) {
      return FilterApi.trivial(false);
    }

    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Ge<T> predicate) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col);
    T max = resolveMax(col);

    // if predicate value is less than or equals minimum value, then predicate is trivial, since it
    // would mean that we scan entire range anyway.
    if (predicate.getValue().compareTo(min) <= 0) {
      return FilterApi.trivial(true);
    }

    // if predicate value is greater than maximum value, then predicate is trivial, since there are
    // no such values in range.
    if (predicate.getValue().compareTo(max) > 0) {
      return FilterApi.trivial(false);
    }

    // if "GreaterThanOrEqual" value is a maximum value, then this simply becomes equality
    // predicate, since there are no values greater than maximum value
    if (predicate.getValue().compareTo(max) == 0) {
      return FilterApi.eq(predicate.getColumn(), max);
    }

    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Lt<T> predicate) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col);
    T max = resolveMax(col);

    // if predicate value is less than or equal minimum value, then predicate is trivial, since
    // there are no values less than minimum value.
    if (predicate.getValue().compareTo(min) <= 0) {
      return FilterApi.trivial(false);
    }

    // if predicate value is greater than maximum value, then predicate is trivial, since all values
    // fall into range (< value which is larger than maximum value).
    if (predicate.getValue().compareTo(max) > 0) {
      return FilterApi.trivial(true);
    }

    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Le<T> predicate) {
    if (predicate.getValue() == null) {
      return FilterApi.trivial(false);
    }

    Column<T> col = predicate.getColumn();
    T min = resolveMin(col);
    T max = resolveMax(col);

    // if predicate value is less than minimum value, then predicate is trivial, see above for more
    // information.
    if (predicate.getValue().compareTo(min) < 0) {
      return FilterApi.trivial(false);
    }

    // if predicate value is greater than or equal to maximum value, then predicate is trivial, and
    // full scan is performed.
    if (predicate.getValue().compareTo(max) >= 0) {
      return FilterApi.trivial(true);
    }

    // if predicate value equals minimum value, then predicate becomes equality operator.
    if (predicate.getValue().compareTo(min) == 0) {
      return FilterApi.eq(predicate.getColumn(), min);
    }

    return predicate;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(In<T> predicate) {
    // TODO: we do not do any updates for "In", since there is very low chance of being out of
    // range {min, max}.

    return predicate;
  }

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

  @Override
  public FilterPredicate transform(TrivialPredicate predicate) {
    return predicate;
  }

  //////////////////////////////////////////////////////////////
  // Visitor API
  //////////////////////////////////////////////////////////////

  @Override
  public <T extends Comparable<T>> boolean accept(Eq<T> predicate) {
    filteredColumnsSet.add(predicate.getColumn());
    return true;
  }

  @Override
  public <T extends Comparable<T>> boolean accept(Gt<T> predicate) {
    filteredColumnsSet.add(predicate.getColumn());
    return true;
  }

  @Override
  public <T extends Comparable<T>> boolean accept(Ge<T> predicate) {
    filteredColumnsSet.add(predicate.getColumn());
    return true;
  }

  @Override
  public <T extends Comparable<T>> boolean accept(Lt<T> predicate) {
    filteredColumnsSet.add(predicate.getColumn());
    return true;
  }

  @Override
  public <T extends Comparable<T>> boolean accept(Le<T> predicate) {
    filteredColumnsSet.add(predicate.getColumn());
    return true;
  }

  @Override
  public <T extends Comparable<T>> boolean accept(In<T> predicate) {
    filteredColumnsSet.add(predicate.getColumn());
    return true;
  }

  private ScanStrategy strategy = null;
  private final HashSet<Column<?>> filteredColumnsSet;
  private final HashMap<Column<?>, ColumnStats<?>> statistics;
}
