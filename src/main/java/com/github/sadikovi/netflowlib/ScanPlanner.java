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
import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.FilterApi;
import com.github.sadikovi.netflowlib.predicate.Inspectors.Inspector;
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
  }

  private ScanStrategy getStrategy() {
    return strategy;
  }
  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Eq<T> predicate,
      HashMap<String, Statistics> stats) {
    return null;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Gt<T> predicate,
      HashMap<String, Statistics> stats) {
    return null;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Ge<T> predicate,
      HashMap<String, Statistics> stats) {
    return null;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Lt<T> predicate,
      HashMap<String, Statistics> stats) {
    return null;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(Le<T> predicate,
      HashMap<String, Statistics> stats) {
    return null;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate transform(In<T> predicate,
      HashMap<String, Statistics> stats) {
    return null;
  }

  @Override
  public FilterPredicate transform(And predicate) {
    return null;
  }

  @Override
  public FilterPredicate transform(Or predicate) {
    return null;
  }

  @Override
  public FilterPredicate transform(Not predicate) {
    return null;
  }

  private ScanStrategy strategy = null;
  private HashMap<String, ArrayList<Inspector>> inspector = null;
}
