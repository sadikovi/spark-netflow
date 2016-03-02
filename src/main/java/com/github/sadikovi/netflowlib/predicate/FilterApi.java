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

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
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

/**
 * Filter API to create predicate tree. Usage
 * {{{
 * import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
 * import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;
 * import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;
 *
 * ShortColumn col1 = new ShortColumn(1, 0);
 * IntColumn col2 = new IntColumn(1, 2);
 * FilterPredicate predicate = and(eq(col1, 2), gt(col2, 100));
 * }}}
 *
 */
public final class FilterApi {
  private FilterApi() { }

  /** Test equality of column and value */
  public static Eq eq(Column column, Object value) {
    return new Eq(column, value);
  }

  /** Test if column is greater than value */
  public static Gt gt(Column column, Object value) {
    return new Gt(column, value);
  }

  /** Test if column is greater than or equal to value */
  public static Ge ge(Column column, Object value) {
    return new Ge(column, value);
  }

  /** Test if column is less than value  */
  public static Lt lt(Column column, Object value) {
    return new Lt(column, value);
  }

  /** Test if column is less than or equal to value */
  public static Le le(Column column, Object value) {
    return new Le(column, value);
  }

  /** Test if column is in the set of requested values */
  public static In in(Column column, HashSet<?> values) {
    return new In(column, values);
  }

  /** Test "and" logical operator for left and right predicates */
  public static And and(FilterPredicate left, FilterPredicate right) {
    return new And(left, right);
  }

  /** Test "or" logical operator for left and right predicates */
  public static Or or(FilterPredicate left, FilterPredicate right) {
    return new Or(left, right);
  }

  /** Test inversed operator for child predicate */
  public static Not not(FilterPredicate child) {
    return new Not(child);
  }

  /**
   * Trivial predicate, should not be used directly, but only for building predicate tree from
   * external system.
   */
  public static TrivialPredicate trivial(boolean result) {
    return new TrivialPredicate(result);
  }
}
