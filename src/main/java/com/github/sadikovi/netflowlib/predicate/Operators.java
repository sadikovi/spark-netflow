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

import java.io.Serializable;
import java.util.HashSet;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;

public final class Operators {
  private Operators() { }

  static abstract interface FilterPredicate {
    /** Visit current operator */
    public boolean visit(Visitor visitor);
  }

  /**
   * [[ColumnPredicate]] is a base class for column predicates, links to a column and filtering
   * value.
   * @param column predicate column
   * @param value filtering value
   * @param values list of values for predicates that support multiple values
   */
  static abstract class ColumnPredicate<T> implements FilterPredicate, Serializable {
    ColumnPredicate(Column<T> column, T value) {
      this.column = column;
      this.value = value;
    }

    public Column<T> getColumn() {
      return column;
    }

    public T getValue() {
      return value;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + column.toString() + ", " + value.toString() + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      ColumnPredicate that = (ColumnPredicate) obj;

      if (!column.equals(that.column)) return false;
      if (value != null ? !value.equals(that.value) : that.value != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = column.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + getClass().hashCode();
      return result;
    }

    private final Column<T> column;
    private final T value;
  }

  /** Equality filter including null-safe filtering */
  public static final class Eq<T> extends ColumnPredicate<T> {
    Eq(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean visit(Visitor visitor) {
      return visitor.accept(this);
    }
  }

  /** "Greater Than" filter */
  public static final class Gt<T> extends ColumnPredicate<T> {
    Gt(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean visit(Visitor visitor) {
      return visitor.accept(this);
    }
  }

  /** "Greater Than Or Equal" filter */
  public static final class Ge<T> extends ColumnPredicate<T> {
    Ge(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean visit(Visitor visitor) {
      return visitor.accept(this);
    }
  }

  /** "Less Than" filter */
  public static final class Lt<T> extends ColumnPredicate<T> {
    Lt(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean visit(Visitor visitor) {
      return visitor.accept(this);
    }
  }

  /** "Less Than Or Equal" filter */
  public static final class Le<T> extends ColumnPredicate<T> {
    Le(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean visit(Visitor visitor) {
      return visitor.accept(this);
    }
  }

  /**
   * [[MultiValueColumnPredicate]] allows to compare across values in the set provided, e.g. "In"
   * predicate. Currently does not check against null values. Note, that `getValue()` method in
   * case of multi value predicate returns null, use `getValues()` instead.
   * @param values set of values to compare
   */
  static abstract class MultiValueColumnPredicate<T> extends ColumnPredicate<T> {
    MultiValueColumnPredicate(Column<T> column, HashSet<T> values) {
      super(column, null);
      this.values = values;
    }

    public HashSet<T> getValues() {
      return values;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + getColumn().toString() + ", <" +
        values.getClass().getSimpleName() + ">)";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      MultiValueColumnPredicate that = (MultiValueColumnPredicate) obj;

      if (!getColumn().equals(that.getColumn())) return false;
      if (values != null ? !values.equals(that.values) : that.values != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = getColumn().hashCode();
      result = 31 * result + (values != null ? values.hashCode() : 0);
      result = 31 * result + getClass().hashCode();
      return result;
    }

    private final HashSet<T> values;
  }

  /** "In" filter, should be evaluated to true, if value contains in the set */
  public static final class In<T> extends MultiValueColumnPredicate<T> {
    In(Column<T> column, HashSet<T> values) {
      super(column, values);
    }

    @Override
    public boolean visit(Visitor visitor) {
      return visitor.accept(this);
    }
  }

  /**
   * [[BinaryLogicalPredicate]] is a composite filter which result is determined by binary
   * comparison of two child filters. Usually it is "And" and "Or" filters.
   */
  static abstract class BinaryLogicalPredicate implements FilterPredicate, Serializable {
    BinaryLogicalPredicate(FilterPredicate left, FilterPredicate right) {
      this.left = left;
      this.right = right;
    }

    public FilterPredicate getLeft() {
      return left;
    }

    public FilterPredicate getRight() {
      return right;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + left.toString() + ", " + right.toString() + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      BinaryLogicalPredicate that = (BinaryLogicalPredicate) obj;

      if (!left.equals(that.left)) return false;
      if (!right.equals(that.right)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = left.hashCode();
      result = 31 * result + right.hashCode();
      result = 31 * result + getClass().hashCode();
      return result;
    }

    private final FilterPredicate left;
    private final FilterPredicate right;
  }

  /** "And" logical operator */
  public static final class And extends BinaryLogicalPredicate {
    And(FilterPredicate left, FilterPredicate right) {
      super(left, right);
    }

    @Override
    public boolean visit(Visitor visitor) {
      return getLeft().visit(visitor) && getRight().visit(visitor);
    }
  }

  /** "And" logical operator */
  public static final class Or extends BinaryLogicalPredicate {
    Or(FilterPredicate left, FilterPredicate right) {
      super(left, right);
    }

    @Override
    public boolean visit(Visitor visitor) {
      return getLeft().visit(visitor) || getRight().visit(visitor);
    }
  }

  /**
   * [[SimpleLogicalPredicate]] is an interface for simple logical operators, such as inverse of
   * the current operator.
   */
  static abstract class SimpleLogicalPredicate implements FilterPredicate, Serializable {
    SimpleLogicalPredicate(FilterPredicate child) {
      this.child = child;
    }

    public FilterPredicate getChild() {
      return child;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + child + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      SimpleLogicalPredicate that = (SimpleLogicalPredicate) obj;

      if (!child.equals(that.child)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = child.hashCode();
      result = 31 * result + getClass().hashCode();
      return result;
    }

    private final FilterPredicate child;
  }

  /** "Not" inversion operator */
  public static final class Not extends SimpleLogicalPredicate {
    Not(FilterPredicate child) {
      super(child);
    }

    @Override
    public boolean visit(Visitor visitor) {
      return !getChild().visit(visitor);
    }
  }

  /**
   * [[TrivialPredicate]] is an interface to provide direct resolution to filters that are by
   * definition either true or false, so we do not need to wait for columns to be scanned, we can
   * answer it immediately. For example, if object is null or is not null.
   * @param result result of trivial filter
   */
  public static final class TrivialPredicate implements FilterPredicate {
    TrivialPredicate(boolean result) {
      this.result = result;
    }

    public boolean getResult() {
      return result;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + result + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      TrivialPredicate that = (TrivialPredicate) obj;

      return result == that.result;
    }

    @Override
    public boolean visit(Visitor visitor) {
      return result;
    }

    private final boolean result;
  }
}
