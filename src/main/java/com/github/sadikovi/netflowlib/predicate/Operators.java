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
import java.util.HashMap;
import java.util.HashSet;

import com.github.sadikovi.netflowlib.codegen.CodeGenContext;
import com.github.sadikovi.netflowlib.codegen.CodeGenExpression;
import com.github.sadikovi.netflowlib.codegen.CodeGenNode;
import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Inspectors.Inspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.AndInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.NotInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.OrInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
import com.github.sadikovi.netflowlib.statistics.Statistics;

public final class Operators {
  private Operators() { }

  /**
   * Abstract [[FilterPredicate]] interface defines a common predicate instance that can be
   * transformed into another predicate or converted into [[ValueInspector]]. This class represents
   * original user-defined predicate tree.
   */
  public static abstract interface FilterPredicate extends CodeGenExpression {

    public FilterPredicate update(PredicateTransform transformer,
      HashMap<String, Statistics> statistics);

    public Inspector convert();
  }

  /**
   * [[ColumnPredicate]] is a base class for column predicates, links to a column and filtering
   * value.
   * @param column predicate column
   * @param value filtering value
   * @param values list of values for predicates that support multiple values
   */
  static abstract class ColumnPredicate implements FilterPredicate, Serializable {
    ColumnPredicate(Column column, Object value) {
      this.column = column;
      // Since value can be any object potentially, we store it as Serializable to make sure that
      // any distributed systems can resolve predicate once, and ship it to any node.
      this.value = (Serializable) value;
      this.inspector = getValueInspector(column.getColumnType());
    }

    protected abstract ValueInspector getValueInspector(Class<?> klass);

    public Column getColumn() {
      return column;
    }

    public Object getValue() {
      return value;
    }

    public ValueInspector inspector() {
      return inspector;
    }

    public final Inspector convert() {
      return inspector();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + column + ", " + value + ")";
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

    @Override
    public String generate(CodeGenContext ctx) {
      Class<?> type = getColumn().getColumnType();
      String nodeName = getColumn().nodeName(ctx);
      String nodeType = getColumn().nodeType(ctx);
      if (type.equals(Byte.class) || type.equals(Short.class) || type.equals(Integer.class) ||
          type.equals(Long.class)) {
        return generateNumeric(ctx, nodeName, nodeType);
      } else {
        throw new UnsupportedOperationException("Unsupported column " + getColumn() +
          " for code generation");
      }
    }

    /** Generate code for numeric boolean expression */
    protected abstract String generateNumeric(CodeGenContext ctx, String nodeName, String nodeType);

    private final Column column;
    private final Serializable value;
    private final ValueInspector inspector;
  }

  //////////////////////////////////////////////////////////////
  // Concrete implementation of column predicates (Eq, Gt, Le)
  //////////////////////////////////////////////////////////////

  /** Equality filter including null-safe filtering */
  public static final class Eq extends ColumnPredicate {
    Eq(Column column, Object value) {
      super(column, value);
    }

    @Override
    public FilterPredicate update(PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      return transformer.transform(this, statistics);
    }

    @Override
    protected ValueInspector getValueInspector(Class<?> klass) {
      if (klass.equals(Byte.class)) {
        return new ValueInspector() {
          @Override
          public void update(byte value) {
            setResult(value == Byte.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Short.class)) {
        return new ValueInspector() {
          @Override
          public void update(short value) {
            setResult(value == Short.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Integer.class)) {
        return new ValueInspector() {
          @Override
          public void update(int value) {
            setResult(value == Integer.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Long.class)) {
        return new ValueInspector() {
          @Override
          public void update(long value) {
            setResult(value == Long.class.cast(getValue()));
          }
        };
      } else {
        throw new UnsupportedOperationException("Unsupported type for predicate " + toString());
      }
    }

    @Override
    protected String generateNumeric(CodeGenContext ctx, String nodeName, String nodeType) {
      return nodeName + " == " + ctx.normalizeJavaValue(getValue());
    }
  }

  /** "Greater Than" filter */
  public static final class Gt extends ColumnPredicate {
    Gt(Column column, Object value) {
      super(column, value);
    }

    @Override
    public FilterPredicate update(
        PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      return transformer.transform(this, statistics);
    }

    @Override
    protected ValueInspector getValueInspector(Class<?> klass) {
      if (klass.equals(Byte.class)) {
        return new ValueInspector() {
          @Override
          public void update(byte value) {
            setResult(value > Byte.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Short.class)) {
        return new ValueInspector() {
          @Override
          public void update(short value) {
            setResult(value > Short.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Integer.class)) {
        return new ValueInspector() {
          @Override
          public void update(int value) {
            setResult(value > Integer.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Long.class)) {
        return new ValueInspector() {
          @Override
          public void update(long value) {
            setResult(value > Long.class.cast(getValue()));
          }
        };
      } else {
        throw new UnsupportedOperationException("Unsupported type for predicate " + toString());
      }
    }

    @Override
    protected String generateNumeric(CodeGenContext ctx, String nodeName, String nodeType) {
      return nodeName + " > " + ctx.normalizeJavaValue(getValue());
    }
  }

  /** "Greater Than Or Equal" filter */
  public static final class Ge extends ColumnPredicate {
    Ge(Column column, Object value) {
      super(column, value);
    }

    @Override
    public FilterPredicate update(
        PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      return transformer.transform(this, statistics);
    }

    @Override
    protected ValueInspector getValueInspector(Class<?> klass) {
      if (klass.equals(Byte.class)) {
        return new ValueInspector() {
          @Override
          public void update(byte value) {
            setResult(value >= Byte.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Short.class)) {
        return new ValueInspector() {
          @Override
          public void update(short value) {
            setResult(value >= Short.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Integer.class)) {
        return new ValueInspector() {
          @Override
          public void update(int value) {
            setResult(value >= Integer.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Long.class)) {
        return new ValueInspector() {
          @Override
          public void update(long value) {
            setResult(value >= Long.class.cast(getValue()));
          }
        };
      } else {
        throw new UnsupportedOperationException("Unsupported type for predicate " + toString());
      }
    }

    @Override
    protected String generateNumeric(CodeGenContext ctx, String nodeName, String nodeType) {
      return nodeName + " >= " + ctx.normalizeJavaValue(getValue());
    }
  }

  /** "Less Than" filter */
  public static final class Lt extends ColumnPredicate {
    Lt(Column column, Object value) {
      super(column, value);
    }

    @Override
    public FilterPredicate update(
        PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      return transformer.transform(this, statistics);
    }

    @Override
    protected ValueInspector getValueInspector(Class<?> klass) {
      if (klass.equals(Byte.class)) {
        return new ValueInspector() {
          @Override
          public void update(byte value) {
            setResult(value < Byte.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Short.class)) {
        return new ValueInspector() {
          @Override
          public void update(short value) {
            setResult(value < Short.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Integer.class)) {
        return new ValueInspector() {
          @Override
          public void update(int value) {
            setResult(value < Integer.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Long.class)) {
        return new ValueInspector() {
          @Override
          public void update(long value) {
            setResult(value < Long.class.cast(getValue()));
          }
        };
      } else {
        throw new UnsupportedOperationException("Unsupported type for predicate " + toString());
      }
    }

    @Override
    protected String generateNumeric(CodeGenContext ctx, String nodeName, String nodeType) {
      return nodeName + " < " + ctx.normalizeJavaValue(getValue());
    }
  }

  /** "Less Than Or Equal" filter */
  public static final class Le extends ColumnPredicate {
    Le(Column column, Object value) {
      super(column, value);
    }

    @Override
    public FilterPredicate update(
        PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      return transformer.transform(this, statistics);
    }

    @Override
    protected ValueInspector getValueInspector(Class<?> klass) {
      if (klass.equals(Byte.class)) {
        return new ValueInspector() {
          @Override
          public void update(byte value) {
            setResult(value <= Byte.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Short.class)) {
        return new ValueInspector() {
          @Override
          public void update(short value) {
            setResult(value <= Short.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Integer.class)) {
        return new ValueInspector() {
          @Override
          public void update(int value) {
            setResult(value <= Integer.class.cast(getValue()));
          }
        };
      } else if (klass.equals(Long.class)) {
        return new ValueInspector() {
          @Override
          public void update(long value) {
            setResult(value <= Long.class.cast(getValue()));
          }
        };
      } else {
        throw new UnsupportedOperationException("Unsupported type for predicate " + toString());
      }
    }

    @Override
    protected String generateNumeric(CodeGenContext ctx, String nodeName, String nodeType) {
      return nodeName + " <= " + ctx.normalizeJavaValue(getValue());
    }
  }

  /**
   * [[MultiValueColumnPredicate]] allows to compare across values in the set provided, e.g. "In"
   * predicate. Currently does not check against null values. Note, that `getValue()` method in
   * case of multi value predicate returns null, use `getValues()` instead.
   * @param values set of values to compare
   */
  static abstract class MultiValueColumnPredicate extends ColumnPredicate {
    MultiValueColumnPredicate(Column column, HashSet<?> values) {
      super(column, null);
      for (Object obj: values) {
        this.values.add(obj);
      }
    }

    public HashSet<Object> getValues() {
      return values;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + getColumn() + ", <" +
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

    protected HashSet<Object> values = new HashSet<Object>();
  }

  /** "In" filter, should be evaluated to true, if value contains in the set */
  public static final class In extends MultiValueColumnPredicate {
    In(Column column, HashSet<?> values) {
      super(column, values);
    }

    @Override
    public FilterPredicate update(
        PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      return transformer.transform(this, statistics);
    }

    @Override
    protected ValueInspector getValueInspector(Class<?> klass) {
      if (klass.equals(Byte.class)) {
        return new ValueInspector() {
          @Override
          public void update(byte value) {
            setResult(getValues().contains(value));
          }
        };
      } else if (klass.equals(Short.class)) {
        return new ValueInspector() {
          @Override
          public void update(short value) {
            setResult(getValues().contains(value));
          }
        };
      } else if (klass.equals(Integer.class)) {
        return new ValueInspector() {
          @Override
          public void update(int value) {
            setResult(getValues().contains(value));
          }
        };
      } else if (klass.equals(Long.class)) {
        return new ValueInspector() {
          @Override
          public void update(long value) {
            setResult(getValues().contains(value));
          }
        };
      } else {
        throw new UnsupportedOperationException("Unsupported type for predicate " + toString());
      }
    }

    @Override
    protected String generateNumeric(CodeGenContext ctx, String nodeName, String nodeType) {
      StringBuilder buffer = new StringBuilder();
      if (values.isEmpty()) {
        return "false";
      }

      boolean isFirstValue = true;
      for (Object entry: values) {
        if (!isFirstValue) {
          buffer.append(" || ");
        } else {
          isFirstValue = false;
        }
        buffer.append(nodeName + " == " + ctx.normalizeJavaValue(entry));
      }

      return buffer.toString();
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

    public String generate(CodeGenContext ctx) {
      String code1 = getLeft().generate(ctx);
      String code2 = getRight().generate(ctx);
      return generateBinaryExpression(ctx, code1, code2);
    }

    protected abstract String generateBinaryExpression(
      CodeGenContext ctx, String code1, String code2);

    private final FilterPredicate left;
    private final FilterPredicate right;
  }

  //////////////////////////////////////////////////////////////
  // Concrete implementation of binary logical predicate (And, Or)
  //////////////////////////////////////////////////////////////

  /** "And" logical operator */
  public static final class And extends BinaryLogicalPredicate {
    And(FilterPredicate left, FilterPredicate right) {
      super(left, right);
    }

    @Override
    public FilterPredicate update(
        PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      And copy = new And(getLeft().update(transformer, statistics),
        getRight().update(transformer, statistics));
      return transformer.transform(copy);
    }

    @Override
    public Inspector convert() {
      return new AndInspector(getLeft().convert(), getRight().convert());
    }

    @Override
    protected String generateBinaryExpression(CodeGenContext ctx, String code1, String code2) {
      return "(" + code1 + ") && (" + code2 + ")";
    }
  }

  /** "And" logical operator */
  public static final class Or extends BinaryLogicalPredicate {
    Or(FilterPredicate left, FilterPredicate right) {
      super(left, right);
    }

    @Override
    public FilterPredicate update(
        PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      Or copy = new Or(getLeft().update(transformer, statistics),
        getRight().update(transformer, statistics));
      return transformer.transform(copy);
    }

    @Override
    public Inspector convert() {
      return new OrInspector(getLeft().convert(), getRight().convert());
    }

    @Override
    protected String generateBinaryExpression(CodeGenContext ctx, String code1, String code2) {
      return "(" + code1 + ") || (" + code2 + ")";
    }
  }

  /**
   * [[UnaryLogicalPredicate]] is an interface for simple logical operators, such as inverse of
   * the current operator.
   */
  static abstract class UnaryLogicalPredicate implements FilterPredicate, Serializable {
    UnaryLogicalPredicate(FilterPredicate child) {
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

      UnaryLogicalPredicate that = (UnaryLogicalPredicate) obj;

      if (!child.equals(that.child)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = child.hashCode();
      result = 31 * result + getClass().hashCode();
      return result;
    }

    @Override
    public abstract String generate(CodeGenContext ctx);

    private final FilterPredicate child;
  }

  //////////////////////////////////////////////////////////////
  // Concrete implementation of unary logical predicate (Not)
  //////////////////////////////////////////////////////////////

  /** "Not" inversion operator */
  public static final class Not extends UnaryLogicalPredicate {
    Not(FilterPredicate child) {
      super(child);
    }

    @Override
    public FilterPredicate update(
        PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      Not copy = new Not(getChild().update(transformer, statistics));
      return transformer.transform(copy);
    }

    @Override
    public Inspector convert() {
      return new NotInspector(getChild().convert());
    }

    @Override
    public String generate(CodeGenContext ctx) {
      return "!(" + getChild().generate(ctx) + ")";
    }
  }

  /**
   * [[TrivialPredicate]] is an interface to provide direct resolution to filters that are by
   * definition either true or false, so we do not need to wait for columns to be scanned, we can
   * answer it immediately. For example, if object is null or is not null.
   * @param result result of trivial filter
   */
  public static final class TrivialPredicate implements FilterPredicate, Serializable {
    TrivialPredicate() { }

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
    public FilterPredicate update(
        PredicateTransform transformer,
        HashMap<String, Statistics> statistics) {
      return new TrivialPredicate(result);
    }

    @Override
    public Inspector convert() {
      throw new UnsupportedOperationException("Predicate " + toString() + " does not support " +
        "Inspector interface. You should transform FilterPredicate tree to ensure that there are " +
        "no TrivialPredicate instances");
    }

    @Override
    public String generate(CodeGenContext ctx) {
      return "" + getResult();
    }

    private boolean result;
  }
}
