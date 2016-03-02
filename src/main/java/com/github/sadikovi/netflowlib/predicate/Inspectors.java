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

/**
 * [[Inspector]] interface is designed to provide methods of resolving predicate values and give
 * an answer on when to skip record. [[ValueInspector]] should be implemented for all basic
 * filters (leaf nodes). Note that [[TrivialPredicate]] does not support value inspector, so
 * `update()` should be run beforehand to optimizie predicate and remove them. [[BinaryLogical]]
 * inspectors are supported by corresponding predicates `And` and `Or`, and [[UnaryLogical]]
 * inspector is backed by `Not` filter.
 * `Inspector` can be used to resolve predicate incrementally, though it depends on design of a
 * caller.
 */
public final class Inspectors {
  private Inspectors() { }

  public static interface Inspector {

    boolean accept(Visitor visitor);
  }

  /** Inspector for leaf nodes, e.g. Eq, Ge, Gt, Le, Lt, In */
  public static class ValueInspector implements Inspector {
    public ValueInspector() { }

    public void update(boolean value) { throw new UnsupportedOperationException(); }
    public void update(byte value) { throw new UnsupportedOperationException(); }
    public void update(short value) { throw new UnsupportedOperationException(); }
    public void update(int value) { throw new UnsupportedOperationException(); }
    public void update(long value) { throw new UnsupportedOperationException(); }

    public final void reset() {
      known = false;
      result = false;
    }

    public final void setResult(boolean expression) {
      if (isKnown()) {
        throw new IllegalStateException("Inspector is already known, cannot set result");
      }

      result = expression;
      known = true;
    }

    public final boolean getResult() {
      if (!isKnown()) {
        throw new IllegalStateException("Inspector is not known, cannot return result");
      }

      return result;
    }

    public final boolean isKnown() {
      return known;
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }

    private boolean known = false;
    private boolean result = false;
  }

  /** Inspector for binary logical operators, e.g. And, Or */
  static abstract class BinaryLogical implements Inspector {
    BinaryLogical(Inspector left, Inspector right) {
      this.left = left;
      this.right = right;
    }

    public final Inspector getLeft() {
      return left;
    }

    public final Inspector getRight() {
      return right;
    }

    private final Inspector left;
    private final Inspector right;
  }

  public static final class AndInspector extends BinaryLogical {
    public AndInspector(Inspector left, Inspector right) {
      super(left, right);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static final class OrInspector extends BinaryLogical {
    public OrInspector(Inspector left, Inspector right) {
      super(left, right);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  /** Inspector for unary logical operators, e.g. Not */
  static abstract class UnaryLogical implements Inspector {
    UnaryLogical(Inspector child) {
      this.child = child;
    }

    public final Inspector getChild() {
      return child;
    }

    private final Inspector child;
  }

  public static final class NotInspector extends UnaryLogical {
    public NotInspector(Inspector child) {
      super(child);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }
}
