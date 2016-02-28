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

package com.github.sadikovi.netflowlib.record;

import java.util.ArrayList;
import java.util.HashMap;

import io.netty.buffer.ByteBuf;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Inspectors.Inspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.AndInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.OrInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.NotInspector;
import com.github.sadikovi.netflowlib.predicate.Visitor;

public final class PredicateRecordMaterializer extends RecordMaterializer implements Visitor {
  public PredicateRecordMaterializer(
      Column<?>[] columns,
      Inspector tree,
      HashMap<Column<?>, ArrayList<ValueInspector>> inspectors) {
    this.tree = tree;
    this.columns = columns;
    this.numColumns = this.columns.length;
    this.filterColumns = inspectors.keySet().toArray(new Column<?>[inspectors.size()]);
    this.numFilterColumns = this.filterColumns.length;
    this.inspectors = new HashMap<String, ArrayList<ValueInspector>>();
    for (Column<?> col: inspectors.keySet()) {
      this.inspectors.put(col.getColumnName(), inspectors.get(col));
    }
  }

  @Override
  public Object[] processRecord(ByteBuf buffer) {
    // Process filter columns, evaluate predicate and decide whether or not to proceed scanning
    for (int i=0; i<numFilterColumns; i++) {
      updateValueInspectors(filterColumns[i], buffer);
    }

    boolean result = tree.accept(this);
    // Reset value inspectors
    for (int i=0; i<numFilterColumns; i++) {
      resetValueInspectors(filterColumns[i]);
    }

    if (!result) {
      return null;
    } else {
      Object[] newRecord = new Object[numColumns];
      for (int i=0; i<numColumns; i++) {
        newRecord[i] = readField(columns[i], buffer);
      }
      return newRecord;
    }
  }

  private <T extends Comparable<T>> void updateValueInspectors(Column<T> column, ByteBuf buffer) {
    ArrayList<ValueInspector> ins = inspectors.get(column.getColumnName());
    for (ValueInspector vi: ins) {
      if (column.getColumnType() == Byte.class) {
        vi.update(buffer.getByte(column.getColumnOffset()));
      } else if (column.getColumnType() == Short.class) {
        vi.update(buffer.getUnsignedByte(column.getColumnOffset()));
      } else if (column.getColumnType() == Integer.class) {
        vi.update(buffer.getUnsignedShort(column.getColumnOffset()));
      } else if (column.getColumnType() == Long.class) {
        vi.update(buffer.getUnsignedInt(column.getColumnOffset()));
      } else {
        throw new UnsupportedOperationException("Unsupported read type " +
          column.getColumnType().toString());
      }
    }
  }

  private <T extends Comparable<T>> void resetValueInspectors(Column<T> column) {
    ArrayList<ValueInspector> ins = inspectors.get(column.getColumnName());
    for (ValueInspector vi: ins) {
      vi.reset();
    }
  }

  @Override
  public boolean visit(ValueInspector inspector) {
    return inspector.getResult();
  }

  @Override
  public boolean visit(AndInspector inspector) {
    return inspector.getLeft().accept(this) && inspector.getRight().accept(this);
  }

  @Override
  public boolean visit(OrInspector inspector) {
    return inspector.getLeft().accept(this) || inspector.getRight().accept(this);
  }

  @Override
  public boolean visit(NotInspector inspector) {
    return !inspector.getChild().accept(this);
  }

  private final Inspector tree;
  private final Column<?>[] columns;
  private final int numColumns;
  private final Column<?>[] filterColumns;
  private final int numFilterColumns;
  private final HashMap<String, ArrayList<ValueInspector>> inspectors;
}
