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

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Inspectors.Inspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.AndInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.OrInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.NotInspector;
import com.github.sadikovi.netflowlib.predicate.Visitor;
import com.github.sadikovi.netflowlib.util.WrappedByteBuf;

public final class PredicateRecordMaterializer implements RecordMaterializer, Visitor {
  public PredicateRecordMaterializer(
      Column[] columns,
      Inspector tree,
      HashMap<Column, ArrayList<ValueInspector>> columnInspectors) {
    this.tree = tree;
    this.columns = columns;
    this.numColumns = this.columns.length;
    this.filterColumns = columnInspectors.keySet().toArray(new Column[columnInspectors.size()]);
    this.numFilterColumns = this.filterColumns.length;
    this.inspectors = new HashMap<String, ArrayList<ValueInspector>>();
    for (Column col: filterColumns) {
      inspectors.put(col.getColumnName(), columnInspectors.get(col));
    }
  }

  @Override
  public Object[] processRecord(WrappedByteBuf buffer) {
    // Process filter columns, evaluate predicate upfront
    for (int i = 0; i < numFilterColumns; i++) {
      updateValueInspectors(filterColumns[i], buffer);
    }

    boolean result = tree.accept(this);
    // Reset value inspectors
    for (int i=0; i<numFilterColumns; i++) {
      resetValueInspectors(filterColumns[i]);
    }
    // record does not pass predicate, return null - record is discarded in filter iterator
    if (!result) return null;

    Object[] newRecord = new Object[numColumns];
    for (int i = 0; i < numColumns; i++) {
      newRecord[i] = columns[i].readField(buffer);
    }
    return newRecord;
  }

  private void updateValueInspectors(Column column, WrappedByteBuf buffer) {
    ArrayList<ValueInspector> ins = inspectors.get(column.getColumnName());
    for (int i = 0; i < ins.size(); i++) {
      column.updateValueInspector(buffer, ins.get(i));
    }
  }

  private void resetValueInspectors(Column column) {
    ArrayList<ValueInspector> ins = inspectors.get(column.getColumnName());
    for (int i = 0; i < ins.size(); i++) {
      ins.get(i).reset();
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
  private final Column[] columns;
  private final int numColumns;
  private final Column[] filterColumns;
  private final int numFilterColumns;
  private final HashMap<String, ArrayList<ValueInspector>> inspectors;
}
