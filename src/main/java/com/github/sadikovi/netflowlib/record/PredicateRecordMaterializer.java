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

import com.github.sadikovi.netflowlib.codegen.CodeGenContext;
import com.github.sadikovi.netflowlib.codegen.CodeGenNode;
import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Inspectors.Inspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.AndInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.OrInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.NotInspector;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;
import com.github.sadikovi.netflowlib.predicate.Visitor;

public final class PredicateRecordMaterializer extends RecordMaterializer implements Visitor {
  public PredicateRecordMaterializer(
      Column[] columns,
      Inspector tree,
      FilterPredicate predicateTree,
      HashMap<Column, ArrayList<ValueInspector>> columnInspectors) {
    this.tree = tree;
    this.predicateTree = predicateTree;
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
  public Object[] processRecord(ByteBuf buffer) {
    // Process filter columns, evaluate predicate upfront
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

  private void updateValueInspectors(Column column, ByteBuf buffer) {
    ArrayList<ValueInspector> ins = inspectors.get(column.getColumnName());
    for (int i=0; i<ins.size(); i++) {
      updateValueInspector(column, buffer, ins.get(i));
    }
  }

  private void resetValueInspectors(Column column) {
    ArrayList<ValueInspector> ins = inspectors.get(column.getColumnName());
    for (int i=0; i<ins.size(); i++) {
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

  @Override
  protected String generateProcessRecord(CodeGenContext ctx, CodeGenNode buffer) {
    // Because we need to read filter columns and use them as variables in predicate condition,
    // we need to list unique column names and then read fields for each. Filter columns are unique,
    // as they are extracted from key set of value inspectors.
    StringBuilder code = new StringBuilder();
    // Read filter columns and resolve predicate
    for (int i=0; i<numFilterColumns; i++) {
      Column fcol = filterColumns[i];
      code.append(fcol.nodeType(ctx) + " " + fcol.nodeName(ctx) + " = " +
        generateReadField(ctx, fcol, buffer) +  ";\n");
    }
    // Create conditional record evaluation
    String condition = predicateTree.generate(ctx);
    code.append("if (" + condition + ") {\n");
    // Condition is true, create new record and fill it up with values
    CodeGenNode newRecord = generateNewRecordNode(ctx);
    code.append(newRecord.nodeType(ctx) + " " + newRecord.nodeName(ctx) +
      " = new java.lang.Object[" + numColumns + "];\n");
    for (int i=0; i<numColumns; i++) {
      code.append(newRecord.nodeName(ctx) + "[" + i + "] = " +
        generateReadField(ctx, columns[i], buffer) +  ";\n");
    }
    code.append("return " + newRecord.nodeName(ctx) + ";\n");
    code.append("} else {\n");
    code.append("return null;\n");
    code.append("}\n");
    return code.toString();
  }

  private final Inspector tree;
  private final FilterPredicate predicateTree;
  private final Column[] columns;
  private final int numColumns;
  private final Column[] filterColumns;
  private final int numFilterColumns;
  private final HashMap<String, ArrayList<ValueInspector>> inspectors;
}
