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

import io.netty.buffer.ByteBuf;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Columns.ByteColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;

public final class ScanRecordMaterializer extends RecordMaterializer {
  public ScanRecordMaterializer(Column[] selectedColumns) {
    if (selectedColumns.length == 0) {
      throw new IllegalArgumentException("No columns to scan");
    }
    
    numColumns = selectedColumns.length;
    columns = selectedColumns;
  }

  /** Read buffer bytes sequence for column offset */
  private Object readField(Column<?> column, ByteBuf buffer) {
    if (column.getColumnType() == Byte.class) {
      return buffer.getByte(column.getColumnOffset());
    } else if (column.getColumnType() == Short.class) {
      return buffer.getUnsignedByte(column.getColumnOffset());
    } else if (column.getColumnType() == Integer.class) {
      return buffer.getUnsignedShort(column.getColumnOffset());
    } else if (column.getColumnType() == Long.class) {
      return buffer.getUnsignedInt(column.getColumnOffset());
    } else {
      throw new UnsupportedOperationException("Unsupported read type " + column.getColumnType());
    }
  }

  @Override
  public Object[] processRecord(ByteBuf buffer) {
    Object[] newRecord = new Object[numColumns];

    for (int i=0; i<numColumns; i++) {
      newRecord[i] = readField(columns[i], buffer);
    }

    return newRecord;
  }

  @Override
  public Column[] getColumns() {
    return columns;
  }

  @Override
  public FilterPredicate getPredicateTree() {
    throw new UnsupportedOperationException("ScanRecordMaterializer does not support " +
      "predicate tree");
  }

  private int numColumns;
  private final Column[] columns;
}
