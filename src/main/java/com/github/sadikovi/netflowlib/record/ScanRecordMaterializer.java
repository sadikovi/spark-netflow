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

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Columns.ByteColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;

/**
 * [[ScanRecordMaterializer]] is part of [[FullScan]] strategy where we only scan pruned columns.
 * Note that columns might not be unique, e.g. [IntColumn("a"), ShortColumn("b"), IntColumn("a"),
 * LongColumn("c")].
 */
public final class ScanRecordMaterializer extends RecordMaterializer {
  public ScanRecordMaterializer(Column[] columns) {
    this.columns = columns;
    numColumns = columns.length;
    recordSize = 0;
    for (int i = 0; i < numColumns; i++) {
      recordSize += this.columns[i].getUnsignedBytes();
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
  public InternalRow processRow(ByteBuf buffer) {
    byte[] data = new byte[recordSize];
    int[] offsets = new int[numColumns];
    int i = 0, offset = 0, len = 0;
    while (i < numColumns) {
      len = columns[i].getUnsignedBytes();
      System.arraycopy(buffer.array(), buffer.arrayOffset() + columns[i].getColumnOffset(),
        data, offset, len);
      offsets[i] = offset;
      offset += len;
      ++i;
    }
    return new ByteBufRow(offsets, data, buffer.order());
  }

  private final Column[] columns;
  private final int numColumns;
  // total record size in bytes for selected columns
  private int recordSize;
}
