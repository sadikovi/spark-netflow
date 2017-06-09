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

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.util.WrappedByteBuf;

/**
 * [[ScanRecordMaterializer]] is part of [[FullScan]] strategy where we only scan pruned columns.
 * Note that columns might not be unique, e.g. [IntColumn("a"), ShortColumn("b"), IntColumn("a"),
 * LongColumn("c")].
 */
public final class ScanRecordMaterializer implements RecordMaterializer {
  public ScanRecordMaterializer(Column[] columns) {
    this.columns = columns;
    numColumns = columns.length;
  }

  @Override
  public Object[] processRecord(WrappedByteBuf buffer) {
    Object[] newRecord = new Object[numColumns];
    for (int i = 0; i < numColumns; i++) {
      newRecord[i] = columns[i].readField(buffer);
    }
    return newRecord;
  }

  private final Column[] columns;
  private final int numColumns;
}
