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

import com.github.sadikovi.netflowlib.statistics.Statistics;

/**
 * [[Column]] is a base class for all typed columns for all NetFlow versions. They contain basic
 * information about name, type, and offset in the record, so it allows to use standard interface of
 * [[RecordMaterializer]] for parsing a row.
 */
public final class Columns {
  private Columns() { }

  public static abstract class Column implements Serializable, Statistics {
    Column() { }

    Column(String name, Class<?> type, int offset) {
      if (offset < 0) {
        throw new IllegalArgumentException("Wrong offset " + offset);
      }

      columnName = name;
      columnType = type;
      columnOffset = offset;
    }

    /** Get column name */
    public String getColumnName() {
      return columnName;
    }

    /** Get column type */
    public Class<?> getColumnType() {
      return columnType;
    }

    /** Get column offset in a record */
    public int getColumnOffset() {
      return columnOffset;
    }

    @Override
    public String toString() {
      return "Column(" + columnName + ")[" + columnType.getSimpleName() + "][" + columnOffset + "]";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      Column that = (Column) obj;

      if (!columnType.equals(that.columnType)) return false;
      if (!columnName.equals(that.columnName)) return false;
      if (columnOffset != that.columnOffset) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = columnName.hashCode();
      result = 31 * result + columnType.hashCode();
      result = 31 * result + columnOffset;
      return result;
    }

    private String columnName = null;
    private Class<?> columnType = null;
    private int columnOffset = -1;
  }

  /** Column for byte values */
  public static class ByteColumn extends Column {
    ByteColumn() { }

    ByteColumn(String name, int offset, byte min, byte max) {
      super(name, Byte.class, offset);
      minValue = min;
      maxValue = max;
    }

    public ByteColumn(String name, int offset) {
      this(name, offset, (byte) 0, Byte.MAX_VALUE);
    }

    @Override
    public Object getMin() {
      return minValue;
    }

    @Override
    public Object getMax() {
      return maxValue;
    }

    private byte minValue;
    private byte maxValue;
  }

  /** Column for short values */
  public static class ShortColumn extends Column {
    ShortColumn() { }

    ShortColumn(String name, int offset, short min, short max) {
      super(name, Short.class, offset);
      minValue = min;
      maxValue = max;
    }

    public ShortColumn(String name, int offset) {
      this(name, offset, (short) 0, Short.MAX_VALUE);
    }

    @Override
    public Object getMin() {
      return minValue;
    }

    @Override
    public Object getMax() {
      return maxValue;
    }

    private short minValue;
    private short maxValue;
  }

  /** Column for integer values */
  public static class IntColumn extends Column {
    IntColumn() { }

    IntColumn(String name, int offset, int min, int max) {
      super(name, Integer.class, offset);
      minValue = min;
      maxValue = max;
    }

    public IntColumn(String name, int offset) {
      this(name, offset, (int) 0, Integer.MAX_VALUE);
    }

    @Override
    public Object getMin() {
      return minValue;
    }

    @Override
    public Object getMax() {
      return maxValue;
    }

    private int minValue;
    private int maxValue;
  }

  /** Column for long values */
  public static class LongColumn extends Column {
    LongColumn() { }

    LongColumn(String name, int offset, long min, long max) {
      super(name, Long.class, offset);
      minValue = min;
      maxValue = max;
    }

    public LongColumn(String name, int offset) {
      this(name, offset, (long) 0, Long.MAX_VALUE);
    }

    @Override
    public Object getMin() {
      return minValue;
    }

    @Override
    public Object getMax() {
      return maxValue;
    }

    private long minValue;
    private long maxValue;
  }
}
