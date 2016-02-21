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

/**
 * [[Column]] is a base class for all typed columns for all NetFlow versions. They contain basic
 * information about name, type, and offset in the record, so [[FlowInterface]] can use standard
 * method for parsing a row, otherwise you should be able to override and provide your own
 * implementation. This will also allow us to change column name to String with minimum work.
 */
public final class Columns {
  private Columns() { }

  /**
   * [[Column]] is a base class for all the columns in the library, provides simple interface for
   * column name, type, offset and other features that might be added later. Column interface brings
   * type safety to the parsing.
   * @param columnName name of the column
   * @param columnType type of the column
   * @param offset offset in the record for that column (index in the buffer)
   * @param min lower bound for the column (statistics property)
   * @param max upper bound for the column (statistics property)
   */
  public static abstract class Column<T extends Comparable<T>> implements Serializable {
    Column(byte name, Class<T> type, int offset, T min, T max) {
      if (offset < 0) {
        throw new IllegalArgumentException("Wrong offset " + offset);
      }

      columnName = name;
      columnType = type;
      columnOffset = offset;
      minValue = min;
      maxValue = max;
    }

    /** Return column name */
    public byte getColumnName() {
      return columnName;
    }

    public Class<T> getColumnType() {
      return columnType;
    }

    public int getColumnOffset() {
      return columnOffset;
    }

    /** Statistics method to return lower bound for the column. */
    public T getMinValue() {
      return minValue;
    }

    /** Statistics method to return upper bound for the column. */
    public T getMaxValue() {
      return maxValue;
    }

    @Override
    public String toString() {
      return "column(" + columnName + ")[" + columnType.getSimpleName() + "][" + columnOffset + "]";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      Column that = (Column) obj;

      if (!columnType.equals(that.columnType)) return false;
      if (columnName != that.columnName) return false;
      if (columnOffset != that.columnOffset) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = columnName;
      result = 31 * result + columnType.hashCode();
      result = 31 * result + columnOffset;
      return result;
    }

    private final byte columnName;
    private final Class<T> columnType;
    private final int columnOffset;
    private final T minValue;
    private final T maxValue;
  }

  /** Column for byte values */
  public static class ByteColumn extends Column<Byte> {
    /** private constructor for statistics column */
    ByteColumn(byte name, int offset, byte min, byte max) {
      super(name, Byte.class, offset, min, max);
    }

    public ByteColumn(byte name, int offset) {
      this(name, offset, (byte) 0, Byte.MAX_VALUE);
    }
  }

  /** Column for short values */
  public static class ShortColumn extends Column<Short> {
    ShortColumn(byte name, int offset, short min, short max) {
      super(name, Short.class, offset, min, max);
    }

    public ShortColumn(byte name, int offset) {
      this(name, offset, (short) 0, Short.MAX_VALUE);
    }
  }

  /** Column for integer values */
  public static class IntColumn extends Column<Integer> {
    IntColumn(byte name, int offset, int min, int max) {
      super(name, Integer.class, offset, min, max);
    }

    public IntColumn(byte name, int offset) {
      this(name, offset, (int) 0, Integer.MAX_VALUE);
    }
  }

  /** Column for long values */
  public static class LongColumn extends Column<Long> {
    LongColumn(byte name, int offset, long min, long max) {
      super(name, Long.class, offset, min, max);
    }

    public LongColumn(byte name, int offset) {
      this(name, offset, (long) 0, Long.MAX_VALUE);
    }
  }
}
