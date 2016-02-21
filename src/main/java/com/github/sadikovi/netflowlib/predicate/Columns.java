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
   */
  public static abstract class Column<T extends Comparable<T>> implements Serializable {
    Column(byte name, Class<T> type, int offset) {
      if (offset < 0) {
        throw new IllegalArgumentException("Wrong offset " + offset);
      }

      columnName = name;
      columnType = type;
      columnOffset = offset;
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
    public abstract T getMinValue();

    /** Statistics method to return upper bound for the column. */
    public abstract T getMaxValue();

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
  }

  /** Column for byte values */
  public static class ByteColumn extends Column<Byte> {
    public ByteColumn(byte name, int offset) {
      super(name, Byte.class, offset);
    }

    @Override
    public Byte getMinValue() {
      return (byte) 0;
    }

    @Override
    public Byte getMaxValue() {
      return Byte.MAX_VALUE;
    }
  }

  /** Column for short values */
  public static class ShortColumn extends Column<Short> {
    public ShortColumn(byte name, int offset) {
      super(name, Short.class, offset);
    }

    @Override
    public Short getMinValue() {
      return (short) 0;
    }

    @Override
    public Short getMaxValue() {
      return Short.MAX_VALUE;
    }
  }

  /** Column for integer values */
  public static class IntColumn extends Column<Integer> {
    public IntColumn(byte name, int offset) {
      super(name, Integer.class, offset);
    }

    @Override
    public Integer getMinValue() {
      return (int) 0;
    }

    @Override
    public Integer getMaxValue() {
      return Integer.MAX_VALUE;
    }
  }

  /** Column for long values */
  public static class LongColumn extends Column<Long> {
    public LongColumn(byte name, int offset) {
      super(name, Long.class, offset);
    }

    @Override
    public Long getMinValue() {
      return (long) 0;
    }

    @Override
    public Long getMaxValue() {
      return Long.MAX_VALUE;
    }
  }
}
