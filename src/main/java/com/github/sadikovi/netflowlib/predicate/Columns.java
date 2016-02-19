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
  static abstract class Column<T> implements Serializable {
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
  public static final class ByteColumn extends Column<Byte> {
    public ByteColumn(byte name, int offset) {
      super(name, Byte.class, offset);
    }
  }

  /** Column for short values */
  public static final class ShortColumn extends Column<Short> {
    public ShortColumn(byte name, int offset) {
      super(name, Short.class, offset);
    }
  }

  /** Column for integer values */
  public static final class IntColumn extends Column<Integer> {
    public IntColumn(byte name, int offset) {
      super(name, Integer.class, offset);
    }
  }

  /** Column for long values */
  public static final class LongColumn extends Column<Long> {
    public LongColumn(byte name, int offset) {
      super(name, Long.class, offset);
    }
  }
}
