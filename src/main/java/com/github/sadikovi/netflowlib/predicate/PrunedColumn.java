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

import com.github.sadikovi.netflowlib.predicate.Columns.Column;

/**
 * [[PrunedColumn]] interface provides a wrapper for a column with an ability to set special flags
 * to it, such as whether or not column is a leaf of predicate tree, or both. Every column passed
 * should be converted to a PrunedColumn. Note that it does not require list of columns to be
 * unique, caller should ensure the validitiy of flags set.
 * @param column Column object to wrap
 * @param flags special bit vector of flags
 */
public final class PrunedColumn {
  // flag for filtered column
  public static final byte FILTERED_COLUMN = 1;

  public PrunedColumn(Column<?> column, byte flags) {
    this.column = column;
    this.flags = flags;
  }

  public PrunedColumn(Column<?> column) {
    this(column, (byte)0);
  }

  /** Set arbitrary flag, or group of flags */
  public void setFlag(byte flag) {
    flags |= flag;
  }

  /** Whether or not flag is set */
  public boolean hasFlag(byte flag) {
    return (flags & flag) > 0;
  }

  public Column<?> getColumn() {
    return column;
  }

  public byte getFlags() {
    return flags;
  }

  @Override
  public String toString() {
    return "Pruned[" + column.toString() + "]{" + flags + "}";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;

    PrunedColumn that = (PrunedColumn) obj;

    if (!column.equals(that.column)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = column.hashCode();
    result = 31 * result + getClass().hashCode();
    return result;
  }

  private Column<?> column;
  private byte flags;
}
