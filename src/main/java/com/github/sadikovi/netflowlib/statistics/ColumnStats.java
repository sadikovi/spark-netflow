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

package com.github.sadikovi.netflowlib.statistics;

import java.io.Serializable;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;

/**
 * [[ColumnStats]] interface defines all possible statistics for column.
 */
public final class ColumnStats<T extends Comparable<T>> implements Serializable {
  public ColumnStats(Column<T> column) {
    if (column == null) {
      throw new IllegalArgumentException("Statistics column is undefined");
    }

    this.column = column;
  }

  public Column<T> getColumn() {
    return column;
  }

  public void setMin(T value) {
    min = value;
  }

  public void setMax(T value) {
    max = value;
  }

  public T getMin() {
    return min;
  }

  public T getMax() {
    return max;
  }

  private final Column<T> column;
  private T max;
  private T min;
}
