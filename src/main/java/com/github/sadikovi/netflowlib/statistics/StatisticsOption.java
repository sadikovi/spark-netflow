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

/**
 * [[StatisticsOption]] interface to keep metadata for a particular field. `min` and `max` fields
 * size independent, this conversion should be done in either [[StatisticsWriter]] or
 * [[StatisticsReader]]. Currently we only support fields of size 1, 2, 4, and 8 bytes.
 */
public class StatisticsOption {

  public StatisticsOption(long field, short size, long min, long max) {
    this.field = field;
    this.size = validateSize(size);
    this.min = min;
    this.max = max;
  }

  /** Static method for byte field */
  public static StatisticsOption forField(long field, byte min, byte max) {
    return new StatisticsOption(field, (short)1, min, max);
  }

  /** Static method for short field */
  public static StatisticsOption forField(long field, short min, short max) {
    return new StatisticsOption(field, (short)2, min, max);
  }

  /** Static method for int field */
  public static StatisticsOption forField(long field, int min, int max) {
    return new StatisticsOption(field, (short)4, min, max);
  }

  /** Static method for long field */
  public static StatisticsOption forField(long field, long min, long max) {
    return new StatisticsOption(field, (short)8, min, max);
  }

  /** Size validation, we support only 1, 2 or 4 byte fields */
  private short validateSize(short size) {
    if (size == 1 || size == 2 || size == 4 || size == 8) {
      return size;
    } else {
      throw new UnsupportedOperationException("Unsupported size of field " + size);
    }
  }

  public long getField() {
    return field;
  }

  public short getSize() {
    return size;
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  public long fullLength() {
    return 0L;
  }

  private long field = 0;
  private short size = 0;
  private long min = 0;
  private long max = 0;
}
