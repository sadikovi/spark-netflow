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

public final class StatisticsTypes {
  private StatisticsTypes() { }

  /**
   * [[GenericStatistics]] interface to keep track of minimum and maximum values.
   */
  public static abstract class GenericStatistics<T extends Comparable<T>>
      implements Statistics, Serializable {
    GenericStatistics(T min, T max) {
      if (min.compareTo(max) > 0) {
        throw new IllegalArgumentException("Min " + min + " is greater than max " + max);
      }

      this.min = min;
      this.max = max;
    }

    @Override
    public T getMin() {
      return min;
    }

    @Override
    public T getMax() {
      return max;
    }

    private final T max;
    private final T min;
  }

  public static final class ByteStatistics extends GenericStatistics<Byte> {
    public ByteStatistics(byte min, byte max) {
      super(min, max);
    }
  }

  public static final class ShortStatistics extends GenericStatistics<Short> {
    public ShortStatistics(short min, short max) {
      super(min, max);
    }
  }

  public static final class IntStatistics extends GenericStatistics<Integer> {
    public IntStatistics(int min, int max) {
      super(min, max);
    }
  }

  public static final class LongStatistics extends GenericStatistics<Long> {
    public LongStatistics(long min, long max) {
      super(min, max);
    }
  }
}
