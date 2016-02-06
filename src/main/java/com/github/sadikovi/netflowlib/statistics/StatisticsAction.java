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

import java.nio.ByteOrder;

/**
 * Abstract interface for common statistics actions. Provides constants and util methods for
 * classes [[StatisticsReader]] and [[StatisticsWriter]].
 */
public abstract class StatisticsAction {
  public static final short MAGIC_NUMBER_1 = 0xCA;
  public static final short MAGIC_NUMBER_2 = 0x11;
  // byte encoding for byte order LITTLE_ENDIAN
  public static final short BYTE_LITTLE_ENDIAN = 1;
  // byte encoding for byte order BIG_ENDIAN
  public static final short BYTE_BIG_ENDIAN = 2;
  // statistics record size in bytes
  public static final int STATISTICS_RECORD_SIZE = 24;

  /**
   * Convert byte order to numeric representation for writing. Throws exception, if byte order
   * cannot be resolved.
   *
   * @param byteOrder byte order
   * @return order number
   * @throws IllegalArgumentException
   */
  protected static short fromByteOrder(ByteOrder byteOrder) {
    if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
      return BYTE_LITTLE_ENDIAN;
    } else if (byteOrder == ByteOrder.BIG_ENDIAN) {
      return BYTE_BIG_ENDIAN;
    } else {
      throw new IllegalArgumentException("Invalid byte order " + byteOrder);
    }
  }

  /**
   * Convert order number into byte order. Throws exception, if order number cannot be resolved.
   *
   * @param orderNum order number
   * @return byte order
   * @throws IllegalArgumentException
   */
  protected static ByteOrder toByteOrder(short orderNum) {
    if (orderNum == BYTE_LITTLE_ENDIAN) {
      return ByteOrder.LITTLE_ENDIAN;
    } else if (orderNum == BYTE_BIG_ENDIAN) {
      return ByteOrder.BIG_ENDIAN;
    } else {
      throw new IllegalArgumentException("Invalid order num " + orderNum);
    }
  }

  protected ByteOrder order = null;
}
