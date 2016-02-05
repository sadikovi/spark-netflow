package com.github.sadikovi.netflowlib.statistics;

import java.nio.ByteOrder;

/**
 * Abstract interface for common statistics actions. Provides constants and util methods for
 * classes [[StatisticsReader]] and [[StatisticsWriter]].
 */
public abstract class StatisticsAction {
  public static final short MAGIC_NUMBER_1 = 0xCA;
  public static final short MAGIC_NUMBER_2 = 0x11;
  public static final short BYTE_LITTLE_ENDIAN = 1;
  public static final short BYTE_BIG_ENDIAN = 2;

  /**
   * Convert byte order to numeric representation for writing. Throws exception, if byte order
   * cannot be resolved.
   *
   * @param byteOrder byte order
   * @return order number
   * @throws IllegalArgumentException
   */
  protected short fromByteOrder(ByteOrder byteOrder) {
    if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
      return BYTE_LITTLE_ENDIAN;
    } else if (byteOrder == ByteOrder.BIG_ENDIAN) {
      return BYTE_BIG_ENDIAN;
    } else {
      throw new IllegalArgumentException("Invalid byte order " + order);
    }
  }

  /**
   * Convert order number into byte order. Throws exception, if order number cannot be resolved.
   *
   * @param orderNum order number
   * @return byte order
   * @throws IllegalArgumentException
   */
  protected ByteOrder toByteOrder(short orderNum) {
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
