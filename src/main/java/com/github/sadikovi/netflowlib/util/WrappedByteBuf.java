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

package com.github.sadikovi.netflowlib.util;

/**
 * Simple alternative to java.nio.ByteBuffer with methods to read unsigned values similar to
 * netty ByteBuf.
 */
public class WrappedByteBuf {
  // only keep reference to the data
  public WrappedByteBuf(byte[] data) {
    this.data = data;
  }

  /**
   * Gets a byte at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return byte value
   */
  public byte getByte(int ordinal) {
    return data[ordinal];
  }

  /**
   * Gets an unsigned byte at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return short value that represents unsigned byte
   */
  public short getUnsignedByte(int ordinal) {
    return (short) (data[ordinal] & 0xff);
  }

  /**
   * Get short at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return short value
   */
  public short getShort(int ordinal) {
    return (short) (data[ordinal] << 8 | data[ordinal + 1] & 0xff);
  }

  /**
   * Gets an unsigned 16-bit short integer at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return int value that represents unsigned short
   */
  public int getUnsignedShort(int ordinal) {
    return ((data[ordinal] & 0xff) << 8) | (data[ordinal + 1] & 0xff);
  }

  /**
   * Get int at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return int value
   */
  public int getInt(int ordinal) {
    return ((data[ordinal] & 0xff) << 24) |
      ((data[ordinal + 1] & 0xff) << 16) |
      ((data[ordinal + 2] & 0xff) << 8) |
      (data[ordinal + 3] & 0xff);
  }

  /**
   * Gets an unsigned 32-bit integer at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return long value that represents unsigned int
   */
  public long getUnsignedInt(int ordinal) {
    return getInt(ordinal) & 0xffffffffL;
  }

  /**
   * Return reference to the underlying array.
   * @return backed byte array
   */
  public byte[] array() {
    return data;
  }

  private final byte[] data;
}
