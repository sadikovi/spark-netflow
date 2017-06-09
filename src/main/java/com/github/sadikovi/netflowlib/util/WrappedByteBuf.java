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

import java.nio.ByteOrder;

/**
 * Simple alternative to java.nio.ByteBuffer with methods to read unsigned values similar to
 * netty ByteBuf.
 */
public abstract class WrappedByteBuf {
  protected final byte[] data;

  /**
   * Create wrapped byte buffer for provided byte array and required endianness of bytes in array.
   * @param data byte array
   * @param order endianness of the data in array
   * @return wrapped big/little endian byte buffer
   */
  public static WrappedByteBuf init(byte[] data, ByteOrder order) {
    return (order == ByteOrder.BIG_ENDIAN) ? new WrappedByteBufB(data) : new WrappedByteBufL(data);
  }

  // only keep reference to the data
  protected WrappedByteBuf(byte[] data) {
    this.data = data;
  }

  /**
   * Gets a byte at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return byte value
   */
  public abstract byte getByte(int ordinal);

  /**
   * Gets an unsigned byte at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return short value that represents unsigned byte
   */
  public abstract short getUnsignedByte(int ordinal);

  /**
   * Get short at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return short value
   */
  public abstract short getShort(int ordinal);

  /**
   * Gets an unsigned 16-bit short integer at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return int value that represents unsigned short
   */
  public abstract int getUnsignedShort(int ordinal);

  /**
   * Get int at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return int value
   */
  public abstract int getInt(int ordinal);

  /**
   * Gets an unsigned 32-bit integer at the specified absolute index in this buffer.
   * @param ordinal start position in buffer
   * @return long value that represents unsigned int
   */
  public abstract long getUnsignedInt(int ordinal);

  /**
   * Return reference to the underlying array.
   * @return backed byte array
   */
  public byte[] array() {
    return data;
  }

  /** Wrapped byte buffer for little endianness */
  static class WrappedByteBufL extends WrappedByteBuf {
    protected WrappedByteBufL(byte[] data) {
      super(data);
    }

    @Override
    public byte getByte(int ordinal) {
      return data[ordinal];
    }

    @Override
    public short getUnsignedByte(int ordinal) {
      return (short) (data[ordinal] & 0xff);
    }

    @Override
    public short getShort(int ordinal) {
      return (short) (data[ordinal + 1] << 8 | data[ordinal] & 0xff);
    }

    @Override
    public int getUnsignedShort(int ordinal) {
      return ((data[ordinal + 1] & 0xff) << 8) | (data[ordinal] & 0xff);
    }

    @Override
    public int getInt(int ordinal) {
      return ((data[ordinal + 3] & 0xff) << 24) |
        ((data[ordinal + 2] & 0xff) << 16) |
        ((data[ordinal + 1] & 0xff) << 8) |
        (data[ordinal + 0] & 0xff);
    }

    @Override
    public long getUnsignedInt(int ordinal) {
      return getInt(ordinal) & 0xffffffffL;
    }
  }

  /** Wrapped byte buffer for big endianness */
  static class WrappedByteBufB extends WrappedByteBuf {
    protected WrappedByteBufB(byte[] data) {
      super(data);
    }

    @Override
    public byte getByte(int ordinal) {
      return data[ordinal];
    }

    @Override
    public short getUnsignedByte(int ordinal) {
      return (short) (data[ordinal] & 0xff);
    }

    @Override
    public short getShort(int ordinal) {
      return (short) (data[ordinal] << 8 | data[ordinal + 1] & 0xff);
    }

    @Override
    public int getUnsignedShort(int ordinal) {
      return ((data[ordinal] & 0xff) << 8) | (data[ordinal + 1] & 0xff);
    }

    @Override
    public int getInt(int ordinal) {
      return ((data[ordinal] & 0xff) << 24) |
        ((data[ordinal + 1] & 0xff) << 16) |
        ((data[ordinal + 2] & 0xff) << 8) |
        (data[ordinal + 3] & 0xff);
    }

    @Override
    public long getUnsignedInt(int ordinal) {
      return getInt(ordinal) & 0xffffffffL;
    }
  }
}
