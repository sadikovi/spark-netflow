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

import java.io.IOException;
import java.nio.ByteOrder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * Interface to write NetFlow statistics as metadata to output stream provided. Handles stream
 * inside, e.g. `flush()` or `close()`, to ensure that resources are properly released after
 * reading.
 */
public class StatisticsWriter extends StatisticsAction {

  // writer buffer initial capacity
  public static final int INIT_BUF_CAPACITY = 128;

  /** Initialize statistics writer with output stream from creating a file */
  public StatisticsWriter(FSDataOutputStream out, ByteOrder order) throws IOException {
    if (out == null) {
      throw new IllegalArgumentException("Output stream is null");
    }

    if (order == null) {
      throw new IllegalArgumentException("Byte order is null");
    }

    this.out = out;
    this.order = order;

    checkStreamState();
  }

  /** StatisticsWriter with default BIG_ENDIAN byte order */
  public StatisticsWriter(FSDataOutputStream out) throws IOException {
    this(out, ByteOrder.BIG_ENDIAN);
  }

  /** Check stream non-empty state */
  private void checkStreamState() throws IOException {
    if (out.getPos() != 0L) {
      throw new IOException("Output stream is not empty");
    }
  }

  /**
   * Write metadata using Statistics object (recommended), as it does perform additional validation.
   *
   * @param stats Statistics to write
   * @return number of bytes written
   * @throws IOException
   */
  public long write(Statistics stats) throws IOException {
    return write(stats.getVersion(), stats.getCount(), stats.getOptions());
  }

  /**
   * Write metadata into output stream. This includes meta information for correct parsing, such as
   * magic numbers and byte order, header, and content of statistics options [[StatisticsOption]].
   *
   * @param version NetFlow version
   * @param count number of records
   * @param options list statistics options for certain fields
   * @return number of bytes written
   * @throws IOException
   */
  public long write(short version, long count, StatisticsOption[] options) throws IOException {
    checkStreamState();
    ByteBuf metabuf = null;

    if (options == null) {
      throw new IllegalArgumentException("Options array is null");
    }

    try {
      metabuf = Unpooled.directBuffer(INIT_BUF_CAPACITY).order(order);
      // write metadata statistics: magic number 1, magic number 2, byte order, each element takes
      // one byte of space, order independent
      metabuf.writeByte(MAGIC_NUMBER_1);
      metabuf.writeByte(MAGIC_NUMBER_2);
      metabuf.writeByte((byte)fromByteOrder(order));

      // write header (fixed size): length of header, version, and count of all the records
      int len = 10; // version 2 bytes and count 8 bytes
      metabuf.writeInt(len);
      metabuf.writeShort(version);
      metabuf.writeLong(count);

      // store full length of the options in metadata
      long fullLength = 0;
      for (StatisticsOption option: options) {
        if (option != null) {
          fullLength += STATISTICS_RECORD_SIZE;
        }
      }
      metabuf.writeLong(fullLength);

      // write options as TLV chain, we do not store length of the content
      byte[] optionArr = new byte[STATISTICS_RECORD_SIZE];
      ByteBuf optionBuf = Unpooled.wrappedBuffer(optionArr).order(order);

      for (StatisticsOption option: options) {
        if (option != null) {
          optionBuf.clear();
          // write field name
          optionBuf.writeInt((int)option.getField());
          // write field size
          short fieldSize = option.getSize();
          optionBuf.writeShort(fieldSize);
          // write min value based on size
          if (fieldSize == 1) {
            optionBuf.writeByte((byte)option.getMin());
            optionBuf.writeByte((byte)option.getMax());
          } else if (fieldSize == 2) {
            optionBuf.writeShort((short)option.getMin());
            optionBuf.writeShort((short)option.getMax());
          } else if (fieldSize == 4) {
            optionBuf.writeInt((int)option.getMin());
            optionBuf.writeInt((int)option.getMax());
          } else if (fieldSize == 8) {
            optionBuf.writeLong(option.getMin());
            optionBuf.writeLong(option.getMax());
          } else {
            throw new UnsupportedOperationException("Unsupported field size " + fieldSize);
          }

          metabuf.writeBytes(optionArr);
        }
      }

      // clear byte array and release option buffer
      if (optionBuf != null && optionBuf.refCnt() > 0) {
        optionBuf.release(optionBuf.refCnt());
      }
      optionArr = null;
      optionBuf = null;

      int writtenBytes = metabuf.readableBytes();
      metabuf.readBytes(out, writtenBytes);
      out.hflush();

      return (long)writtenBytes;
    } finally {
      if (metabuf != null && metabuf.refCnt() > 0) {
        metabuf.release(metabuf.refCnt());
      }
      metabuf = null;
      out.close();
    }
  }

  private FSDataOutputStream out = null;
}
