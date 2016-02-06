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

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Interface to read NetFlow statistics written by [[StatisticsWriter]]. Handles passed stream, so
 * it is guaranteed to close stream after reading.
 */
public class StatisticsReader extends StatisticsAction {
  public StatisticsReader(FSDataInputStream in, ByteOrder order) {
    if (in == null) {
      throw new IllegalArgumentException("Input stream is null");
    }

    if (order == null) {
      throw new IllegalArgumentException("Byte order is null");
    }

    this.in = in;
    this.order = order;
  }

  public StatisticsReader(FSDataInputStream in) {
    this(in, ByteOrder.BIG_ENDIAN);
  }

  /**
   * Read statistics data from input stream provided. Reading order is as follows:
   * - meta statistics (magic numbers, byte order). After this byte order is refreshed.
   * - header version, length, and count fields
   * - length, content until length equals num bytes read
   *
   * @return Statistics object
   * @throws IOException
   */
  public Statistics read() throws IOException {
    // place reader index at the beginning of the stream
    in.seek(0);
    byte[] bytearr = null;
    ByteBuf buf = null;

    try {
      // parse meta statistics: magic numbers and byte order, and update byte order
      bytearr = new byte[3];
      buf = Unpooled.wrappedBuffer(bytearr);
      in.read(bytearr, 0, bytearr.length);

      short magic1 = buf.readUnsignedByte();
      short magic2 = buf.readUnsignedByte();
      short byteOrder = buf.readUnsignedByte();
      buf.release();

      if (magic1 != MAGIC_NUMBER_1 || magic2 != MAGIC_NUMBER_2) {
        throw new IOException("Invalid input: Wrong magic number");
      }

      order = toByteOrder(byteOrder);

      // parse header: version, count, length
      bytearr = new byte[22]; // length of the header + version + count + content length
      in.read(bytearr, 0, bytearr.length);
      buf = Unpooled.wrappedBuffer(bytearr).order(order);

      long headerLength = buf.readUnsignedInt();
      short version = buf.readShort();
      long count = buf.readLong();
      long contentLength = buf.readLong();
      buf.release();

      // parse content
      StatisticsOption[] opts;

      if (contentLength == 0) {
        opts = new StatisticsOption[0];
      } else {
        int numOptions = (int)(contentLength / STATISTICS_RECORD_SIZE);
        if (contentLength % STATISTICS_RECORD_SIZE != 0) {
          throw new IOException("Statistics content is corrupted, length " + contentLength);
        }

        opts = new StatisticsOption[numOptions];
        // reuse bytearr and buf instances
        bytearr = new byte[STATISTICS_RECORD_SIZE];
        buf = Unpooled.wrappedBuffer(bytearr).order(order);

        for (int i=0; i<numOptions; i++) {
          buf.readerIndex(0);
          in.read(bytearr, 0, bytearr.length);

          long fieldName = buf.readUnsignedInt();
          short fieldSize = buf.readShort();
          long min = 0;
          long max = 0;

          if (fieldSize == 1) {
            min = buf.readUnsignedByte();
            max = buf.readUnsignedByte();
          } else if (fieldSize == 2) {
            min = buf.readShort();
            max = buf.readShort();
          } else if (fieldSize == 4) {
            min = buf.readInt();
            max = buf.readInt();
          } else if (fieldSize == 8) {
            min = buf.readLong();
            max = buf.readLong();
          } else {
            throw new UnsupportedOperationException("Unsupported field size " + fieldSize);
          }

          opts[i] = new StatisticsOption(fieldName, fieldSize, min, max);
        }
      }

      return new Statistics(version, count, opts);
    } finally {
      if (buf != null && buf.refCnt() > 0) {
        buf.release(buf.refCnt());
      }
      buf = null;
      in.close();
    }
  }

  private FSDataInputStream in = null;
}
