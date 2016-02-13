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

package com.github.sadikovi.netflowlib;

import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.hadoop.fs.FSDataInputStream;

public class RecordBuffer implements Iterable<Object[]> {
  // length of the buffer, usually 32768
  public static final int BUFFER_LENGTH = 32768;

  // length of buffer ~3Mb (option 1)
  public static final int BUFFER_LENGTH_1 = 3698688;

  /**
   * Create RecordBuffer with default buffer length.
   *
   * @param in input stream of raw data
   * @param hl aggregator, flow interface
   * @param ord byte order to use when creating buffer to read record
   * @param isCmp boolean flag, showing that raw data is compressed
   * @return iterator of records as Object[]
   */
  public RecordBuffer(FSDataInputStream in, FlowInterface hl, ByteOrder ord, boolean isCmp) {
    this(in, hl, ord, isCmp, BUFFER_LENGTH);
  }

  /**
   * Create RecordBuffer using input stream of data.
   *
   * @param in input stream of raw data
   * @param hl record holder, currently only V5 is supported
   * @param ord byte order to use when creating buffer to read record
   * @param isCmp boolean flag, showing that raw data is compressed
   * @param bufLen length of buffer for compressed stream
   * @return iterator of records as Object[]
   */
  public RecordBuffer(
      FSDataInputStream in,
      FlowInterface hl,
      ByteOrder ord,
      boolean isCmp,
      int bufLen) {
    RECORD_SIZE = hl.size();
    // instantiate appropriate stream for the compressed / uncompressed file
    if (isCmp) {
      this.inflater = new Inflater();
      stream = new InflaterInputStream(in, this.inflater, bufLen);
      compression = true;
    } else {
      stream = in;
      compression = false;
    }

    // record holder provides general methods such as "size()", and processing of a record
    recordHolder = hl;
    // primary array to read bytes from
    primary = new byte[RECORD_SIZE];
    // secondary array to fill up primary in case of compression
    secondary = new byte[RECORD_SIZE];
    // record buffer, wrapped so allocated once only
    buffer = Unpooled.wrappedBuffer(primary).order(ord);
    // number of bytes read from the stream
    numBytes = 0;
  }

  ////////////////////////////////////////////////////////////
  // Iterator API
  ////////////////////////////////////////////////////////////

  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      @Override
      public boolean hasNext() {
        // `stream.available()` returns either [0, 1] in case of compressed stream and
        // number of bytes left in case of uncompressed stream. When it fails then we
        // reach EOF.
        boolean hasNext = true;
        try {
          if (compression) {
            // since we operate on compressed stream we do not know when EOF is going to happen
            // unless it has already happened. So we check inflater to see if it is finished.
            hasNext = !inflater.finished();
          } else {
            hasNext = stream.available() > 0;
          }
        } catch (IOException io) {
          return false;
        } finally {
          if (!hasNext) {
            try {
              stream.close();
            } catch (IOException io) {
              stream = null;
            }

            // close buffer after EOF
            if (buffer != null && buffer.refCnt() > 0) {
              buffer.release(buffer.refCnt());
            }
            buffer = null;
          }
          return hasNext;
        }
      }

      @Override
      public Object[] next() {
        try {
          numBytes = stream.read(primary, 0, RECORD_SIZE);
          if (numBytes < 0) {
            throw new IOException("EOF");
          } else if (numBytes < RECORD_SIZE) {
            // We have to read entire record when there is no compression, anything else is
            // considered failure. When stream is compressed we can read less, but then we need
            // buffer up remaning data.
            if (!compression) {
              throw new IllegalArgumentException(
                "Failed to read record: " + numBytes + " < " + RECORD_SIZE);
            } else {
              int remaining = RECORD_SIZE - numBytes;
              int addBytes = stream.read(secondary, 0, remaining);
              if (addBytes != remaining) {
                throw new IllegalArgumentException(
                  "Failed to read record: " + addBytes + " != " + remaining);
              }
              // Copy the remaning bytes into primary array
              System.arraycopy(secondary, 0, primary, numBytes, remaining);
            }
          }
        } catch (IOException io) {
          throw new IllegalArgumentException("Unexpected EOF");
        }

        return recordHolder.processRecord(buffer);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Remove operation is not supported");
      }
    };
  }

  // flag to indicate compression
  private boolean compression;
  // reference to inflater to find out EOF mainly
  private Inflater inflater;
  // stream to read either standard DataInputStream or InflaterInputStream
  private FilterInputStream stream;
  // record holder (subclass of FlowInterface with information how to parse single record)
  private FlowInterface recordHolder;
  // primary array of bytes for a record
  private byte[] primary;
  // secondary array of bytes for a record, used when compression buffer needs to be refilled
  private byte[] secondary;
  // buffer for the record
  private ByteBuf buffer;
  // number of bytes currently have been read
  private int numBytes;
  // size of record, depends on NetFlow format
  private int RECORD_SIZE;
}
