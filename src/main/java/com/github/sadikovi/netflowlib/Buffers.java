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

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.github.sadikovi.netflowlib.record.RecordMaterializer;
import com.github.sadikovi.netflowlib.util.FilterIterator;
import com.github.sadikovi.netflowlib.util.ReadAheadInputStream;

/**
 * All buffers supported in NetFlow reader.
 *
 */
public final class Buffers {
  private Buffers() { }

  public static abstract class RecordBuffer implements Iterable<Object[]> {
    // min length of the buffer in bytes, usually 32768
    public static final int MIN_BUFFER_LENGTH = 32768;
    // length of buffer in bytes ~3Mb (option 1)
    public static final int BUFFER_LENGTH_1 = 3698688;
    // length of buffer in bytes ~1Mb (option 2)
    public static final int BUFFER_LENGTH_2 = 1048576;

    public abstract Iterator<Object[]> iterator();

    @Override
    public String toString() {
      return "Record buffer: " + getClass().getCanonicalName();
    }
  }

  /**
   * [[EmptyRecordBuffer]] is introduced for [[SkipScan]] strategy when entire file needs to be
   * skipped. Essentially returns empty iterator of records.
   */
  public static final class EmptyRecordBuffer extends RecordBuffer {
    public EmptyRecordBuffer() { }

    @Override
    public Iterator<Object[]> iterator() {
      return new Iterator<Object[]>() {
        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Object[] next() {
          throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Remove operation is not supported");
        }
      };
    }
  }

  /**
   * [[ScanRecordBuffer]] is used when full scan is required, since there is no filtering on a
   * result from [[RecordMaterializer]], though can be used in [[FilterScan]] too.
   */
  public static class ScanRecordBuffer extends RecordBuffer {
    public ScanRecordBuffer(
        DataInputStream in,
        RecordMaterializer recordMaterializer,
        int recordSize,
        ByteOrder byteOrder,
        boolean isCompressed,
        int bufferLength) {
      if (isCompressed) {
        inflater = new Inflater();
        // InflaterInputStream is replaced with ReadAheadInputStream to allow to resolve EOF before
        // actual record reading
        stream = new ReadAheadInputStream(in, inflater, bufferLength);
        compression = true;
      } else {
        inflater = null;
        stream = in;
        compression = false;
      }

      this.recordMaterializer = recordMaterializer;
      this.recordSize = recordSize;
      primary = new byte[recordSize];
      secondary = new byte[recordSize];
      buffer = Unpooled.wrappedBuffer(primary).order(byteOrder);
      numBytesRead = 0;
    }

    @Override
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
              // Since we operate on compressed stream we do not know when EOF is going to happen
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

              // Release buffer after EOF
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
            numBytesRead = stream.read(primary, 0, recordSize);
            if (numBytesRead < 0) {
              throw new IOException("EOF, " + numBytesRead + " bytes read");
            } else if (numBytesRead < recordSize) {
              // We have to read entire record when there is no compression, anything else is
              // considered failure. When stream is compressed we can read less, but then we need
              // buffer up remaning data.
              if (!compression) {
                throw new IllegalArgumentException(
                  "Failed to read record: " + numBytesRead + " < " + recordSize);
              } else {
                int remaining = recordSize - numBytesRead;
                int addBytes = stream.read(secondary, 0, remaining);
                if (addBytes != remaining) {
                  throw new IllegalArgumentException(
                    "Failed to read record: " + addBytes + " != " + remaining);
                }
                // Copy the remaning bytes into primary array
                System.arraycopy(secondary, 0, primary, numBytesRead, remaining);
              }
            }
          } catch (IOException io) {
            throw new IllegalArgumentException("Unexpected EOF", io);
          }

          return recordMaterializer.processRecord(buffer);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Remove operation is not supported");
        }
      };
    }

    @Override
    public String toString() {
      return "Record buffer: " + getClass().getCanonicalName() + "[compression: " + compression +
        ", record size: " + recordSize + "]";
    }

    // Whether or not input stream is compressed
    private final boolean compression;
    // Reference to inflater to find out EOF mainly
    private final Inflater inflater;
    // Stream to read either standard DataInputStream or InflaterInputStream
    private FilterInputStream stream;
    // Primary array of bytes for a record
    private final byte[] primary;
    // Secondary array of bytes for a record, used when compression buffer needs to be refilled
    private final byte[] secondary;
    // Buffer for the record
    private ByteBuf buffer;
    // Number of bytes currently have been read
    private int numBytesRead;
    // Size of record, depends on NetFlow format
    private final int recordSize;
    // Record materializer to process individual record
    private final RecordMaterializer recordMaterializer;
  }

  /**
   * [[FilterRecordBuffer]] is used when filtering is required on result of [[RecordMaterializer]],
   * in this case all "null" records would be skipped, e.g. records that failed predicate
   * requirement.
   */
  public static final class FilterRecordBuffer extends ScanRecordBuffer {
    public FilterRecordBuffer(
        DataInputStream in,
        RecordMaterializer recordMaterializer,
        int recordSize,
        ByteOrder byteOrder,
        boolean isCompressed,
        int bufferLength) {
      super(in, recordMaterializer, recordSize, byteOrder, isCompressed, bufferLength);
    }

    @Override
    public Iterator<Object[]> iterator() {
      return new FilterIterator<Object[]>(super.iterator());
    }
  }
}
