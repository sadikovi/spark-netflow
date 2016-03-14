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

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * [[ReadAheadInputStream]] allows to check if compressed stream is empty using stream API,
 * without calling `read()` to verify if EOF is reached. Standard method `available()` or
 * `finished()` method of the `Inflater` instance can be used.
 */
public class ReadAheadInputStream extends InflaterInputStream {
  public ReadAheadInputStream(InputStream in, Inflater inf, int size) {
    super(in, inf, size);
    try {
      firstByte = (byte)super.read();
    } catch (IOException ioe) {
      throw new UnsupportedOperationException(
        "Unexpected EOF when reading first bytes, might indicate corrupt input", ioe);
    }
    isOffsetActive = firstByte != -1;
  }

  public ReadAheadInputStream(InputStream in, Inflater inf) {
    super(in, inf);
  }

  /** Check if stream is still open */
  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    ensureOpen();
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }
    // If offset is not used yet, we correct read buffer by overwriting first byte and fetching
    // less data from the stream. Note that we have to return correct number of bytes read including
    // first byte.
    if (isOffsetActive) {
      b[off] = firstByte;
      isOffsetActive = false;
      return super.read(b, off + 1, len - 1) + 1;
    } else {
      return super.read(b, off, len);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    // Similar to `read()` we have to make a correction to the fact that we have used first byte.
    // Though bytes skipped are less than provided, actual returned number of bytes is corrected by
    // the offset.
    if (isOffsetActive) {
      isOffsetActive = false;
      n = n - 1;
      return super.skip(n) + 1;
    } else {
      return super.skip(n);
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      inf.end();
      in.close();
      closed = true;
    }
  }

  private boolean closed = false;
  private boolean isOffsetActive;
  private byte firstByte;
}
