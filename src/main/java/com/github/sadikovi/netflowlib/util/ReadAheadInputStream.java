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
 * [[ReadAheadInputStream]] interface is a simple wrapper around InflaterInputStream. Provides
 * convinient methods to check end of stream through inflater availability, even when compressed
 * stream is empty. Standard methods `available()` method can be used to check if stream is
 * finished.
 * See: https://docs.oracle.com/javase/7/docs/api/java/util/zip/InflaterInputStream.html
 */
public class ReadAheadInputStream extends InflaterInputStream {
  public ReadAheadInputStream(InputStream in, Inflater inf, int size) {
    super(in, inf, size);
    int maybeFirstByte;
    try {
      maybeFirstByte = super.read();
    } catch (IOException ioe) {
      throw new UnsupportedOperationException(
        "Unexpected EOF when reading first bytes, might indicate corrupt input", ioe);
    }

    // We always use first byte offset for the second read
    useFirstByteOffset = true;
    // If `maybeFirstByte` returns -1, we have reached EOF, after this point `super.available()`
    // will report stream status correctly
    isEOF = maybeFirstByte == -1;
    // First byte is considered to be unsigned, but we can still assign 255 as -1 byte, or -1
    // directly, since it will be EOF in latter case anyway.
    firstByte = (byte) maybeFirstByte;
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
    if (useFirstByteOffset) {
      useFirstByteOffset = false;
      b[off] = firstByte;
      return super.read(b, off + 1, len - 1) + 1;
    } else {
      return super.read(b, off, len);
    }
  }

  @Override
  public int available() throws IOException {
    // `available()` returns 0, if EOF has been reached, but in case of empty compressed stream, it
    // is unclear, and `available()` will return 1.
    // for `ReadAheadInputStream` availability is determined either by first byte offset, whether
    // or read was successful or based on `finished()` method of Inflater instance.
    // Note that `available()` does not reset `useFirstByteOffset`.
    if (useFirstByteOffset) {
      return isEOF ? 0 : 1;
    } else {
      return inf.finished() ? 0 : 1;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    if (n < 0) {
      throw new IllegalArgumentException("negative skip length");
    }

    ensureOpen();
    if (n == 0) {
      return 0;
    }
    // Similar to `read()` we have to make a correction to the fact that we have used first byte.
    // Though bytes skipped are less than provided, actual returned number of bytes is corrected by
    // the offset.
    if (useFirstByteOffset) {
      useFirstByteOffset = false;
      return super.skip(n - 1) + 1;
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

  // Flag to indicate whether or not it is end of stream, syncronized with parent
  private boolean isEOF = false;
  // Flag to show whether or not underlying stream is closed
  private boolean closed = false;
  // Flag to indicate if we need to take offset into account
  private boolean useFirstByteOffset;
  // When `useFirstByteOffset` we have to include first byte read
  private byte firstByte;
}
