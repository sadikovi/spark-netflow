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
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BufferInputStream extends InputStream {
  // Create single thread executor service
  private ExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition readAsyncFinished = lock.newCondition();
  private final InputStream in;
  private final byte[] oneByte;
  private ByteBuffer buffer;
  private byte[] active;
  private byte[] async;
  private int readAsyncBytes;
  private RuntimeException asyncError;
  private boolean canSwap;

  public BufferInputStream(InputStream in, int size) {
    this.in = in;
    // for `read()` calls
    oneByte = new byte[1];
    // active buffer for reads
    active = new byte[size];
    async = new byte[size];
    asyncError = null;
    readAsyncBytes = 0;
    canSwap = false;
    buffer = ByteBuffer.wrap(active);
    buffer.flip();
  }

  private void swap() {
    lock.lock();
    try {
      while (!canSwap) {
        readAsyncFinished.await();
      }
      byte[] tmp = active;
      active = async;
      async = tmp;
      // when it is EOF, assign 0 bytes as length, buffer will have 0 bytes remaining
      buffer = ByteBuffer.wrap(active, 0, Math.max(0, readAsyncBytes));
    } catch (Exception err) {
      throw new RuntimeException(err);
    } finally {
      lock.unlock();
    }
  }

  private void readAsync() {
    service.execute(new Runnable() {
      @Override
      public void run() {
        lock.lock();
        canSwap = false;
        try {
          readAsyncBytes = in.read(async, 0, async.length);
        } catch (Exception err) {
          asyncError = new RuntimeException(err);
        } finally {
          canSwap = true;
          readAsyncFinished.signalAll();
          lock.unlock();
        }
      }
    });
  }

  private void fill() {
    if (asyncError != null) throw asyncError;
    if (buffer.remaining() > 0) return;
    readAsync();
    swap();
  }

  @Override
  public int read() throws IOException {
    if (read(oneByte, 0, 1) > 0) return oneByte[0] & 0xff;
    return -1;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    fill();
    // EOF is reached
    if (buffer.remaining() == 0) {
      return -1;
    }
    // at this point we know that there is at least 1 byte available
    // try inserting as many bytes as possible
    int left = len, offset = 0;
    while (left > 0 && buffer.remaining() > 0) {
      int copyLen = Math.min(left, buffer.remaining());
      buffer.get(b, off + offset, copyLen);
      left -= copyLen;
      offset += copyLen;
      fill();
    }
    return len - left;
  }

  @Override
  public int available() throws IOException {
    if (buffer.remaining() > 0) return 1;
    fill();
    return buffer.remaining() > 0 ? 1 : 0;
  }

  @Override
  public long skip(long n) throws IOException {
    if (buffer.remaining() > n) {
      buffer.position(buffer.position() + (int) n);
      return n;
    } else {
      long left = n - buffer.remaining();
      while (left > 0) {
        fill();
        // reached EOF, return bytes skipped so far
        if (buffer.remaining() == 0) break;
        // continue reading bytes, if available skipping full buffer if necessary
        int skipBytes = left < buffer.remaining() ? (int) left : buffer.remaining();
        buffer.position(buffer.position() + skipBytes);
        left -= skipBytes;
      }
      return n - left;
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
    service.shutdownNow();
  }
}
