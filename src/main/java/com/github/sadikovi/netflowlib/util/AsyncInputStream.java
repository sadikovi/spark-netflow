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

public class AsyncInputStream extends InputStream {
  // Create single thread executor service
  private ExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition swapFinished = lock.newCondition();

  public AsyncInputStream(InputStream in, int size) throws IOException {
    this.in = in;
    // for `read()` calls
    oneByte = new byte[1];
    // active buffer for reads
    active = new byte[size];
    async = new byte[size];
    asyncError = null;
    readAsyncBytes = in.read(active, 0, active.length);
    buffer = ByteBuffer.wrap(active);
    buffer.limit(readAsyncBytes);
    readAsync();
  }

  private void fill() {
    if (asyncError != null) throw asyncError;
    if (buffer.remaining() > 0) return;
    swapBuffers();
    readAsync();
  }

  private void swapBuffers() {
    lock.lock();
    try {
      byte[] tmp = active;
      active = async;
      async = tmp;
      buffer.clear();
      if (readAsyncBytes < 0) {
        // indicates EOF
        readAsyncBytes = 0;
      }
      buffer.limit(readAsyncBytes);
    } finally {
      swapFinished.signal();
      lock.unlock();
    }
  }

  private void readAsync() {
    service.execute(new Runnable() {
      @Override
      public void run() {
        lock.lock();
        try {
          swapFinished.await();
          readAsyncBytes = in.read(async, 0, async.length);
        } catch (Exception err) {
          asyncError = new RuntimeException(err);
        } finally {
          lock.unlock();
        }
      }
    });
  }

  @Override
  public int read() throws IOException {
    if (read(oneByte, 0, 1) > 0) return oneByte[0] & 0xff;
    return -1;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (buffer.remaining() == 0) return -1;
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
      fill();
      return n;
    } else {
      long left = n - buffer.remaining();
      buffer.position(buffer.limit());
      fill();
      while (left > 0 && buffer.remaining() > 0) {
        int skipBytes = left < buffer.remaining() ? (int) left : buffer.remaining();
        buffer.position(buffer.position() + skipBytes);
        fill();
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

  private byte[] active;
  private byte[] async;
  private int readAsyncBytes;
  private RuntimeException asyncError;
  private ByteBuffer buffer;
  private final byte[] oneByte;
  private final InputStream in;
}
