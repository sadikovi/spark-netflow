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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;

import com.github.sadikovi.netflowlib.util.FilterIterator;
import com.github.sadikovi.netflowlib.util.SafeIterator;
import com.github.sadikovi.netflowlib.util.WrappedByteBuf;

public class UtilSuite {
  @Test(expected = NoSuchElementException.class)
  public void testFilterIteratorFailIfEmpty() {
    List<String> values = new ArrayList<String>();
    FilterIterator<String> iter = new FilterIterator<String>(values.iterator());
    iter.next();
  }

  @Test
  public void testFilterIteratorReturnAll() {
    List<String> values = new ArrayList<String>();
    values.add("a");
    values.add("a");
    values.add("a");
    FilterIterator<String> iter = new FilterIterator<String>(values.iterator());

    int count = 0;
    while (iter.hasNext()) {
      assertSame(iter.next(), "a");
      count++;
    }

    assertEquals(count, values.size());
  }

  @Test
  public void testFilterIteratorReturnSome() {
    List<String> values = new ArrayList<String>();
    values.add("a");
    values.add(null);
    values.add("a");
    FilterIterator<String> iter = new FilterIterator<String>(values.iterator());

    int count = 0;
    while (iter.hasNext()) {
      assertSame(iter.next(), "a");
      count++;
    }

    assertEquals(count, 2);
  }

  @Test
  public void testFilterIteratorReturnNone() {
    List<String> values = new ArrayList<String>();
    values.add(null);
    values.add(null);
    values.add(null);
    FilterIterator<String> iter = new FilterIterator<String>(values.iterator());

    int count = 0;
    while (iter.hasNext()) {
      count++;
    }

    assertEquals(count, 0);
  }

  @Test(expected = NoSuchElementException.class)
  public void testSafeIteratorFailIfEmpty() {
    List<String> values = new ArrayList<String>();
    SafeIterator<String> iter = new SafeIterator<String>(values.iterator());
    iter.next();
  }

  @Test
  public void testSafeIteratorReturnAll() {
    List<String> values = new ArrayList<String>();
    values.add("a");
    values.add("a");
    values.add("a");
    SafeIterator<String> iter = new SafeIterator<String>(values.iterator());

    int count = 0;
    while (iter.hasNext()) {
      assertSame(iter.next(), "a");
      count++;
    }

    assertEquals(count, values.size());
  }

  @Test
  public void testSafeIteratorTerminateOnErrorInHasNext() {
    final List<String> values = new ArrayList<String>();
    values.add("a");
    values.add(null);
    values.add("a");
    Iterator<String> delegate = new Iterator<String>() {
      private Iterator<String> parent = values.iterator();
      private String current;

      @Override
      public boolean hasNext() {
        current = parent.next();
        if (current == null) {
          throw new IllegalStateException("Test");
        }
        return parent.hasNext();
      }

      @Override
      public String next() {
        return current;
      }

      @Override
      public void remove() { }
    };
    SafeIterator<String> iter = new SafeIterator<String>(delegate);

    int count = 0;
    while (iter.hasNext()) {
      assertSame(iter.next(), "a");
      count++;
    }

    // Expect one record only, since second record fails with state exception
    assertEquals(count, 1);
  }

  @Test
  public void testSafeIteratorTerminateOnErrorInNext() {
    final List<String> values = new ArrayList<String>();
    values.add("a");
    values.add(null);
    values.add("a");
    Iterator<String> delegate = new Iterator<String>() {
      private Iterator<String> parent = values.iterator();

      @Override
      public boolean hasNext() {
        return parent.hasNext();
      }

      @Override
      public String next() {
        return parent.next().toString();
      }

      @Override
      public void remove() { }
    };
    SafeIterator<String> iter = new SafeIterator<String>(delegate);

    int count = 0;
    while (iter.hasNext()) {
      assertSame(iter.next(), "a");
      count++;
    }

    // Expect one record only, since second record fails with null pointer exception
    assertEquals(count, 1);
  }

  @Test
  public void testWrappedByteBufGetters() {
    // test wrapped byte buf functionality
    byte[] bytes = new byte[1024];
    Random rand = new Random();
    // check big endian
    for (int i = 0; i <= bytes.length - 4; i++) {
      rand.nextBytes(bytes);
      WrappedByteBuf buf = WrappedByteBuf.init(bytes, ByteOrder.BIG_ENDIAN);
      ByteBuffer javaBuf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

      assertSame(buf.array(), bytes);
      assertEquals(buf.getByte(i), javaBuf.get(i));
      assertEquals(buf.getUnsignedByte(i), (short) (javaBuf.get(i) & 0xff));
      assertEquals(buf.getShort(i), javaBuf.getShort(i));
      assertEquals(buf.getUnsignedShort(i), javaBuf.getShort(i) & 0xffff);
      assertEquals(buf.getInt(i), javaBuf.getInt(i));
      assertEquals(buf.getUnsignedInt(i), javaBuf.getInt(i) & 0xffffffffL);
    }

    // check little endian
    for (int i = 0; i <= bytes.length - 4; i++) {
      rand.nextBytes(bytes);
      WrappedByteBuf buf = WrappedByteBuf.init(bytes, ByteOrder.LITTLE_ENDIAN);
      ByteBuffer javaBuf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

      assertSame(buf.array(), bytes);
      assertEquals(buf.getByte(i), javaBuf.get(i));
      assertEquals(buf.getUnsignedByte(i), (short) (javaBuf.get(i) & 0xff));
      assertEquals(buf.getShort(i), javaBuf.getShort(i));
      assertEquals(buf.getUnsignedShort(i), javaBuf.getShort(i) & 0xffff);
      assertEquals(buf.getInt(i), javaBuf.getInt(i));
      assertEquals(buf.getUnsignedInt(i), javaBuf.getInt(i) & 0xffffffffL);
    }
  }
}
