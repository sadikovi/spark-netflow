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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator implementation that terminates delegate iterator when exception occurs during value
 * extraction.
 */
public class SafeIterator<E> implements Iterator<E> {
  private boolean gotNext = false;
  private E nextValue = null;
  protected boolean finished = false;
  private Iterator<E> delegate;

  public SafeIterator(Iterator<E> delegate) {
    this.delegate = delegate;
  }

  /**
   * If no next element is available, `finished` is set to `true` and may return any value
   * (it will be ignored). This convention is required because `null` may be a valid value.
   * @return E instance, or set 'finished' to `true` when done
   */
  private E getNext() {
    try {
      if (delegate.hasNext()) {
        return delegate.next();
      } else {
        finished = true;
        return null;
      }
    } catch (Exception err) {
      finished = true;
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext();
        gotNext = true;
      }
    }
    return !finished;
  }

  @Override
  public E next() {
    if (!hasNext()) {
      throw new NoSuchElementException("End of stream");
    }
    gotNext = false;
    return nextValue;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Operation 'remove' is not supported");
  }
}
