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
 * [[FilterIterator]] is a wrapper on standard iterator interface, allows to filter out "null"
 * values. Instead of implementing `hasNext()` and `next()` methods, caller needs to implement
 * internal methods to provide original stream of values. When iterator is called, standard methods
 * will be called with filter already.
 */
public final class FilterIterator<E> implements Iterator<E> {
  public FilterIterator(Iterator<E> iterator) {
    this.iterator = iterator;
  }

  @Override
  public final boolean hasNext() {
    while (!found && iterator.hasNext()) {
      foundItem = iterator.next();
      if (foundItem != null) {
        found = true;
      }
    }
    return found;
  }

  @Override
  public final E next() {
    if (!found) {
      throw new NoSuchElementException("Either iterator is empty, or iterator state has not " +
        "been updated");
    }

    if (foundItem == null) {
      throw new IllegalStateException("Potential out of sync error in " +
        getClass().getSimpleName());
    }

    found = false;
    return foundItem;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Operation 'remove' is not supported");
  }

  private E foundItem = null;
  private boolean found = false;
  private final Iterator<E> iterator;
}
