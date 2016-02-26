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

package com.github.sadikovi.netflowlib.predicate;

import java.io.Serializable;

/**
 * [[ValueInspector]] interface to update value while processing record of NetFlow data. Has
 * convinient methods to reset and update values. Uses parameterized type similar to [[Column]],
 * does not strictly depend on that interface.
 * @value value for `ValueInspector`
 */
public final class ValueInspector<T extends Comparable<T>> implements Serializable {
  /** Create `ValueInspector` with some default value */
  public ValueInspector(T newValue) {
    update(newValue, true);
  }

  /** Create `ValueInspector` with default "null" value */
  public ValueInspector() {
    value = null;
  }

  /** Check whether value is reset or not */
  private final boolean isReset() {
    return value == null;
  }

  /** Update value, `strict` allows to fail, if value was not reset properly, before update */
  public final void update(T newValue, boolean strict) {
    if (newValue == null) {
      throw new IllegalArgumentException("Updating value is null");
    }

    if (!isReset() && strict) {
      throw new IllegalStateException("Value inspector is not reset before update");
    }

    value = newValue;
  }

  public final void reset() {
    value = null;
  }

  public T getCurrentValue() {
    if (!isReset()) {
      throw new IllegalStateException(
        "Cannot resolve value. Are you trying to get value after reset?");
    }

    return value;
  }

  private T value;
}
