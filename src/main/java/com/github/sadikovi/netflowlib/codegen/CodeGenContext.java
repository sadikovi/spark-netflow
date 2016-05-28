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

package com.github.sadikovi.netflowlib.codegen;

import java.util.HashMap;

/** [[CodeGenContext]] interface to provide utility methods for code generation */
public class CodeGenContext {
  // Global instance of `CodeGenContext`
  private static CodeGenContext instance = null;
  public static final String DEFAULT_ATTRIBUTE = "attr";

  /** Singleton method to reuse the same instance of context */
  public static CodeGenContext getOrCreate() {
    if (instance == null) {
      instance = new CodeGenContext();
    }

    return instance;
  }

  /** Reset current context, so next call to get context will return new instance */
  public static void reset() {
    instance = null;
  }

  private CodeGenContext() {
    nameState = new HashMap<String, Integer>();
    // Also register default name "attr"
    registerNameState(DEFAULT_ATTRIBUTE);
  }

  /** Whether or not character is supported */
  private boolean isValid(char ch) {
    // numbers range [48-57], uppercase chars [65-90], lowercase chars [97-122]
    return (ch >= 48 && ch <= 57) || (ch >= 65 && ch <= 90) || (ch >= 97 && ch <= 122);
  }

  /**
   * Make attribute name canonicalized, check if it is null, convert into camel-case and replace all
   * illegal characters. Note that we only keep ASCII characters and numbers (A-Za-z0-9), any other
   * characters will be removed. If pattern turns out to be empty after removal, default attribute
   * name is used.
   */
  public String canonicalize(String name) {
    if (name == null || name == "") {
      throw new IllegalArgumentException("Name " + name + " cannot be registered");
    }

    StringBuilder buffer = new StringBuilder();
    boolean useUpperCase = false;
    for (int i=0; i<name.length(); i++) {
      char curr = name.charAt(i);
      if (isValid(curr)) {
        if (useUpperCase && buffer.length() > 0) {
          curr = Character.toUpperCase(curr);
        }
        buffer.append(curr);
        useUpperCase = false;
      } else {
        useUpperCase = true;
      }
    }

    if (buffer.length() == 0) {
      return DEFAULT_ATTRIBUTE;
    } else {
      return buffer.toString();
    }
  }

  /**
   * Register new attribute name. If name already exists, returns `false`, otherwise registers
   * attribute with default state as 0.
   */
  public synchronized boolean registerNameState(String name) {
    String updatedName = canonicalize(name);
    if (nameState.containsKey(updatedName)) {
      return false;
    } else {
      nameState.put(updatedName, 0);
      return true;
    }
  }

  /** Get next name for attribute */
  public synchronized String getAttributeName(String name) {
    String updatedName = canonicalize(name);
    if (!nameState.containsKey(updatedName)) {
      throw new IllegalArgumentException(
        "Name state for " + name + " (" + updatedName + ") is undefined, register it first");
    }

    int counter = nameState.get(updatedName);
    String nextName = updatedName + "" + counter;
    nameState.put(updatedName, ++counter);
    return nextName;
  }

  /** Get name for default attribute */
  public String getAttributeName() {
    return getAttributeName(DEFAULT_ATTRIBUTE);
  }

  /**
   * Normalize Java value of any type supported, e.g. value of `(long) 10` is converted into `10L`.
   */
  public String normalizeJavaValue(Object value) {
    if (value instanceof Byte) {
      return "(byte) " + value;
    } else if (value instanceof Short) {
      return "(short) " + value;
    } else if (value instanceof Integer) {
      return "" + value;
    } else if (value instanceof Long) {
      return "" + value + "L";
    } else {
      throw new UnsupportedOperationException("Unsupported value " + value + " to normalize");
    }
  }

  // Internal name state registry
  private final HashMap<String, Integer> nameState;
}
