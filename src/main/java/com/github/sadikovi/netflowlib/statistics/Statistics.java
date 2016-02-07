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

package com.github.sadikovi.netflowlib.statistics;

import java.io.Serializable;

/**
 * Simple interface to hold multiple results for statistics, e.g. version, count, and options.
 * Also used as unified interface between writer and reader.
 */
public class Statistics implements Serializable {

  /** Recommended constructor */
  public Statistics(short version, long count, StatisticsOption[] options) {
    if (version < 0) {
      throw new IllegalArgumentException("Version is invalid: " + version);
    }

    if (count < 0) {
      throw new IllegalArgumentException("Count is invalid: " + count);
    }

    this.version = version;
    this.count = count;
    this.options = options;
  }

  /** For testing purposes only */
  protected Statistics(short version, long count) {
    this(version, count, new StatisticsOption[0]);
  }

  /** For testing purposes only */
  protected Statistics(short version) {
    this(version, 0);
  }

  /** Increment count by delta */
  public void increaseCount(long delta) {
    count += delta;
  }

  /** Set count */
  public void setCount(long cnt) {
    count = cnt;
  }

  /** Set options */
  public void setOptions(StatisticsOption[] opts) {
    options = opts;
  }

  /** Get current version */
  public short getVersion() {
    return version;
  }

  /** Get current count */
  public long getCount() {
    return count;
  }

  /** Get statistics options */
  public StatisticsOption[] getOptions() {
    return options;
  }

  private short version = 0;
  private long count = 0;
  private StatisticsOption[] options = null;
}
