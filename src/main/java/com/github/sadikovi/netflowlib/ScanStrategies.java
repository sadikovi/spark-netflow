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

import com.github.sadikovi.netflowlib.record.RecordMaterializer;

/**
 * All possible strategies to scan, e.g. skipping entire file, full scan of records, or different
 * filtering scans for a record. Every strategy must be instantiated with set of pruned columns
 * and/or set of filters to apply. Make sure that predicate is optimized on previous step.
 */
public final class ScanStrategies {
  private ScanStrategies() { }

  /**
   * [[ScanStrategy]] is a base class for all strategies, defines two basic methods.
   * `skip()` tells caller whether or not to skip scan entirely, `getRecordMaterializer()` returns
   * record materializer that skips record on row basis or reads record according to the selected
   * columns.
   */
  public static abstract class ScanStrategy implements Serializable {
    ScanStrategy() { }

    /** Whether or not to skip reading of the file */
    public abstract boolean skip();

    /** Get record materializer for the strategy */
    public abstract RecordMaterializer getRecordMaterializer();
  }
}
