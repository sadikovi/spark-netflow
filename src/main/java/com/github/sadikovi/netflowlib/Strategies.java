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

import java.io.Serializable;

import com.github.sadikovi.netflowlib.record.RecordMaterializer;
import com.github.sadikovi.netflowlib.record.ScanRecordMaterializer;

/**
 * All possible strategies to scan, e.g. skipping entire file, full scan of records, or different
 * filtering scans for a record. Every strategy must be instantiated with set of pruned columns
 * and/or set of filters to apply. Make sure that predicate is optimized on previous step.
 */
public final class Strategies {
  private Strategies() { }

  public static abstract class ScanStrategy {

  }
}
