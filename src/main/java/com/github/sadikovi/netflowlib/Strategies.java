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

import java.util.ArrayList;
import java.util.HashMap;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Inspectors.Inspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
import com.github.sadikovi.netflowlib.record.RecordMaterializer;
import com.github.sadikovi.netflowlib.record.ScanRecordMaterializer;
import com.github.sadikovi.netflowlib.record.PredicateRecordMaterializer;

/**
 * All possible strategies to scan, e.g. skipping entire file, full scan of records, or different
 * filtering scans for a record. Every strategy must be instantiated with set of pruned columns
 * and/or set of filters to apply. Make sure that predicate is optimized on previous step.
 */
public final class Strategies {
  private Strategies() { }

  public static abstract class ScanStrategy {
    public abstract boolean skipScan();

    public abstract boolean fullScan();

    public abstract RecordMaterializer getRecordMaterializer();

    @Override
    public String toString() {
      return "Strategy [" + getClass().getSimpleName() + "]";
    }
  }

  //////////////////////////////////////////////////////////////
  // Strategies
  //////////////////////////////////////////////////////////////

  /** Skip scan fully without reading file */
  public static final class SkipScan extends ScanStrategy {
    public SkipScan() { }

    @Override
    public boolean skipScan() {
      return true;
    }

    @Override
    public boolean fullScan() {
      return false;
    }

    @Override
    public RecordMaterializer getRecordMaterializer() {
      throw new UnsupportedOperationException("RecordMaterializer is not supported for " +
        getClass().getSimpleName());
    }
  }

  /** Full scan of the file without any predicate */
  public static final class FullScan extends ScanStrategy {
    public FullScan(Column[] columns) {
      rm = new ScanRecordMaterializer(columns);
    }

    @Override
    public boolean skipScan() {
      return false;
    }

    @Override
    public boolean fullScan() {
      return true;
    }

    @Override
    public RecordMaterializer getRecordMaterializer() {
      return rm;
    }

    private final RecordMaterializer rm;
  }

  /** Scan with filtering (fairly expensive, so we try resolving either to skip or full scan) */
  public static class FilterScan extends ScanStrategy {
    public FilterScan(
        Column[] columns,
        Inspector tree,
        HashMap<Column, ArrayList<ValueInspector>> inspectors) {
      rm = new PredicateRecordMaterializer(columns, tree, inspectors);
    }

    @Override
    public boolean skipScan() {
      return false;
    }

    @Override
    public boolean fullScan() {
      return false;
    }

    @Override
    public RecordMaterializer getRecordMaterializer() {
      return rm;
    }

    private final RecordMaterializer rm;
  }
}
