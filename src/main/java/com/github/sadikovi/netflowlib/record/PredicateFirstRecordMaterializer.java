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

package com.github.sadikovi.netflowlib.record;

import io.netty.buffer.ByteBuf;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Columns.ByteColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;

public final class PredicateFirstRecordMaterializer extends RecordMaterializer {
  public PredicateFirstRecordMaterializer(
      Column[] columns,
      Column[] filtered,
      FilterPredicate tree) {
    // initialize record materializer
  }

  @Override
  public Object[] processRecord(ByteBuf buffer) {
    return null;
  }

  @Override
  public Column[] getColumns() {
    return null;
  }

  @Override
  public FilterPredicate getPredicateTree() {
    return null;
  }
}
