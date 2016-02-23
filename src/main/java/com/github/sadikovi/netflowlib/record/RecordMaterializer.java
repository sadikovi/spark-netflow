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
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;

/**
 * [[RecordMaterializer]] interface provides all necessary methods to parse single record and return
 * either array of values that match list and order of pruned columns, or null, if record does not
 * pass predicate. Predicate check is optional and should depend on implementation.
 * [[RecordMaterializer]] operates with [[Column]], so it is responsibility of subclasses to
 * implement filtering by flag (pruned, filtered, etc).
 */
public abstract class RecordMaterializer {
  RecordMaterializer() { }

  /** Process single record and return either filled array or null, if predicate returns false */
  public abstract Object[] processRecord(ByteBuf buffer);

  /** Return boxed columns for this RecordMaterializer */
  public abstract Column[] getColumns();

  /** Return filter predicate for this RecordMaterializer */
  public abstract FilterPredicate getPredicateTree();
}
