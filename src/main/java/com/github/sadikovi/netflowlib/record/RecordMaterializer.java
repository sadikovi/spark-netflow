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

import com.github.sadikovi.netflowlib.util.WrappedByteBuf;

/**
 * [[RecordMaterializer]] interface provides all necessary methods to parse single record and return
 * either array of values that match list and order of pruned columns, or null, if record does not
 * pass predicate. Predicate check is optional and should depend on implementation.
 */
public interface RecordMaterializer {

  /**
   * Process record using wrapped byte buffer, array holds value for each column in projection.
   * Returned values should have the same type as corresponding columns.
   *
   * Can return null array.
   *
   * @param buffer wrapped byte buffer
   * @return list of values
   */
  Object[] processRecord(WrappedByteBuf buffer);
}
