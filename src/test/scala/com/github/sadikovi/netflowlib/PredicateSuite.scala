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

package com.github.sadikovi.netflowlib

import com.github.sadikovi.netflowlib.predicate.{FilterApi => Fa}
import com.github.sadikovi.netflowlib.predicate.Columns.{Column, ByteColumn, IntColumn, ShortColumn}
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate
import com.github.sadikovi.testutil.UnitTestSpec

class PredicateSuite extends UnitTestSpec {
  test("optimize predicate in scan") {
    val cols: Array[Column[_]] = Array(new IntColumn(1.toByte, 0), new IntColumn(2, 4),
      new IntColumn(3, 8))
    var tree: FilterPredicate = null
    val scan = new ScanState(cols, null)

    val value1: java.lang.Integer = 1

    tree = Fa.and(Fa.eq(new IntColumn(1, 0), value1), Fa.trivial(false))
    tree.update(scan) should be (Fa.trivial(false))

    tree = Fa.or(Fa.eq(new IntColumn(1, 0), value1), Fa.trivial(false))
    tree.update(scan) should be (Fa.eq(new IntColumn(1, 0), value1))
  }
}
