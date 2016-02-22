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

package com.github.sadikovi.netflowlib.predicate

import com.github.sadikovi.netflowlib.predicate.Columns.{ByteColumn, IntColumn, LongColumn}

import com.github.sadikovi.testutil.UnitTestSpec

class ColumnSuite extends UnitTestSpec {
  test("column creation and error check") {
    val sample: java.lang.Byte = 1.toByte
    val col = new ByteColumn(1.toByte, 10, -1, 1)

    col.getColumnName() should be (1L)
    col.getColumnType() should be (sample.getClass())
    col.getMinValue() should be (-1)
    col.getMaxValue() should be (1)
  }

  test("column should fail if offset is negative") {
    intercept[IllegalArgumentException] {
      new IntColumn(1, -1, -1, 1)
    }

    // but "0" offset is valid
    val col = new IntColumn(1, 0, -1, 1)
    col.getColumnOffset() should be (0)
  }

  test("column comparison") {
    new IntColumn(1, 0).equals(new IntColumn(1, 0)) should be (true)
    new IntColumn(1, 0).equals(new LongColumn(1, 0)) should be (false)
    new IntColumn(1, 0).equals(new IntColumn(2, 0)) should be (false)
    new IntColumn(1, 0).equals(new IntColumn(1, 1)) should be (false)
    new IntColumn(1, 0, 100, 101).equals(new IntColumn(1, 0, 98, 99)) should be (true)
  }
}
