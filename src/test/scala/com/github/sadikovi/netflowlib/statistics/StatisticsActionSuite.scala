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

package com.github.sadikovi.netflowlib.statistics

import java.nio.ByteOrder

import com.github.sadikovi.testutil.UnitTestSpec

class StatisticsActionSuite extends UnitTestSpec {
  // test for parent methods for byte order conversion
  // we do not implement those tests for `StatisticsReader`
  test("byte order conversion") {
    // byte order => order number
    StatisticsAction.fromByteOrder(ByteOrder.LITTLE_ENDIAN) should be (
      StatisticsAction.BYTE_LITTLE_ENDIAN)

    StatisticsAction.fromByteOrder(ByteOrder.BIG_ENDIAN) should be (
      StatisticsAction.BYTE_BIG_ENDIAN)

    intercept[IllegalArgumentException] {
      StatisticsAction.fromByteOrder(null)
    }

    // byte order <= order number
    StatisticsAction.toByteOrder(StatisticsAction.BYTE_LITTLE_ENDIAN) should be (
      ByteOrder.LITTLE_ENDIAN)

    StatisticsAction.toByteOrder(StatisticsAction.BYTE_BIG_ENDIAN) should be (
      ByteOrder.BIG_ENDIAN)

    intercept[IllegalArgumentException] {
      StatisticsAction.toByteOrder(-1)
    }

    intercept[IllegalArgumentException] {
      StatisticsAction.toByteOrder(0)
    }

    intercept[IllegalArgumentException] {
      StatisticsAction.toByteOrder(3)
    }
  }
}
