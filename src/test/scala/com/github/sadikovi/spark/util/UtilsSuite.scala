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

package com.github.sadikovi.spark.util

import com.github.sadikovi.testutil.UnitTestSpec

class UtilsSuite extends UnitTestSpec {

  test("parse buffer size") {
    val sizes = Seq("1024", "10Kb", "10K", "10Mb", "10M", "10Gb", "10G")
    val expected = Seq(1024L, 10240L, 10240L, 10485760L, 10485760L, 10737418240L, 10737418240L)
    val result = sizes.map(str => Utils.byteStringAsBytes(str))
    result should be (expected)
  }

  test("fail to parse correctly") {
    val sizes = Seq("Kb", "corrupt", ".0")
    sizes.foreach(str => {
      intercept[NumberFormatException] {
        Utils.byteStringAsBytes(str)
      }
    })

    intercept[NullPointerException] {
      Utils.byteStringAsBytes(null)
    }
  }
}
