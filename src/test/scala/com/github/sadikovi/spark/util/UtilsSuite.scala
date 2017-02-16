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

import com.github.sadikovi.testutil.UnitTestSuite
import com.github.sadikovi.testutil.implicits._

class UtilsSuite extends UnitTestSuite {

  test("parse buffer size") {
    val sizes = Seq("1024", "10Kb", "10K", "10Mb", "10M", "10Gb", "10G")
    val expected = Seq(1024L, 10240L, 10240L, 10485760L, 10485760L, 10737418240L, 10737418240L)
    val result = sizes.map(str => Utils.byteStringAsBytes(str))
    result should be (expected)
  }

  test("fail to parse correctly") {
    val sizes = Seq("Kb", "corrupt", ".0", "-1Kb", "-1")
    sizes.foreach(str => {
      intercept[NumberFormatException] {
        Utils.byteStringAsBytes(str)
      }
    })

    intercept[NullPointerException] {
      Utils.byteStringAsBytes(null)
    }
  }

  test("create UUID for string") {
    val str = baseDirectory() / "_metadata"
    val uuids = for (i <- 0 until 10) yield Utils.uuidForString(str)
    // check that uuids are the same
    uuids.distinct.length should be (1)
    uuids.distinct.head should be (uuids.head)
  }

  test("truncate mean - empty array") {
    intercept[IllegalArgumentException] {
      Utils.truncatedMean(Array.empty, 0.5)
    }
  }

  test("truncate mean - negative sample") {
    intercept[IllegalArgumentException] {
      Utils.truncatedMean(Array(1L, 2L), -0.5)
    }
  }

  test("truncate mean - simple array") {
    Utils.truncatedMean(Array(1L, 2L, 3L, 4L, 5L), 1.0) should be (3)
    Utils.truncatedMean(Array(1L, 2L, 3L, 4L, 5L), 0.8) should be (3)
    Utils.truncatedMean(Array(1L, 2L, 3L, 4L, 5L), 0.6) should be (2)
  }

  test("histogram mode - empty array") {
    intercept[IllegalArgumentException] {
      Utils.histogramMode(Array.empty, None)
    }
  }

  test("histogram mode - single element") {
    val mean = Utils.histogramMode(Array(1L), None)
    mean should be (1L)
  }

  test("histogram mode - simple array") {
    val arr = Array(1L, 1L, 1L, 1L, 2L, 2L, 3L, 4L, 5L)
    val mean = Utils.histogramMode(arr, None)
    mean should be (2L)
  }

  test("histogram mode - extreme array") {
    val arr = Array(1L, 2L)
    val mean = Utils.histogramMode(arr, None)
    mean should be (1L)
  }
}
