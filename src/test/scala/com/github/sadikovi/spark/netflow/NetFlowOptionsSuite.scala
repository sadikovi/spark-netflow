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

package com.github.sadikovi.spark.netflow

import com.github.sadikovi.netflowlib.Buffers.RecordBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class NetFlowOptionsSuite extends UnitTestSuite {
  test("NetFlowOptions - predicate pushdown is enabled when no option is provided") {
    val opts = new NetFlowOptions(Map.empty)
    opts.usePredicatePushdown should be (true)
  }

  test("NetFlowOptions - predicate pushdown is disabled when 'false' is provided") {
    val opts = new NetFlowOptions(Map("predicate-pushdown" -> "false"))
    opts.usePredicatePushdown should be (false)
  }

  test("NetFlowOptions - predicate pushdown is enabled when 'true' is provided") {
    val opts = new NetFlowOptions(Map("predicate-pushdown" -> "true"))
    opts.usePredicatePushdown should be (true)
  }

  test("NetFlowOptions - test buffer size") {
    // check that buffer size is default
    var opts = new NetFlowOptions(Map.empty)
    opts.bufferSize should be (RecordBuffer.BUFFER_LENGTH_2)

    // set buffer size to be 64Kb
    opts = new NetFlowOptions(Map("buffer" -> "64Kb"))
    opts.bufferSize should be (64 * 1024)
  }

  test("NetFlowOptions - invalid buffer size") {
    // buffer size >> Integer.MAX_VALUE
    var err = intercept[RuntimeException] {
      new NetFlowOptions(Map("buffer" -> "10Gb"))
    }
    assert(err.getMessage.contains("> maximum buffer size"))

    // negative buffer size
    intercept[NumberFormatException] {
      new NetFlowOptions(Map("buffer" -> "-1"))
    }

    // buffer size < min buffer size
    err = intercept[RuntimeException] {
      new NetFlowOptions(Map("buffer" -> "10"))
    }
    assert(err.getMessage.contains("< minimum buffer size"))

    // just for completeness, test on wrong buffer value
    intercept[NumberFormatException] {
      new NetFlowOptions(Map("buffer" -> "wrong"))
    }
  }

  test("NetFlowOptions - stringify is enabled by default") {
    val opts = new NetFlowOptions(Map.empty)
    opts.applyConversion should be (true)
  }

  test("NetFlowOptions - stringify is disabled, if false") {
    val opts = new NetFlowOptions(Map("stringify" -> "false"))
    opts.applyConversion should be (false)
  }

  test("NetFlowOptions - stringify is enabled, if true") {
    val opts = new NetFlowOptions(Map("stringify" -> "true"))
    opts.applyConversion should be (true)
  }

  test("NetFlowOptions - toString 1") {
    val opts = new NetFlowOptions(Map.empty)
    opts.toString should be ("NetFlowOptions(applyConversion=true, " +
      s"bufferSize=${RecordBuffer.BUFFER_LENGTH_2}, usePredicatePushdown=true)")
  }

  test("NetFlowOptions - toString 2") {
    val opts = new NetFlowOptions(Map("stringify" -> "false", "buffer" -> "32768",
      "predicate-pushdown" -> "false"))
    opts.toString should be ("NetFlowOptions(applyConversion=false, " +
      s"bufferSize=32768, usePredicatePushdown=false)")
  }
}
