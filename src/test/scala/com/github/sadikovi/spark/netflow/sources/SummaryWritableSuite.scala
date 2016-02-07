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

package com.github.sadikovi.spark.netflow.sources

import com.github.sadikovi.netflowlib.statistics.StatisticsOption
import com.github.sadikovi.testutil.UnitTestSpec

class SummaryWritableSuite extends UnitTestSpec {
  // version of a NetFlow file
  val version: Short = 5

  test("increment count") {
    val initCount = 0
    val summary = SummaryWritable(version, initCount)

    for (i <- 0 until 100) {
      summary.incrementCount()
    }

    val stats = summary.finalizeStatistics()
    stats.getVersion() should be (version)
    stats.getCount() should be (100)
  }

  test("add options") {
    val summary = SummaryWritable(version, 0)
    summary.add(1L, new StatisticsOption(1L, 1, 0, 100))
    summary.add(2L, new StatisticsOption(2L, 2, 100, 200))
    summary.add(3L, new StatisticsOption(3L, 4, 200, 300))
    summary.add(4L, new StatisticsOption(4L, 8, 300, 400))

    val stats = summary.finalizeStatistics()
    stats.getOptions().length should be (4)

    intercept[IllegalArgumentException] {
      summary.add(1L, new StatisticsOption(5L, 2, 0, 100))
    }
  }

  test("exists option") {
    val summary = SummaryWritable(version, 0)
    summary.add(1L, new StatisticsOption(1L, 1, 0, 100))
    summary.exists(1L) should be (true)
    summary.exists(2L) should be (false)
  }

  test("update for index") {
    val summary = SummaryWritable(version, 0)
    summary.add(1L, new StatisticsOption(1L, 2, 0, 100))
    summary.add(2L, new StatisticsOption(2L, 4, Long.MinValue, Long.MaxValue))

    // update non-existent index
    intercept[IllegalArgumentException] {
      summary.updateForIndex(3L, -1)
    }

    // update non-numeric field
    intercept[IllegalArgumentException] {
      summary.updateForIndex(1L, "wrong-value")
    }

    summary.updateForIndex(1L, 200.toShort)
    summary.updateForIndex(2L, 200)

    val stats = summary.finalizeStatistics()

    val option1 = stats.getOptions().find(_.getField() == 1L).get
    option1.getMin() should be (0)
    option1.getMax() should be (200)

    val option2 = stats.getOptions().find(_.getField() == 2L).get
    option2.getMin() should be (200)
    option2.getMax() should be (200)
  }
}
