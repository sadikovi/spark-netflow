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

import com.github.sadikovi.netflowlib.statistics.StatisticsOption
import com.github.sadikovi.spark.netflow.sources.{SummaryReadable, SummaryWritable}
import com.github.sadikovi.testutil.UnitTestSpec

class SummarySuite extends UnitTestSpec {
  // version of a NetFlow file
  val version: Short = 5

  test("set count") {
    val summary = new SummaryReadable(version, "")
    summary.setCount(100L)

    val stats = summary.finalizeStatistics()
    stats.getCount() should be (100L)
  }

  test("increment count") {
    val summary = new SummaryWritable(version, "")

    for (i <- 0 until 100) {
      summary.incrementCount()
    }

    val stats = summary.finalizeStatistics()
    stats.getVersion() should be (version)
    stats.getCount() should be (100L)
  }

  test("add options") {
    val summary = new SummaryWritable(version, "")
    summary.setOption(1L, new StatisticsOption(1L, 1, 0, 100))
    summary.setOption(2L, new StatisticsOption(2L, 2, 100, 200))
    summary.setOption(3L, new StatisticsOption(3L, 4, 200, 300))
    summary.setOption(4L, new StatisticsOption(4L, 8, 300, 400))

    val stats = summary.finalizeStatistics()
    stats.getOptions().length should be (4)

    intercept[IllegalArgumentException] {
      summary.setOption(1L, new StatisticsOption(5L, 2, 0, 100))
    }
  }

  test("exists option") {
    val summary = new SummaryWritable(version, "")
    summary.setOption(1L, new StatisticsOption(1L, 1, 0, 100))
    summary.exists(1L) should be (true)
    summary.exists(2L) should be (false)
  }

  test("update for index") {
    val summary = new SummaryWritable(version, "")
    summary.setOption(1L, new StatisticsOption(1L, 2, 0, 100))
    summary.setOption(2L, new StatisticsOption(2L, 8, Long.MinValue, Long.MaxValue))

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

  test("set options from source") {
    val summary = new SummaryReadable(version, "")

    summary.setOption(1L, new StatisticsOption(1L, 2, 1, 100))
    summary.setOption(2L, new StatisticsOption(2L, 2, 0, 0))

    summary.setOptionsFromSource(Array(new StatisticsOption(2L, 2, 2, 200),
      new StatisticsOption(3L, 2, 3, 300)))

    val stats = summary.finalizeStatistics()

    stats.getOptions().length should be (3)
    stats.getOptions().map(_.getMin()).sortWith(_ < _) should be (Array(Short.MinValue, 2, 3))
    stats.getOptions().map(_.getMax()).sortWith(_ < _) should be (Array(200, 300, Short.MaxValue))
  }
}
