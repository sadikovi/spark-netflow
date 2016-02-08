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

import com.github.sadikovi.netflowlib.statistics.{Statistics, StatisticsOption}
import com.github.sadikovi.spark.netflow.Summary

final class SummaryWritable(version: Short, filepath: String) extends Summary(version, filepath) {

  override def readonly(): Boolean = false

  /** Simple count increment */
  def incrementCount(): Unit = {
    count.incrementAndGet()
  }

  /** Update value for min/max of option */
  private def setMinMax(opt: StatisticsOption, value: Long): Unit = {
    if (opt.getMin() > value) {
      opt.setMin(value)
    } else if (opt.getMin() == lowerBoundForSize(opt.getSize())) {
      opt.setMin(value)
    }

    if (opt.getMax() < value) {
      opt.setMax(value)
    } else if (opt.getMax() == upperBoundForSize(opt.getSize())) {
      opt.setMax(value)
    }
  }

  /** Update value for index. Value is resolved as `Long` regardless the size of the field */
  def updateForIndex(index: Long, value: Any): Unit = {
    if (!exists(index)) {
      throw new IllegalArgumentException(s"No key ${index} exists")
    }

    val opt = options.get(index)

    value match {
      case a: Byte => setMinMax(opt, a.toLong)
      case b: Short => setMinMax(opt, b.toLong)
      case c: Int => setMinMax(opt, c.toLong)
      case d: Long => setMinMax(opt, d)
      case other =>
        throw new IllegalArgumentException(s"Value ${other} cannot be resolved for key ${index}")
    }
  }
}
