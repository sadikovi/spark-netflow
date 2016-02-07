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

import java.util.concurrent.ConcurrentHashMap

import com.github.sadikovi.netflowlib.statistics.{Statistics, StatisticsOption}

/**
 * `SummaryWritable` is a serializable and concurrent alternative to [[Statistics]] from netflowlib.
 * Stores options using map where index is either field name or field index. Use
 * `finalizeStatistics()` to get statistics for writes.
 */
case class SummaryWritable(private val version: Short, private var count: Long) {

  private val options = new ConcurrentHashMap[Long, StatisticsOption]()

  /** Update value for min / max of option */
  private def setMinMax(opt: StatisticsOption, value: Long): Unit = {
    if (opt.getMin() > value) {
      opt.setMin(value)
    }

    if (opt.getMax() < value) {
      opt.setMax(value)
    }
  }

  /** Simple count increment */
  def incrementCount(): Unit = {
    count += 1
  }

  /** Add option for a specific index */
  def add(index: Long, option: StatisticsOption): Unit = {
    if (exists(index)) {
      throw new IllegalArgumentException(s"Key ${index} already exists")
    }
    options.put(index, option)
  }

  /** Key exists in the map */
  def exists(index: Long): Boolean = {
    options.containsKey(index)
  }

  /** Update value for index. Value is resolved according size of the option's field */
  def updateForIndex(index: Long, value: Any): Unit = {
    if (!exists(index)) {
      throw new IllegalArgumentException(s"No key ${index} exists")
    }

    val opt = options.get(index)

    value match {
      case a: Byte if opt.getSize() == 1 => setMinMax(opt, a.toLong)
      case b: Short if opt.getSize() == 2 => setMinMax(opt, b.toLong)
      case c: Int if opt.getSize() == 4 => setMinMax(opt, c.toLong)
      case d: Long if opt.getSize() == 8 => setMinMax(opt, d)
      case other =>
        throw new IllegalArgumentException(s"Value ${other} cannot be resolved for key ${index}")
    }
  }

  /** Prepare Statistics for writing */
  def finalizeStatistics(): Statistics = {
    return new Statistics(version, count, options.values().toArray(
      new Array[StatisticsOption](options.size())))
  }
}
