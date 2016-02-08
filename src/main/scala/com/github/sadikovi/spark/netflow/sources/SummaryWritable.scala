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
import java.util.concurrent.atomic.AtomicLong

import com.github.sadikovi.netflowlib.statistics.{Statistics, StatisticsOption}

/**
 * `SummaryWritable` is a serializable and concurrent alternative to [[Statistics]] from netflowlib.
 * Stores options using map where index is either field name or field index. Use
 * `finalizeStatistics()` to get statistics for writes.
 */
case class SummaryWritable(private val version: Short, cnt: Long) {

  private val options = new ConcurrentHashMap[Long, StatisticsOption]()
  private val count = new AtomicLong(cnt)

  /** Update value for min / max of option */
  private def setMinMax(opt: StatisticsOption, value: Long): Unit = {
    if (opt.getMin() == Long.MinValue || opt.getMin() > value) {
      opt.setMin(value)
    }

    if (opt.getMax() == Long.MaxValue || opt.getMax() < value) {
      opt.setMax(value)
    }
  }

  /** Simple count increment */
  def incrementCount(): Unit = {
    count.incrementAndGet()
  }

  /** Update current count with value provided */
  def updateCount(newValue: Long): Unit = {
    count.set(newValue)
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

  /**
   * Update current options from source provided. It is safe to have more options specified in
   * [[SummaryWritable]] than in source, since we update values to be min/max boundaries. If source
   * contains more options, those options will be discarded, as there is not key mapping exists.
   */
  private[spark] def setOptionsFromSource(source: Array[StatisticsOption]): Unit = {
    // build map from source
    val sourceMap = source.map { opt => (opt.getField(), opt)}.toMap
    val iter = options.values().iterator()
    while (iter.hasNext) {
      val localOption = iter.next()
      val fieldName = localOption.getField()

      if (sourceMap.contains(fieldName)) {
        val sourceOption = sourceMap.get(fieldName).get
        localOption.setMin(sourceOption.getMin())
        localOption.setMax(sourceOption.getMax())
      } else {
        // set lower and upper boundaries for those options
        localOption.getSize().toInt match {
          case 1 =>
            localOption.setMin(Byte.MinValue)
            localOption.setMin(Byte.MaxValue)
          case 2 =>
            localOption.setMin(Short.MinValue)
            localOption.setMin(Short.MaxValue)
          case 4 =>
            localOption.setMin(Int.MinValue)
            localOption.setMin(Int.MaxValue)
          case 8 =>
            localOption.setMin(Long.MinValue)
            localOption.setMin(Long.MaxValue)
          case _ => // do nothing, leave as default
        }
      }
    }
  }

  /** Prepare Statistics for writing */
  def finalizeStatistics(): Statistics = {
    return new Statistics(version, count.get(), options.values().toArray(
      new Array[StatisticsOption](options.size())))
  }
}
