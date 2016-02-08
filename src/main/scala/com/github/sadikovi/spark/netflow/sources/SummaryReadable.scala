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

final class SummaryReadable(version: Short, filepath: String) extends Summary(version, filepath) {

  override def readonly(): Boolean = true

  /**
   * Update current options from source provided. It is safe to have more options specified in
   * [[SummaryReadable]] than in source, since we update values to be min/max boundaries. If source
   * contains more options, those options will be added with field keys.
   */
  private[spark] def setOptionsFromSource(source: Array[StatisticsOption]): Unit = {
    // update all options to the default values
    val iter = options.values().iterator()
    while (iter.hasNext) {
      val localOption = iter.next()
      localOption.setMin(lowerBoundForSize(localOption.getSize()))
      localOption.setMax(upperBoundForSize(localOption.getSize()))
    }

    // set options according to source
    for (sourceOption <- source) {
      val key = sourceOption.getField()

      if (options.containsKey(key)) {
        val localOption = options.get(key)
        localOption.setMin(sourceOption.getMin())
        localOption.setMax(sourceOption.getMax())
      } else {
        setOption(key, sourceOption)
      }
    }
  }
}
