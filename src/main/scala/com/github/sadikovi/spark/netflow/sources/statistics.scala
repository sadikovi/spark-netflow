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

import java.util.{HashSet => JHashSet}

import scala.reflect.ClassTag

import org.apache.hadoop.fs.{Path => HadoopPath}

/**
 * [[StatisticsPathResolver]] is a simple class to find the statistics path based on a file path.
 * Also takes into account possible root to store/read statistics. Note that root may not
 * necessarily exist.
 */
case class StatisticsPathResolver(maybeRoot: Option[String]) {
  if (maybeRoot.isDefined) {
    require(maybeRoot.get != null && maybeRoot.get.nonEmpty,
      s"Root path is expected to be non-empty, got $maybeRoot")
  }

  /**
   * Return statistics path based on root and file path.
   * If root is not specified statistics file is stored side-by-side with the original file,
   * otherwise, directory structure is replicated starting with root:
   * {{{
   *    val path = "/a/b/c/file"
   *    val root = "/x/y/z"
   *    // then statistics file will stored:
   *    val stats = "/x/y/z/a/b/c/.statistics-file"
   * }}}
   */
  def getStatisticsPath(filePath: String): String = {
    // Return updated path with suffix appended
    def withSuffix(path: HadoopPath, suffix: String): HadoopPath = {
      path.suffix(s"${HadoopPath.SEPARATOR}${suffix}")
    }

    val path = new HadoopPath(filePath)
    maybeRoot match {
      case Some(root) =>
        val rootPath = new HadoopPath(root)
        withSuffix(HadoopPath.mergePaths(rootPath, path).getParent(),
          getStatisticsName(path.getName)).toString
      case None =>
        withSuffix(path.getParent(), getStatisticsName(path.getName)).toString
    }
  }

  /** Return statistics name based on original file name */
  private def getStatisticsName(fileName: String): String = s".statistics-$fileName"
}

/**
 * [[Attribute]] interface to collect and check statistics. Included support of different
 * combinations of collected parameters: count, min/max, and set of values through bit vector. Here
 * are some common flags: 7 - enable all parameters, 1 - enable count, 6 - enable min/max and set.
 * In order to create attribute comparison function is required similar the `lt` function used in
 * `sortWith` method. Name must unique to the attribute.
 */
case class Attribute[T](
    name: String,
    lt: (T, T) => Boolean, flags: Byte)(implicit tag: ClassTag[T]) {
  private var count: Long = if (isCountEnabled) 0 else Long.MinValue
  private var min: T = _
  private var max: T = _
  private var set: JHashSet[T] = if (isSetEnabled) new JHashSet() else null

  /** Add value to the attribute, it automatically checks all available modes */
  def addValue(value: T): Unit = {
    if (isCountEnabled) {
      count += 1
    }

    if (isMinMaxEnabled) {
      if (min == null || !lt(min, value)) {
        min = value
      }

      if (max == null || lt(max, value)) {
        max = value
      }
    }

    if (isSetEnabled) {
      set.add(value)
    }
  }

  private def isEnabled(flag: Byte): Boolean = {
    (flags & flag) == flag
  }

  /** Check if count is collected by this attribute */
  def isCountEnabled(): Boolean = isEnabled(1)

  /** Check if min/max is collected by this attribute */
  def isMinMaxEnabled(): Boolean = isEnabled(2)

  /** Check if set is collected by this attribute */
  def isSetEnabled(): Boolean = isEnabled(4)

  /** Get count, if mode is enabled, otherwise None */
  def getCount(): Option[Long] = {
    if (isCountEnabled) Some(count) else None
  }

  /** Check if value is in min-max range, if mode is enabled, otherwise None */
  def containsInRange(value: T): Option[Boolean] = {
    if (isMinMaxEnabled) Some(!lt(value, min) && !lt(max, value)) else None
  }

  /** Check if value is in set, if mode is enabled, otherwise None */
  def containsInSet(value: T): Option[Boolean] = {
    if (isSetEnabled) Some(set.contains(value)) else None
  }

  /** Update count with value */
  private[sources] def setCount(value: Long): Unit = {
    require(isCountEnabled, s"Count mode is disabled, bit flags: $flags")
    count = value
  }

  /** Update min/max directly with values */
  private[sources] def setMinMax(minValue: T, maxValue: T): Unit = {
    require(isMinMaxEnabled, s"Min-max mode is disabled, bit flags: $flags")
    min = minValue
    max = maxValue
  }

  /** Update set directly with value */
  private[sources] def setSet(setValue: JHashSet[T]): Unit = {
    require(isSetEnabled, s"Set mode is disabled, bit flags: $flags")
    set = setValue
  }

  override def toString(): String = {
    s"${getClass().getCanonicalName}, name: $name, bit flags: $flags"
  }
}
