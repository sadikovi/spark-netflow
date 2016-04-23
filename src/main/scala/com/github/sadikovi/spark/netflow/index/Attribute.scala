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

package com.github.sadikovi.spark.netflow.index

import java.util.{HashSet => JHashSet}

import scala.reflect.ClassTag

/**
 * [[Attribute]] interface to collect and check statistics. Included support of different
 * combinations of collected parameters: count, min/max, and set of values through bit vector. Here
 * are some common flags: 7 - enable all parameters, 1 - enable count, 6 - enable min/max and set.
 * In order to create attribute comparison function is required similar the `lt` function used in
 * `sortWith` method. Name must unique to the attribute.
 */
case class Attribute[T](
    name: String, lt: (T, T) => Boolean, flags: Byte)(implicit tag: ClassTag[T]) {
  private val klass = tag.runtimeClass
  private var count: Long = if (isCountEnabled) 0 else Long.MinValue
  private var min: T = _
  private var max: T = _
  private var set: JHashSet[T] = if (isSetEnabled) new JHashSet() else null
  private var hasNull: Boolean = false

  require(name != null && name.nonEmpty, "Attribute name is empty")

  private def isEnabled(flag: Byte): Boolean = {
    (flags & flag) == flag
  }

  /** Check if count is collected by this attribute */
  def isCountEnabled(): Boolean = isEnabled(1)

  /** Check if min/max is collected by this attribute */
  def isMinMaxEnabled(): Boolean = isEnabled(2)

  /** Check if set is collected by this attribute */
  def isSetEnabled(): Boolean = isEnabled(4)

  /** Add value to the attribute, it automatically checks all available modes */
  def addValue(value: T): Unit = {
    if (!hasNull && value == null) {
      hasNull = true
    }

    if (isCountEnabled) {
      count += 1
    }

    // Min/max statistics are kept only for non-null values
    if (isMinMaxEnabled && value != null) {
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

  /** Get count, if mode is enabled, otherwise None */
  def getCount(): Option[Long] = {
    if (isCountEnabled) Some(count) else None
  }

  /** Get min/max, internal operation to write min/max */
  def getMinMax(): Option[(T, T)] = {
    if (isMinMaxEnabled) Some((min, max)) else None
  }

  /** Get set, internal operation to write set */
  def getSet(): Option[JHashSet[T]] = {
    if (isSetEnabled) Some(set) else None
  }

  /** Check if value is in min-max range, if mode is enabled, otherwise None */
  def containsInRange(value: T): Option[Boolean] = {
    if (isMinMaxEnabled) {
      // null value is always out of range, false is returned
      if (value != null) Some(!lt(value, min) && !lt(max, value)) else Some(false)
    } else {
      None
    }
  }

  /** Check if value is in set, if mode is enabled, otherwise None */
  def containsInSet(value: T): Option[Boolean] = {
    if (isSetEnabled) Some(set.contains(value)) else None
  }

  /** Update count with value */
  private[index] def setCount(value: Long): Unit = {
    require(isCountEnabled, s"Count mode is disabled, bit flags: $flags")
    count = value
  }

  /** Update min/max directly with values, lazily update `hasNull` */
  private[index] def setMinMax(minValue: T, maxValue: T): Unit = {
    require(isMinMaxEnabled, s"Min-max mode is disabled, bit flags: $flags")
    hasNull = hasNull || minValue == null || maxValue == null
    min = minValue
    max = maxValue
  }

  /** Update set directly with value, lazily update `hasNull` */
  private[index] def setSet(setValue: JHashSet[T]): Unit = {
    require(isSetEnabled, s"Set mode is disabled, bit flags: $flags")
    hasNull = hasNull || setValue == null || setValue.contains(null)
    set = setValue
  }

  /** Get actual generic runtime class */
  def getClassTag(): Class[_] = klass

  /**
   * Whether or not attribute has null values.
   * It checks initial state of min, max and set, and tracks values being added, thus overall
   * nullability is cumulative effect of states.
   */
  def containsNull(): Boolean = hasNull || (isSetEnabled && (set == null || set.contains(null))) ||
    (isMinMaxEnabled && (min == null || max == null))

  override def toString(): String = {
    s"${getClass().getCanonicalName}[name: $name, bit flags: $flags, tag: $tag]"
  }
}
