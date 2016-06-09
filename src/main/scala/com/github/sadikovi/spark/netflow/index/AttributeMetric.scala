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

import scala.reflect.ClassTag

/**
 * Abstract attribute metric to keep track of a particular typed metric. Also provides methods to
 * load and save content. Note that column attribute can support many metric types.
 */
abstract class AttributeMetric[T](implicit tag: ClassTag[T]) {
  // Runtime class of generic type, needed to correctly compare unresolved value
  protected val clazz = tag.runtimeClass
  // Type has processed any null value
  protected var hasNull: Boolean = false

  /**
   * Whether or not type supports nullability. Note that in case metric does support nullability,
   * it is metric's responsibility to handle different cases involving null including loading and
   * saving data.
   */
  def nullable(): Boolean

  /** Add resolved value, which can still be null, if nullability is supported */
  def addResolvedValue(value: T): Unit

  /**
   * Read byte array and reconstruct attribute metric. Should contain full information on how to
   * resolve metadata and content.
   */
  def read(b: Array[Byte]): Unit

  /**
   * Write metadata and content of metric into byte array. Should be sufficient to reconstruct
   * fully type by loading byte array.
   */
  def write(): Array[Byte]

  /** Get count if metric supports it, `None` is no-op */
  def countQuery(): Option[Long] = None

  /** Check if metric contains / keeps track of the value, `None` is no-op */
  def containsQuery(value: T): Option[Boolean] = None

  /**
   * Check if value is in range provided by metric, first parameter is a value to check, second
   * parameter is a range query - (min, value, max) => Boolean, whether predicate passes or does
   * not pass query.
   */
  def rangeQuery(value: T)(func: (T, T, T) => Boolean): Option[Boolean] = None

  /**
   * Add unresolved value, checks and compares type with runtime class including Java classes.
   * If value is null and type is not nullable, typed method invocation is ignored.
   */
  def addValue(unresolvedValue: Any): Unit = {
    if (unresolvedValue != null) {
      require(StatisticsUtils.softCompare(unresolvedValue.getClass, clazz),
        s"Value '$unresolvedValue' does not match runtime class '$clazz'")
    } else {
      // Set it to true even if type does not support nullability, we just check two properties
      // together later
      hasNull = true
    }
    // We invoke this method for either nullable types or non-null values
    if (unresolvedValue != null || (nullable() && unresolvedValue == null)) {
      addResolvedValue(unresolvedValue.asInstanceOf[T])
    }
  }

  /** Metric contains null */
  def containsNull(): Boolean = nullable() && hasNull
}
