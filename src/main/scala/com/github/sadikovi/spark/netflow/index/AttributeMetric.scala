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

import io.netty.buffer.{ByteBuf, Unpooled}

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

/** Count metric */
class CountMetric extends AttributeMetric[Long] {
  private var count: Long = 0

  override def addResolvedValue(value: Long): Unit = count += value

  override def nullable(): Boolean = false

  override def read(b: Array[Byte]): Unit = {
    val buffer = Unpooled.wrappedBuffer(b)
    count = buffer.readLong()
    buffer.release(buffer.refCnt)
  }

  override def write(): Array[Byte] = {
    val buffer = Unpooled.buffer(8)
    buffer.writeLong(count)
    buffer.array()
  }

  override def countQuery(): Option[Long] = Some(count)

  override def toString(): String = {
    s"CountMetric(count=$count)"
  }
}

/** Metric to store min and max values */
abstract class MinMaxMetric[T <: AnyVal](implicit tag: ClassTag[T]) extends AttributeMetric[T] {
  private var min: T = _
  private var max: T = _

  /** Comparison for left and right, returns true if left is smaller than right */
  protected def cmp(left: T, right: T): Boolean

  override def addResolvedValue(value: T): Unit = {
    if (min == null || cmp(value, min)) {
      min = value
    }

    if (max == null || cmp(max, value)) {
      max = value
    }
  }

  override def rangeQuery(value: T)(func: (T, T, T) => Boolean): Option[Boolean] = {
    // `func` should contain: (min, value, max) => Boolean
    if (min == null || max == null || value == null) Some(false) else Some(func(min, value, max))
  }

  override def nullable(): Boolean = false

  override def read(b: Array[Byte]): Unit = {
    val buffer = Unpooled.wrappedBuffer(b)
    min = StatisticsUtils.readValue(buffer, clazz).asInstanceOf[T]
    max = StatisticsUtils.readValue(buffer, clazz).asInstanceOf[T]
  }

  override def write(): Array[Byte] = {
    val buffer = Unpooled.buffer(32)
    StatisticsUtils.writeValue(buffer, min, clazz)
    StatisticsUtils.writeValue(buffer, max, clazz)
    buffer.array().slice(0, buffer.writerIndex)
  }

  override def toString(): String = {
    s"MinMaxMetric[$clazz](min=$min, max=$max)"
  }
}

/** Metric for byte min/max */
class ByteMinMaxMetric extends MinMaxMetric[Byte] {
  override protected def cmp(left: Byte, right: Byte) = left < right
}

/** Metric for short min/max */
class ShortMinMaxMetric extends MinMaxMetric[Short] {
  override protected def cmp(left: Short, right: Short) = left < right
}

/** Metric for int min/max */
class IntMinMaxMetric extends MinMaxMetric[Int] {
  override protected def cmp(left: Int, right: Int) = left < right
}

/** Metric for long min/max */
class LongMinMaxMetric extends MinMaxMetric[Long] {
  override protected def cmp(left: Long, right: Long) = left < right
}

/** HashSet-based metric */
abstract class SetMetric[T](implicit tag: ClassTag[T]) extends AttributeMetric[T] {
  private var set: JHashSet[T] = new JHashSet[T]()

  override def addResolvedValue(value: T): Unit = set.add(value)

  override def containsQuery(value: T): Option[Boolean] = Some(set.contains(value))

  override def nullable(): Boolean = true

  override def read(b: Array[Byte]): Unit = {
    val buffer = Unpooled.wrappedBuffer(b)
    hasNull = buffer.readBoolean()
    val size = (buffer.readInt() / 0.75).toInt
    set = new JHashSet[T](size)
    while (buffer.isReadable) {
      set.add(StatisticsUtils.readValue(buffer, clazz).asInstanceOf[T])
    }

    if (hasNull) {
      set.add(null.asInstanceOf[T])
    }

    buffer.release(buffer.refCnt)
  }

  override def write(): Array[Byte] = {
    // First byte is metadata for set, e.g. contains nulls, etc., each value is written except null.
    // Initial size of the buffer is set size, this is at least number of elements for byte set.
    val buffer = Unpooled.buffer(set.size)
    buffer.writeBoolean(containsNull)
    buffer.writeInt(set.size)
    val iter = set.iterator
    while (iter.hasNext) {
      val value = iter.next
      if (value != null) {
        StatisticsUtils.writeValue(buffer, value, clazz)
      }
    }
    buffer.array().slice(0, buffer.writerIndex)
  }
}

/** Byte set metric */
class ByteSetMetric extends SetMetric[Byte]

/** Short set metric */
class ShortSetMetric extends SetMetric[Short]

/** Int set metric */
class IntSetMetric extends SetMetric[Int]

/** Long set metric */
class LongSetMetric extends SetMetric[Long]
