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

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.sql.types.{DataType, StructType}

import com.github.sadikovi.netflowlib.statistics.{Statistics, StatisticsOption}

/**
 * Schema resolver for Netflow versions. Also provides mapping for a particular column name.
 */
private[spark] object SchemaResolver {
  /** Get specific mapper for Netflow version */
  def getMapperForVersion(version: Short): Mapper = version match {
    case version if version == MapperV5.version() => MapperV5
    case other => throw new UnsupportedOperationException(
      s"Netflow version ${other} is not supported")
  }

  /**
   * Validate version of Netflow statically.
   * Will check actual supported version when creating mapper.
   */
  def validateVersion(possibleVersion: String): Option[Short] = {
    Try(possibleVersion.toShort).toOption
  }
}

/** Internal representation of columns */
private[spark] case class SchemaField(name: String, index: Long, dtype: DataType)

/**
 * Internal mapper for a Netflow version. Maps java columns to Scala column names.
 */
private[spark] trait Mapper {
  /** Mapper version */
  def version(): Short

  /**
   * Get full schema for a version.
   * Option "stringify" might change schema if field is convertable.
   */
  def getFullSchema(stringify: Boolean): StructType

  /** Get all internal fields for the version. */
  def getInternalColumns(): Array[Long]

  /** Get first internal column for quick scanning. */
  def getFirstInternalColumn(): Array[Long]

  /** Get internal column for Spark SQL column name */
  def getInternalColumnForName(name: String): Long

  /** Get map of conversion functions for a set of fields. Map is built using fields' indices */
  def getConversionsForFields(fields: Array[Long]): Map[Int, Any => String] = {
      throw new UnsupportedOperationException
  }

  def getSummaryReadable(filepath: String): Option[Summary] = None

  def getSummaryWritable(filepath: String, fields: Array[Long]): Option[Summary] = None

  /** Get list of columns that statistics can be collected for */
  def getStatisticsColumns(): Array[Long] = getFirstInternalColumn()
}

/**
 * Conversion functions for fields, e.g. number to IP address.
 */
object ConversionFunctions extends Serializable {
  /** Convert number to IP address */
  def numToIp(value: Any): String = value match {
    case num: Long =>
      require(num >= 0 && num < (2L << 31), s"Invalid number to convert: ${num}")
      val buf = new StringBuilder()
      var i = 24
      var ip = num
      while (i >= 0) {
          val a = ip >> i
          ip = a << i ^ ip
          buf.append(a)
          if (i > 0) {
              buf.append(".")
          }
          i = i - 8
      }
      buf.toString()
    case _ => value.toString()
  }
}

/**
 * Base class for [[SummaryWritable]] and [[SummaryReadable]]. Instead of `Path` object we store
 * string representation of it.
 */
private[spark] abstract class Summary(
    protected val version: Short,
    protected val filepath: String) extends Serializable {
  // initialize count to "0", there is a method to set it to arbitrary starting number
  protected val count = new AtomicLong(0L)
  protected val options = new ConcurrentHashMap[Long, StatisticsOption]()

  /** Lower bound for the field of size "size" */
  protected def lowerBoundForSize(size: Int): Long = size match {
    case 1 => Byte.MinValue
    case 2 => Short.MinValue
    case 4 => Int.MinValue
    case 8 => Long.MinValue
    case _ => throw new UnsupportedOperationException(s"Unsupported field size ${size}")
  }

  /** Upper bound for the field of size "size" */
  protected def upperBoundForSize(size: Int): Long = size match {
    case 1 => Byte.MaxValue
    case 2 => Short.MaxValue
    case 4 => Int.MaxValue
    case 8 => Long.MaxValue
    case _ => throw new UnsupportedOperationException(s"Unsupported field size ${size}")
  }

  /** Whether or not summary is read only */
  def readonly(): Boolean

  /** Update current count with value provided */
  def setCount(newValue: Long): Unit = {
    count.set(newValue)
  }

  /** Add option for a specific index */
  def setOption(index: Long, option: StatisticsOption): Unit = {
    if (exists(index)) {
      throw new IllegalArgumentException(s"Key ${index} already exists")
    }
    options.put(index, option)
  }

  /** Key exists in the map */
  def exists(index: Long): Boolean = {
    options.containsKey(index)
  }

  /** Get option of value for a key */
  def get(index: Long): Option[StatisticsOption] = {
    if (exists(index)) {
      Option(options.get(index))
    } else {
      None
    }
  }

  /** Prepare Statistics for writing */
  def finalizeStatistics(): Statistics = {
    return new Statistics(version, count.get(), options.values().toArray(
      new Array[StatisticsOption](options.size())))
  }

  /** Get file path to the summary file */
  def getFilepath(): String = filepath
}
