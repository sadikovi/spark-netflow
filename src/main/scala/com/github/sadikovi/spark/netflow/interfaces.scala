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

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.sql.types.{DataType, StructType}

import com.github.sadikovi.spark.netflow.sources.{MapperV5, SummaryWritable}

/**
 * Schema resolver for Netflow versions. Also provides mapping for a particular column name.
 */
private[netflow] object SchemaResolver {
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
private[netflow] case class SchemaField(name: String, index: Long, dtype: DataType)

/**
 * Internal mapper for a Netflow version. Maps java columns to Scala column names.
 */
private[netflow] trait Mapper {
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

  /**
   * Resolve and return [[SummaryWritable]] object to store statitics options for fields. Can
   * return `None`, if conditions for gathering statistics are not met: depends on particular
   * mapper.
   */
  def getStatisticsOptionsForFields(fields: Array[Long]): Option[SummaryWritable] = {
    throw new UnsupportedOperationException
  }

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
