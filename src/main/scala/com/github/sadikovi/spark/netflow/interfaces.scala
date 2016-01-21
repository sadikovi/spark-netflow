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

import scala.util.Try

import org.apache.spark.Logging
import org.apache.spark.sql.types.{StructType, StructField, DataType, LongType, IntegerType, ShortType}

import com.github.sadikovi.netflow.version.NetflowV5

////////////////////////////////////////////////////////////////
// Schema resolver
////////////////////////////////////////////////////////////////
/** Schema resolver for Netflow versions. Also provides mapping for a particular column name. */
private[netflow] object SchemaResolver extends Logging {
  // Netflow version 5
  final val V5: Short = 5

  /** Get specific mapper for Netflow version */
  def getMapperForVersion(version: Short): Mapper = version match {
    case V5 => MapperV5
    case other => throw new UnsupportedOperationException(
      s"Netflow version ${other} is not supported")
  }

  /** Resolve schema for a specific version */
  def getSchemaForVersion(version: Short): StructType = {
    getMapperForVersion(version).getFullSchema()
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
private case class SchemaField(name: String, index: Long, dtype: DataType)

/** Internal mapper for a Netflow version. Maps java columns to Scala column names. */
private trait Mapper {
  def getFullSchema(): StructType

  def getInternalColumns(): Array[Long]

  // get first internal column for quick scanning
  def getFirstInternalColumn(): Array[Long]

  def getInternalColumnForName(name: String): Long
}

////////////////////////////////////////////////////////////////
// Mappers for different Netflow versions
////////////////////////////////////////////////////////////////
private object MapperV5 extends Mapper {
  override def getFullSchema(): StructType = {
    val sqlColumns = columns.map(field => StructField(field.name, field.dtype, false))
    StructType(sqlColumns)
  }

  override def getInternalColumns(): Array[Long] = columns.map(_.index).toArray

  override def getFirstInternalColumn(): Array[Long] = Array(columns.head.index)

  override def getInternalColumnForName(name: String): Long = {
    index.getOrElse(name, sys.error(s"Index does not have information about column ${name}"))
  }

  // mapping of SQL columns and internal columns for NetflowV5
  private val columns: Seq[SchemaField] = Seq(
    SchemaField("unix_secs", NetflowV5.V5_FIELD_UNIX_SECS, LongType),
    SchemaField("unix_nsecs", NetflowV5.V5_FIELD_UNIX_NSECS, LongType),
    SchemaField("sysuptime", NetflowV5.V5_FIELD_SYSUPTIME, LongType),
    SchemaField("exaddr", NetflowV5.V5_FIELD_EXADDR, LongType),
    SchemaField("srcip", NetflowV5.V5_FIELD_SRCADDR, LongType),
    SchemaField("dstip", NetflowV5.V5_FIELD_DSTADDR, LongType),
    SchemaField("nexthop", NetflowV5.V5_FIELD_NEXTHOP, LongType),
    SchemaField("input", NetflowV5.V5_FIELD_INPUT, IntegerType),
    SchemaField("output", NetflowV5.V5_FIELD_OUTPUT, IntegerType),
    SchemaField("packets", NetflowV5.V5_FIELD_DPKTS, LongType),
    SchemaField("octets", NetflowV5.V5_FIELD_DOCTETS, LongType),
    SchemaField("first_flow", NetflowV5.V5_FIELD_FIRST, LongType),
    SchemaField("last_flow", NetflowV5.V5_FIELD_LAST, LongType),
    SchemaField("srcport", NetflowV5.V5_FIELD_SRCPORT, IntegerType),
    SchemaField("dstport", NetflowV5.V5_FIELD_DSTPORT, IntegerType),
    SchemaField("protocol", NetflowV5.V5_FIELD_PROT, ShortType),
    SchemaField("tos", NetflowV5.V5_FIELD_TOS, ShortType),
    SchemaField("tcp_flags", NetflowV5.V5_FIELD_TCP_FLAGS, ShortType),
    SchemaField("engine_type", NetflowV5.V5_FIELD_ENGINE_TYPE, ShortType),
    SchemaField("engine_id", NetflowV5.V5_FIELD_ENGINE_ID, ShortType),
    SchemaField("src_mask", NetflowV5.V5_FIELD_SRC_MASK, ShortType),
    SchemaField("dst_mask", NetflowV5.V5_FIELD_DST_MASK, ShortType),
    SchemaField("src_as", NetflowV5.V5_FIELD_SRC_AS, IntegerType),
    SchemaField("dst_as", NetflowV5.V5_FIELD_DST_AS, IntegerType)
  )

  // helper index to map sql column to internal column
  private lazy val index: Map[String, Long] = columns.map(field =>
    (field.name, field.index)).toMap
}
