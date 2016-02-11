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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types._

import com.github.sadikovi.netflowlib.statistics.StatisticsOption
import com.github.sadikovi.netflowlib.version.NetflowV5

/** Mapper for NetFlow version 5 */
private[netflow] object MapperV5 extends Mapper {
  override def version(): Short = 5

  override def getFullSchema(stringify: Boolean): StructType = {
    val sqlColumns = columns.map(field => {
      if (stringify) {
        val fieldType = if (conversions.contains(field.index)) StringType else field.dtype
        StructField(field.name, fieldType, false)
      } else {
        StructField(field.name, field.dtype, false)
      }
    })
    StructType(sqlColumns)
  }

  override def getInternalColumns(): Array[Long] = columns.map(_.index).toArray

  override def getFirstInternalColumn(): Array[Long] = Array(columns.head.index)

  override def getInternalColumnForName(name: String): Long = {
    index.getOrElse(name, sys.error(s"Index does not have information about column ${name}"))
  }

  override def getConversionsForFields(fields: Array[Long]): Map[Int, Any => String] = {
    val buf: ArrayBuffer[(Int, Any => String)] = new ArrayBuffer()
    for (elem <- fields.zipWithIndex) {
      // elem is a tuple with first element being actual value and second element being index
      conversions.get(elem._1) match {
        case Some(func) => buf.append((elem._2, func))
        case None => // do nothing
      }
    }
    buf.toMap
  }

  override def getReverseConversionForField(field: Long): Option[String => Any] = {
    reverseConversions.get(field)
  }

  override def getSummaryReadable(filepath: String): Option[Summary] = {
    val summary = new SummaryReadable(version(), filepath)
    for (field <- getStatisticsColumns()) {
      // we cheat and keep all values as long fields, saves time on size resolution
      summary.setOption(field, new StatisticsOption(field, 8, Long.MinValue, Long.MaxValue))
    }
    Option(summary)
  }

  override def getSummaryWritable(filepath: String, fields: Array[Long]): Option[Summary] = {
    // to collect statistics we require all necessary fields to exist
    val gatherStatistics = statisticsFields.forall { case (key, flag) =>
      fields.exists(elem => elem == key)
    }

    if (gatherStatistics) {
      val summary = new SummaryWritable(version(), filepath)
      for (elem <- fields.zipWithIndex) {
        val (field, fieldIndex) = elem
        if (!summary.exists(fieldIndex) && statisticsFields.contains(field)) {
          // we cheat and keep all values as long fields, saves time on size resolution
          summary.setOption(fieldIndex,
            new StatisticsOption(field, 8, Long.MinValue, Long.MaxValue))
        }
      }

      Option(summary)
    } else {
      None
    }
  }

  override def getStatisticsColumns(): Array[Long] =
    statisticsFields.map { case (key, flag) => key }.toArray

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

  // helper index of conversion functions
  private lazy val conversions: Map[Long, Any => String] = Map(
    NetflowV5.V5_FIELD_EXADDR -> ConversionFunctions.numToIp,
    NetflowV5.V5_FIELD_SRCADDR -> ConversionFunctions.numToIp,
    NetflowV5.V5_FIELD_DSTADDR -> ConversionFunctions.numToIp,
    NetflowV5.V5_FIELD_NEXTHOP -> ConversionFunctions.numToIp
  )

  // reverse conversions, it is recommended to update both maps when new field is required
  // conversion
  private lazy val reverseConversions: Map[Long, String => Any] = Map(
    NetflowV5.V5_FIELD_EXADDR -> ConversionFunctions.ipToNum,
    NetflowV5.V5_FIELD_SRCADDR -> ConversionFunctions.ipToNum,
    NetflowV5.V5_FIELD_DSTADDR -> ConversionFunctions.ipToNum,
    NetflowV5.V5_FIELD_NEXTHOP -> ConversionFunctions.ipToNum
  )

  // helper index of fields that we keep statistics for
  private lazy val statisticsFields: Map[Long, Boolean] = Map(
    NetflowV5.V5_FIELD_UNIX_SECS -> true,
    NetflowV5.V5_FIELD_SRCADDR -> true,
    NetflowV5.V5_FIELD_SRCPORT -> true,
    NetflowV5.V5_FIELD_DSTADDR -> true,
    NetflowV5.V5_FIELD_DSTPORT -> true
  )
}
