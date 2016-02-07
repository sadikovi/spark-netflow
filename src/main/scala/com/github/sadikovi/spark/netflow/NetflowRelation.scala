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

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.sources.{HadoopFsRelation, OutputWriterFactory, Filter}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}

import org.slf4j.LoggerFactory

import com.github.sadikovi.netflowlib.RecordBuffer
import com.github.sadikovi.spark.rdd.{NetflowFileRDD, NetflowMetadata}
import com.github.sadikovi.spark.util.Utils

private[netflow] class NetflowRelation(
    override val paths: Array[String],
    private val maybeDataSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    private val parameters: Map[String, String])
    (@transient val sqlContext: SQLContext) extends HadoopFsRelation {

  private val logger = LoggerFactory.getLogger(getClass)

  // [Netflow version]
  private val version = parameters.get("version") match {
    case Some(str) => SchemaResolver.validateVersion(str).
      getOrElse(sys.error(s"Invalid version specified: ${str}"))
    case None => sys.error("'version' must be specified for Netflow data")
  }

  // [buffer size], by default use standard record buffer size ~3Mb
  private val bufferSize = parameters.get("buffer") match {
    case Some(str) =>
      val bytes = Utils.byteStringAsBytes(str)
      if (bytes > Integer.MAX_VALUE) {
        sys.error(s"Cannot set buffer larger than ${Integer.MAX_VALUE}")
      }
      bytes.toInt
    case None => RecordBuffer.BUFFER_LENGTH_1
  }

  // [conversion of numeric field into string, such as IP], by default is off
  private val stringify = parameters.get("stringify") match {
    case Some("true") => true
    case _ => false
  }

  // [whether or not to use/collect NetFlow statistics], "statistics" option can have either
  // boolean value or directory to store and look up metadata files. Directory can be either on
  // local file system or HDFS, should be available for reads and writes, otherwise throws
  // IOException
  private val maybeStatistics: Option[String] = parameters.get("statistics") match {
    case Some("true") => Some("")
    case Some("false") => None
    case Some(dir) => Option(dir)
    case _ => None
  }

  // mapper for Netflow version, will be used to create schema and convert columns
  private val mapper = SchemaResolver.getMapperForVersion(version)

  // get buffer size in bytes, mostly for testing
  private[netflow] def getBufferSize(): Int = bufferSize

  private[netflow] def inferSchema(): StructType = {
    mapper.getFullSchema(stringify)
  }

  override def dataSchema: StructType = inferSchema()

  override def buildScan(
      requiredColumns: Array[String],
      inputFiles: Array[FileStatus]): RDD[Row] = {
    buildScan(requiredColumns, Array.empty, inputFiles)
  }

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus]): RDD[Row] = {
    if (inputFiles.isEmpty) {
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      // convert to internal Netflow fields
      val resolvedColumns: Array[Long] = if (requiredColumns.isEmpty) {
        logger.warn("Required columns are empty, using first column instead")
        // when required columns are empty, e.g. in case of direct `count()` we use only one column
        // schema to quickly read records
        mapper.getFirstInternalColumn()
      } else {
        requiredColumns.map(col => mapper.getInternalColumnForName(col))
      }

      // conversion array, if stringify option is true, we return map. Each key is an index of a
      // field and value is a conversion function "Any => String". Map is empty when there is
      // no columns with applied conversion or when stringify option is false
      val conversions: Map[Int, Any => String] = if (stringify) {
        mapper.getConversionsForFields(resolvedColumns)
      } else {
        Map.empty
      }

      // we have to reconstruct `FileStatus` for each partition from file path, it is not
      // serializable and does not behave well with `SerializableWriteable`
      val metadata = inputFiles.map { status => {
        // resolved statistics options if summary collection is enabled and fields requirements are
        // met. We have to do it for every file to separate summaries
        val statOpts = if (maybeStatistics.isDefined) {
          mapper.getStatisticsOptionsForFields(resolvedColumns)
        } else {
          None
        }

        NetflowMetadata(version, status.getPath().toString(), status.getLen(), bufferSize,
          conversions, statOpts)
      } }
      // return `NetflowFileRDD`, we store data of each file in individual partition
      new NetflowFileRDD(sqlContext.sparkContext, metadata, metadata.length, resolvedColumns,
        filters, maybeStatistics)
    }
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported in this version of package")
  }
}
