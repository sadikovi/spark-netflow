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

import java.io.IOException

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

  // Netflow version
  private val version = parameters.get("version") match {
    case Some(str) => SchemaResolver.validateVersion(str).
      getOrElse(sys.error(s"Invalid version specified: ${str}"))
    case None => sys.error("'version' must be specified for Netflow data")
  }

  // buffer size, by default use standard record buffer size ~3Mb
  private val bufferSize = parameters.get("buffer") match {
    case Some(str) =>
      val bytes = Utils.byteStringAsBytes(str)
      if (bytes > Integer.MAX_VALUE) {
        sys.error(s"Cannot set buffer larger than ${Integer.MAX_VALUE}")
      }
      bytes.toInt
    case None => RecordBuffer.BUFFER_LENGTH_1
  }

  // conversion of numeric field into string, such as IP, by default is off
  private val stringify = parameters.get("stringify") match {
    case Some("true") => true
    case _ => false
  }

  // whether or not to use/collect NetFlow statistics, "statistics" option can have either boolean
  // value or directory to store and look up metadata files. Directory can be either on local file
  // system or HDFS, should be available for reads and writes, otherwise throws IOException. If
  // option is "true", then we assume that statistics files are stored in the same directory as
  // actual files.
  private val maybeStatistics: Option[Path] = parameters.get("statistics") match {
    case Some("true") => Some(null)
    case Some("false") => None
    case Some(dir: String) =>
      // resolve directory for storing and looking up statistics. Directory will be resolved for
      // Spark Hadoop configuration, and will be fully qualified path without any symlinks.
      val conf = sqlContext.sparkContext.hadoopConfiguration
      val maybeDir = new Path(dir)
      val dirFileSystem = maybeDir.getFileSystem(conf)
      if (dirFileSystem.isDirectory(maybeDir)) {
        val resolvedPath = dirFileSystem.resolvePath(maybeDir)
        Some(resolvedPath)
      } else {
        throw new IOException(s"Path for statistics ${maybeDir} is not a directory")
      }
    case _ => None
  }

  // mapper for Netflow version, will be used to create schema and convert columns
  private val mapper = SchemaResolver.getMapperForVersion(version)

  // get buffer size in bytes, mostly for testing
  private[netflow] def getBufferSize(): Int = bufferSize

  // get statistics option, mostly for testing
  private[netflow] def getStatistics(): Option[Path] = maybeStatistics

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
        if (maybeStatistics.isEmpty) {
          logger.warn("Required columns are empty, using first column instead")
          mapper.getFirstInternalColumn()
        } else {
          // when required columns are empty, e.g. in case of direct `count()` we use statistics
          // fields to collect summary for a file
          logger.warn("Required columns are empty, using statistics columns instead")
          mapper.getStatisticsColumns()
        }
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

      // Netflow metadata/summary for each file. We cannot pass `FileStatus` for each partition from
      // file path, it is not serializable and does not behave well with `SerializableWriteable`.
      // We also resolve statistics path to generate [[SummaryWritable]] or [[SummaryReadable]]
      // depending on whether or not summary file exists. If path is null, then we assume that
      // summary file is stored in the same directory as actual Netflow file. If statistics are not
      // used, this should not impact performance.
      val metadata = inputFiles.map { status => {
        val summary: Option[Summary] = if (maybeStatistics.isEmpty) {
          None
        } else {
          val currentPath = status.getPath()
          val fileName = s"_metadata-r-${Utils.uuidForString(currentPath.getParent().toString())}" +
            s".${currentPath.getName()}"

          val tempDir = maybeStatistics match {
            case Some(resolvedDir: Path) if resolvedDir != null => resolvedDir
            case _ => currentPath.getParent()
          }

          val filePath = new Path(tempDir, fileName)
          val fs = filePath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
          if (fs.exists(filePath)) {
            mapper.getSummaryReadable(filePath.toString())
          } else {
            mapper.getSummaryWritable(filePath.toString(), resolvedColumns)
          }
        }

        NetflowMetadata(version, status.getPath().toString(), status.getLen(), bufferSize,
          conversions, summary)
      } }
      // return `NetflowFileRDD`, we store data of each file in individual partition
      new NetflowFileRDD(sqlContext.sparkContext, metadata, metadata.length, resolvedColumns,
        filters)
    }
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported in this version of package")
  }

  override def toString: String = {
    s"${getClass.getSimpleName}: ${version}"
  }
}
