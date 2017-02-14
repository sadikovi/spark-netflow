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

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

import com.github.sadikovi.netflowlib.NetFlowReader
import com.github.sadikovi.netflowlib.Buffers.RecordBuffer
import com.github.sadikovi.spark.netflow.sources.{NetFlowRegistry, ResolvedInterface}

class DefaultSource extends FileFormat with DataSourceRegister {
  private val logger = LoggerFactory.getLogger(getClass)
  private var interface: ResolvedInterface = _

  override def shortName(): String = "netflow"

  /** Resolve interface based on datasource options and list of files to read */
  private def inferInterface(
      spark: SparkSession,
      options: Map[String, String],
      paths: Seq[Path]): ResolvedInterface = {
    // Create interface name to resolve SQL schema
    val className = options.get("version") match {
      case Some(version) => Try(version.toInt) match {
        case Success(flowVersion) => s"${DefaultSource.INTERNAL_PARTIAL_CLASSNAME}$flowVersion"
        case Failure(error) => version
      }
      case None =>
        // infer schema from files
        val flowVersion = DefaultSource.
          inferVersion(spark.sparkContext.hadoopConfiguration, paths).
          getOrElse(DefaultSource.VERSION_5)
        s"${DefaultSource.INTERNAL_PARTIAL_CLASSNAME}$flowVersion"
    }

    NetFlowRegistry.createInterface(className)
  }

  override def inferSchema(
      spark: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    // Create interface name to resolve SQL schema
    if (interface == null) {
      interface = inferInterface(spark, options, files.map { _.getPath })
    }
    logger.info(s"Resolved interface as $interface")

    // Conversion of numeric field into string, such as IP, by default is on
    val applyConversion = options.get("stringify") match {
      case Some("true") => true
      case Some("false") => false
      case _ => true
    }
    Some(interface.getSQLSchema(applyConversion))
  }

  override def prepareWrite(
      spark: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported in this version of package")
  }

  override def buildReader(
      spark: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    null
  }

  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource => true
    case _ => false
  }
}

/** Companion object to maintain internal constants */
object DefaultSource {
  val INTERNAL_PARTIAL_CLASSNAME = "com.github.sadikovi.spark.netflow.version"
  val VERSION_5 = 5
  val VERSION_7 = 7

  /**
   * Infer NetFlow version given paths to the files.
   * Currently we look up only one file and check header version, since we only support read of
   * files that have the same version.
   */
  def inferVersion(conf: Configuration, paths: Seq[Path]): Option[Int] = {
    if (paths.isEmpty) {
      None
    } else {
      // take first path and read header
      val path = paths.head
      val fs = path.getFileSystem(conf)
      var stream: FSDataInputStream = null
      try {
        // load file with default buffer size and extract header
        stream = fs.open(path)
        val reader = NetFlowReader.prepareReader(stream)
        val header = reader.getHeader()
        Some(header.getFlowVersion())
      } finally {
        if (stream != null) {
          stream.close()
          stream = null
        }
      }
    }
  }
}
