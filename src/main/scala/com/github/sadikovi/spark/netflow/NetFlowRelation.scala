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

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

import com.github.sadikovi.netflowlib.NetFlowReader
import com.github.sadikovi.netflowlib.Buffers.RecordBuffer
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate
import com.github.sadikovi.spark.netflow.index.StatisticsPathResolver
import com.github.sadikovi.spark.netflow.sources._
import com.github.sadikovi.spark.rdd.NetFlowFileRDD
import com.github.sadikovi.spark.util.Utils

private[netflow] class NetFlowRelation(
    override val paths: Array[String],
    private val maybeDataSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    private val parameters: Map[String, String])
    (@transient val sqlContext: SQLContext) extends HadoopFsRelation {

  private val logger = LoggerFactory.getLogger(getClass())

  // Interface for NetFlow version
  private val interface = NetFlowRegistry.createInterface(
    getVersionQualifiedClassName(parameters.get("version")))

  // Buffer size in bytes, by default use standard record buffer size ~1Mb
  private val bufferSize = parameters.get("buffer") match {
    case Some(str) =>
      val bytes = Utils.byteStringAsBytes(str)
      if (bytes > Integer.MAX_VALUE) {
        sys.error(s"Cannot set buffer larger than ${Integer.MAX_VALUE}")
      } else if (bytes < RecordBuffer.MIN_BUFFER_LENGTH) {
        logger.warn(s"Buffer size ${bytes} < minimum buffer size, it will be updated to " +
          "minimum buffer size")
        RecordBuffer.MIN_BUFFER_LENGTH
      } else {
        bytes.toInt
      }
    case None => RecordBuffer.BUFFER_LENGTH_2
  }

  // Conversion of numeric field into string, such as IP, by default is on
  private val applyConversion = parameters.get("stringify") match {
    case Some("true") => true
    case Some("false") => false
    case _ => true
  }

  // Whether or not to use predicate pushdown at the NetFlow library level
  private val usePredicatePushdown = parameters.get("predicate-pushdown") match {
    case Some("true") => true
    case Some("false") => false
    case _ => true
  }

  // Partition mode, allows to specify custom number of partitions. Note that if number of
  // partitions is larger than number of files we use default partitioning (per file)
  private val partitionMode = parameters.get("partitions") match {
    case Some("auto") =>
      // Average partition size, it will be compared with mean size eventually to pick the best
      // suited bucket size
      val partitionSize = Utils.byteStringAsBytes(
        sqlContext.getConf("spark.sql.netflow.partition.size", "144Mb"))
      // Minimum number of partitions to keep as a result of grouping
      val minNumPartitions = Try(sqlContext.getConf("spark.sql.netflow.partition.num").toInt).
        getOrElse(sqlContext.sparkContext.defaultParallelism * 2)
      AutoPartitionMode(partitionSize, minNumPartitions)
    case Some("default") =>
      DefaultPartitionMode(None)
    case Some(maybeNumPartitions) => Try(maybeNumPartitions.toInt) match {
      case Success(numPartitions) =>
        DefaultPartitionMode(Some(numPartitions))
      case Failure(error) =>
        sys.error(s"Wrong number of partitions $maybeNumPartitions")
    }
    case None =>
      DefaultPartitionMode(None)
  }
  logger.info(s"Partition mode: $partitionMode")

  // Usage of statistics, this will trigger either writing statistics or reading them and adding
  // more information to resolving predicate when filter pushdown is specified, otherwise it will
  // not collect or use statistics
  private val statisticsStatus = parameters.get("statistics") match {
    case Some("true") if usePredicatePushdown => Some(StatisticsPathResolver(None))
    case Some("false") => None
    case Some(path) if usePredicatePushdown => Some(StatisticsPathResolver(Option(path)))
    case otherValue => None
  }
  logger.info(s"Statistics: $statisticsStatus")

  // Get currently parsed interface, mostly for testing
  private[netflow] def getInterface(): String = interface.getClass.getName

  // Get buffer size in bytes, mostly for testing
  private[netflow] def getBufferSize(): Int = bufferSize

  private[netflow] def getPredicatePushdown(): Boolean = usePredicatePushdown

  // Get partition mode, mostly for testing
  private[netflow] def getPartitionMode(): PartitionMode = partitionMode

  private[netflow] def inferSchema(): StructType = {
    interface.getSQLSchema(applyConversion)
  }

  /** Resolve version as class name for resolving interface */
  private[netflow] def getVersionQualifiedClassName(maybeVersion: Option[String]): String = {
    maybeVersion match {
      case Some(str: String) => Try(str.toInt) match {
        case Success(version) => s"${NetFlowRelation.INTERNAL_PARTIAL_CLASSNAME}$version"
        case Failure(error) => str
      }
      case None =>
        // we try resolving option if possible using similar approach as Parquet datasource
        val version = NetFlowRelation.
          lookupVersion(sqlContext.sparkContext.hadoopConfiguration, paths).
          getOrElse(NetFlowRelation.VERSION_5.toInt)
        logger.debug(s"Resolved interface to version $version")
        s"${NetFlowRelation.INTERNAL_PARTIAL_CLASSNAME}$version"
    }
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
      logger.warn("Could not resolve input files, potentially files do not exist")
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      // Convert SQL columns to internal mapped columns, also check if statistics are enabled, so
      // schema is adjusted to include statistics columns
      val resolvedColumns: Array[MappedColumn] = if (requiredColumns.isEmpty) {
        if (statisticsStatus.isDefined) {
          val maybeStatisticsColumns = interface.getStatisticsColumns().toArray
          if (maybeStatisticsColumns.nonEmpty) {
            logger.info("Required columns are empty, using statistics columns instead")
            maybeStatisticsColumns
          } else {
            logger.info("Required columns are empty, statistics columns are not provided, using " +
              "first column instead")
            Array(interface.getFirstColumn())
          }
        } else {
          logger.info("Required columns are empty, using first column instead")
          Array(interface.getFirstColumn())
        }
      } else {
        requiredColumns.map(col => interface.getColumn(col))
      }

      // Resolve filters into filters we support, also reduce to return only one Filter value
      val reducedFilter: Option[Filter] = NetFlowFilters.reduceFilter(filters)
      logger.info(s"Reduced filter: $reducedFilter")

      // Convert filters into NetFlow filters, we also use `usePredicatePushdown` to disable
      // predicate pushdown (normally it is used for benchmarks), but in some situations when
      // library filters incorrectly this can be a short time fix
      val resolvedFilter: Option[FilterPredicate] = reducedFilter match {
        case Some(filter) if usePredicatePushdown =>
          Option(NetFlowFilters.convertFilter(filter, interface))
        case Some(filter) if !usePredicatePushdown =>
          logger.warn("Predicate pushdown is disabled")
          None
        case other =>
          None
      }
      logger.info(s"Resolved NetFlow filter: $resolvedFilter")

      // NetFlow file status for each file. We cannot pass `FileStatus` for each partition from
      // file path, it is not serializable and does not behave well with `SerializableWriteable`.
      // Note that file size (`status.getLen()`) is in bytes and can be used for auto partitioning.
      // See: https://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/fs/FileStatus.html
      val fileStatuses = inputFiles.map { status =>
        val filePath = status.getPath().toString()
        val fileLen = status.getLen()
        val statisticsPathStatus = statisticsStatus match {
          case Some(statsResolver) =>
            val statStatus = statsResolver.getStatisticsPathStatus(filePath,
              sqlContext.sparkContext.hadoopConfiguration)
            Some(statStatus)
          case None => None
        }

        NetFlowFileStatus(interface.version(), filePath, fileLen, bufferSize, statisticsPathStatus)
      }

      // Build statistics columns map of index -> key, where key is a column name and index is an
      // index of the column in the sequence. This is used to identify columns during writing
      // statistics file. Note that all statistics columns must exist in resolved columns, otherwise
      // collecting statistics is discarded.
      val statisticsIndex: Map[Int, MappedColumn] = statisticsStatus match {
        case Some(_) =>
          val statColumns = interface.getStatisticsColumns()
          val possColumns = resolvedColumns.zipWithIndex.flatMap { pair =>
            val (value, index) = pair
            if (statColumns.contains(value)) Some((index, value)) else None
          }

          if (statColumns.nonEmpty &&
              statColumns.forall(value => possColumns.exists(_._2 == value))) {
            possColumns.toMap
          } else {
            Map.empty
          }
        case None => Map.empty
      }
      logger.info(s"Resolved statistics index: $statisticsIndex")

      // Return `NetFlowFileRDD`, we store data of each file based on provided partition mode
      new NetFlowFileRDD(sqlContext.sparkContext, fileStatuses, partitionMode, applyConversion,
        resolvedColumns, resolvedFilter, statisticsIndex)
    }
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported in this version of package")
  }

  override def toString: String = {
    s"${getClass.getSimpleName}(" +
      s"version ${interface.version()}, " +
      s"partition mode $partitionMode, " +
      s"buffer size $bufferSize, " +
      s"predicate pushdown $usePredicatePushdown, " +
      s"statistics $statisticsStatus, " +
      s"conversion: $applyConversion)"
  }
}

/** Companion object to maintain internal constants */
private[spark] object NetFlowRelation {
  val INTERNAL_PARTIAL_CLASSNAME = "com.github.sadikovi.spark.netflow.version"
  val VERSION_5 = "5"
  val VERSION_7 = "7"

  /**
   * Look up NetFlow version given paths to the files. We select only subset of files and return
   * final version, since we only support batch of files of same version.
   */
  def lookupVersion(conf: Configuration, paths: Array[String]): Option[Int] = {
    if (paths.isEmpty) {
      None
    } else {
      // take first path and read header
      val path = new Path(paths.head)
      val fs = path.getFileSystem(conf)
      var stream: FSDataInputStream = null
      try {
        stream = fs.open(path)
        // buffer size at this point does not really matter, we are not reading entire file
        val reader = NetFlowReader.prepareReader(stream, RecordBuffer.MIN_BUFFER_LENGTH)
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
