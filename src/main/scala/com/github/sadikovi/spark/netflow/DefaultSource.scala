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

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

import com.github.sadikovi.netflowlib.NetFlowReader
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate
import com.github.sadikovi.spark.netflow.sources._
import com.github.sadikovi.spark.util.{CloseableIterator, SerializableConfiguration}

class DefaultSource extends FileFormat with DataSourceRegister {
  @transient private val log = LoggerFactory.getLogger(classOf[DefaultSource])
  // Resolved interface for NetFlow version schema
  private var interface: ResolvedInterface = _
  // Resolved datasource options
  private var opts: NetFlowOptions = _

  override def shortName(): String = "netflow"

  /** Resolve interface based on datasource options and list of files to read */
  private[netflow] def resolveInterface(
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
      interface = resolveInterface(spark, options, files.map(_.getPath))
    }
    log.info(s"Resolved interface as $interface")

    if (opts == null) {
      opts = new NetFlowOptions(options)
    }
    log.info(s"Resolved options as $opts")

    Some(interface.getSQLSchema(opts.applyConversion))
  }

  override def buildReader(
      spark: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    // Check that interface and options are resolved
    assert(interface != null)
    assert(opts != null)

    // Convert SQL columns to internal mapped columns, also check if statistics are enabled, so
    // schema is adjusted to include statistics columns
    val resolvedColumns: Array[MappedColumn] = if (requiredSchema.isEmpty) {
      Array.empty
    } else {
      requiredSchema.fieldNames.map { col => interface.getColumn(col) }
    }

    // Resolve filters into filters we support, also reduce to return only one Filter value
    val reducedFilter: Option[Filter] = NetFlowFilters.reduceFilter(filters)
    log.info(s"Reduced filter: $reducedFilter")

    // Convert filters into NetFlow filters, we also use `usePredicatePushdown` to disable
    // predicate pushdown (normally it is used for benchmarks), but in some situations when
    // library filters incorrectly this can be a short time fix
    val resolvedFilter: Option[FilterPredicate] = reducedFilter match {
      case Some(filter) if opts.usePredicatePushdown =>
        Option(NetFlowFilters.convertFilter(filter, interface))
      case Some(filter) if !opts.usePredicatePushdown =>
        log.warn("NetFlow library-level predicate pushdown is disabled")
        None
      case other =>
        None
    }
    log.info(s"Resolved NetFlow filter: $resolvedFilter")

    prepareRead(spark, hadoopConf, interface, opts, resolvedColumns, resolvedFilter)
  }

  /** Return partial function to read partitioned files */
  private def prepareRead(
      spark: SparkSession,
      hadoopConf: Configuration,
      interface: ResolvedInterface,
      opts: NetFlowOptions,
      resolvedColumns: Array[MappedColumn],
      resolvedFilter: Option[FilterPredicate]): (PartitionedFile) => Iterator[InternalRow] = {
    // Required version of NetFlow files
    val flowVersion = interface.version()
    // Array of internal columns for NetFlow library
    val internalColumns = resolvedColumns.map(_.internalColumn)
    // Number of columns to process
    val numColumns = resolvedColumns.length
    // when true, ignore corrupt files, either with wrong header or corrupt data block
    val ignoreCorruptFiles = spark.sessionState.conf.ignoreCorruptFiles
    val confBroadcast = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val optsBroadcast = spark.sparkContext.broadcast(opts)

    (file: PartitionedFile) => {
      // Separate logger for each partition, do not use global logger for default source
      val log = LoggerFactory.getLogger(classOf[DefaultSource])
      val opts = optsBroadcast.value
      val conf = confBroadcast.value.value

      val path = new Path(file.filePath)
      val fs = path.getFileSystem(conf)
      val fileLength = file.length

      // Prepare file stream
      var stm: FSDataInputStream = fs.open(path)
      // If reader initialization fails we either reset to null or report error
      // latter check will ensure that we log corrupt file
      val reader = try {
        NetFlowReader.prepareReader(stm, opts.bufferSize, ignoreCorruptFiles)
      } catch {
        case NonFatal(err) if ignoreCorruptFiles => null
        case NonFatal(err) => throw new RuntimeException(s"${err.getMessage}, file=$path", err)
      }
      // This flag is only checked when ignoreCorruptFiles = true, otherwise initialization will
      // throw exception, if file is corrupt
      if (reader == null || !reader.isValid()) {
        log.warn(s"Failed to read file $path, ignoreCorruptFiles=$ignoreCorruptFiles")
        Iterator.empty
      } else {
        val header = reader.getHeader()
        // Actual version of the file
        val actualVersion = header.getFlowVersion()

        log.debug(s"""
            > NetFlow: {
            >   File: $path,
            >   File length: $fileLength bytes,
            >   Flow version: $actualVersion,
            >   Compression: ${header.isCompressed()},
            >   Buffer size: ${opts.bufferSize} bytes,
            >   Start capture: ${header.getStartCapture()},
            >   End capture: ${header.getEndCapture()},
            >   Hostname: ${header.getHostname()},
            >   Comments: ${header.getComments()}
            > }
          """.stripMargin('>'))

        // Currently we cannot resolve version and proceed with parsing, we require pre-set version.
        require(actualVersion == flowVersion,
          s"Expected version $flowVersion, got $actualVersion for file $path. Scan of the files " +
            "with different (but compatible) versions, e.g. 5, 6, and 7 is not supported currently")
        // Build record buffer based on resolved filter, if filter is not defined use default scan
        // with trivial predicate
        val recordBuffer = resolvedFilter match {
          case Some(filter) => reader.prepareRecordBuffer(internalColumns, filter)
          case None => reader.prepareRecordBuffer(internalColumns)
        }

        val rawIterator = new CloseableIterator[Array[Object]] {
          private var delegate = recordBuffer.iterator().asScala
          // add filepath to report for any error message
          private val filepath = path

          override def getNext(): Array[Object] = {
            // If delegate has traversed over all elements mark it as finished
            // to allow to close stream
            if (delegate.hasNext) {
              try {
                delegate.next
              } catch {
                case NonFatal(err) =>
                  throw new RuntimeException(s"${err.getMessage}, file=$filepath", err)
              }
            } else {
              finished = true
              null
            }
          }

          override def close(): Unit = {
            // Close stream if possible of fail silently,
            // at this point exception does not really matter
            try {
              if (stm != null) {
                stm.close()
                stm = null
              }
            } catch {
              case err: Exception => // do nothing
            }
          }
        }

        // Ensure that the reader is closed even if the task fails or doesn't consume the entire
        // iterator of records.
        Option(TaskContext.get()).foreach { taskContext =>
          taskContext.addTaskCompletionListener[Unit] { _ =>
            rawIterator.closeIfNeeded
          }
        }

        // Conversion iterator, applies defined modification for convertable fields
        val withConversionsIterator = if (opts.applyConversion) {
          // For each field we check if possible conversion is available. If it is we apply direct
          // conversion, otherwise return unchanged value. Note that this should be in sync with
          // `applyConversion` and updated schema from `ResolvedInterface`.
          // TODO: improve performance of conversion
          rawIterator.map { arr =>
            for (i <- 0 until numColumns) {
              resolvedColumns(i).convertFunction match {
                case Some(func) => arr(i) = func.directCatalyst(arr(i))
                case None => // do nothing
              }
            }
            arr
          }
        } else {
          rawIterator
        }

        new Iterator[InternalRow] {
          override def next(): InternalRow = InternalRow.fromSeq(withConversionsIterator.next())
          override def hasNext: Boolean = withConversionsIterator.hasNext
        }
      }
    }
  }

  override def prepareWrite(
      spark: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported in this version of package")
  }

  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource => true
    case _ => false
  }

  /** Get resolved interface, for testing */
  private[netflow] def getInterface(): ResolvedInterface = interface

  /** Get NetFlow options, for testing */
  private[netflow] def getOptions(): NetFlowOptions = opts
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
      } catch {
        case ioe: IOException =>
          throw new IOException(
            s"Failed to infer version for provided NetFlow files using path '$path', " +
            s"reason: $ioe. Try specifying version manually using 'version' option", ioe)
      } finally {
        if (stream != null) {
          stream.close()
          stream = null
        }
      }
    }
  }
}
