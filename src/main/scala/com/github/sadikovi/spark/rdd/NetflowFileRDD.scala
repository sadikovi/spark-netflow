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

package com.github.sadikovi.spark.rdd

import java.io.IOException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.spark.{SparkContext, Partition, TaskContext, InterruptibleIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row => SQLRow}
import org.apache.spark.sql.sources.Filter

import com.github.sadikovi.netflowlib.{NetflowReader, NetflowHeader, RecordBuffer}
import com.github.sadikovi.netflowlib.statistics.{StatisticsReader, StatisticsWriter}
import com.github.sadikovi.netflowlib.version.NetflowV5
import com.github.sadikovi.spark.netflow.sources._

/**
 * Netflow metadata that describes file to process. Contains expected version of a file, absolute
 * resolved path to the file, and it's length, buffer size for a particular file (currently we
 * do not make that distinction), conversion functions mapped to a column index, and file
 * statistics summary.
 */
private[spark] case class NetflowMetadata(
  version: Short,
  path: String,
  length: Long,
  bufferSize: Int,
  conversions: Map[Int, Any => String],
  summary: Option[Summary]
)

/** NetflowFilePartition to hold sequence of file paths */
private[spark] class NetflowFilePartition[T<:NetflowMetadata: ClassTag] (
    var rddId: Long,
    var slice: Int,
    var values: Seq[T]) extends Partition with Serializable {
  def iterator: Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: NetflowFilePartition[_] => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice
}

/**
 * `NetflowFileRDD` is designed to process Netflow file of specific version and return iterator of
 * SQL rows back. Used internally solely for the purpose of datasource API. We assume that we
 * process files of the same version, and prune common fields. `NetflowFileRDD` operates on already
 * resolved columns, the same applies to filters.
 */
private[spark] class NetflowFileRDD[T<:SQLRow: ClassTag] (
    @transient sc: SparkContext,
    @transient data: Seq[NetflowMetadata],
    numSlices: Int,
    resolvedColumns: Array[Long],
    resolvedFilters: Array[InternalFilter]) extends FileRDD[SQLRow](sc, Nil) {
  /** Partition [[NetflowMetadata]], slightly modified Spark partitioning function */
  private def slice(seq: Seq[NetflowMetadata], numSlices: Int): Seq[Seq[NetflowMetadata]] = {
    require(numSlices >= 1, "Positive number of slices required")

    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map(i => {
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      })
    }

    val array = seq.toArray
    positions(array.length, numSlices).map { case (start, end) =>
      array.slice(start, end).toSeq
    }.toSeq
  }

  /**
   * Compile [[InternalFilter]] to a boolean value to decide whether we need to skip this file or
   * proceed with scanning. If filter cannot be resolved, we read entire file, being optimistic
   * about records.
   */
  private def compileFilter(
      filter: InternalFilter,
      resolver: Map[Long, (Long, Long)]): Boolean = filter match {
    case InternalEqualTo(field, value) if resolver.contains(field) =>
      val (min, max) = resolver(field)
      value >= min && value <= max

    case InternalGreaterThan(field, value) if resolver.contains(field) =>
      val (_, max) = resolver(field)
      value < max

    case InternalGreaterThanOrEqual(field, value) if resolver.contains(field) =>
      val (_, max) = resolver(field)
      value <= max

    case InternalLessThan(field, value) if resolver.contains(field) =>
      val (min, _) = resolver(field)
      value > min

    case InternalLessThanOrEqual(field, value) if resolver.contains(field) =>
      val (min, _) = resolver(field)
      value >= min

    case InternalIn(field, values) if resolver.contains(field) =>
      val (min, max) = resolver(field)
      values.exists { value => value >= min && value <= max }

    case InternalAnd(left, right) =>
      compileFilter(left, resolver) && compileFilter(right, resolver)

    case InternalOr(left, right) =>
      compileFilter(left, resolver) || compileFilter(right, resolver)

    case InternalUnhandledFilter(status) =>
      status

    case unsupported =>
      logWarning(s"Filter ${unsupported} could not be compiled")
      true
  }

  override def getPartitions: Array[Partition] = {
    val slices = this.slice(data, numSlices).toArray
    slices.indices.map(i => new NetflowFilePartition[NetflowMetadata](id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[SQLRow] = {
    val conf = getConf()
    var buffer: Iterator[Array[Object]] = Iterator.empty

    for (elem <- s.asInstanceOf[NetflowFilePartition[NetflowMetadata]].iterator) {
      // Reconstruct file status: file path, length in bytes
      val path = new Path(elem.path)
      val fs = path.getFileSystem(conf)
      val fileLength = elem.length

      // Find out if statistics are used and unwrap summary object, we also will have to initialize
      // statistics path related file system
      val useStatistics = elem.summary.isDefined

      val statSummary: Summary = if (useStatistics) {
        elem.summary.get
      } else {
        null
      }

      val (statResolvedPath, statFS) = if (useStatistics) {
        val tmpPath = new Path(statSummary.getFilepath())
        (tmpPath, tmpPath.getFileSystem(conf))
      } else {
        (null, null)
      }

      // Before reading actual file we can read related summary file and extract statistics, such
      // number of records in the file, min/max values for certain fields. Statistics are mainly to
      // decide whether we need to proceed scanning file or skip to the next one (similar to bloom
      // filters). We print summary only when we use statistics. Note that if "_metadata" file is
      // not found we will try creating it. Another note is that we resolve statistics for
      // "unix_secs" (capture time) regardless of having summary file or not, since we can extract
      // information from header.
      val filterStatus1 = if (useStatistics && statSummary.readonly()) {
        logDebug("Found statistics, preparing and reading summary file")

        val summaryReadable = statSummary.asInstanceOf[SummaryReadable]

        val inputStream = statFS.open(statResolvedPath)
        val javaSummary = new StatisticsReader(inputStream).read()

        // We fail, if statistics version does not match expected version, since it can lead to
        // problems of different fields with the same column id having applied wrong predicate.
        require(javaSummary.getVersion() == elem.version, "Cannot apply statistics. " +
          s"Expected version ${elem.version}, got ${javaSummary.getVersion()}")

        // Now we have to resolve filters and decide whether we need to scan file. In case of count
        // we should decide whether we can create an boolean iterator of `count` length. We can do
        // it only when no filters are specified. I do not know any other way of by-passing count
        // computation and return just the number of records.
        summaryReadable.setCount(javaSummary.getCount())
        summaryReadable.setOptionsFromSource(javaSummary.getOptions())

        logInfo(s"""
          > NetFlow statistics summary: {
          >   File: ${elem.path}
          >   Statistics: {
          >     usage: ${useStatistics}
          >     path: ${summaryReadable.getFilepath()}
          >     count: ${javaSummary.getCount()}
          >     summary: ${javaSummary.getOptions().mkString(", ")}
          >   }
          > }
        """.stripMargin('>'))

        // Apply filters for summary
        var bufferStatus = true
        val mapResolver: Map[Long, (Long, Long)] = javaSummary.getOptions().map { opt =>
            (opt.getField(), (opt.getMin(), opt.getMax())) }.toMap

        for (filter <- resolvedFilters) {
          bufferStatus = bufferStatus && compileFilter(filter, mapResolver)
        }
        // As final status
        bufferStatus
      } else {
        true
      }

      // If `filterStatus1` is `true`, we check "unix_secs" time, otherwise skip this file and
      // return empty iterator.
      if (filterStatus1) {
        // prepare file stream
        val stm: FSDataInputStream = fs.open(path)
        val nr = new NetflowReader(stm)
        val hr = nr.readHeader()
        // actual version of the file
        val actualVersion = hr.getFlowVersion()
        // conversion rules to apply
        val conversions = elem.conversions
        // compression flag
        val isCompressed = hr.isCompressed()

        // Currently we cannot resolve version and proceed with parsing, we require pre-set version.
        require(actualVersion == elem.version,
          s"Expected version ${elem.version}, got ${actualVersion} for file ${elem.path}")

        logInfo(s"""
          > NetFlow: {
          >   File: ${elem.path}
          >   File length: ${fileLength} bytes
          >   Flow version: ${actualVersion}
          >   Compression: ${isCompressed}
          >   Buffer size: ${elem.bufferSize} bytes
          >   Hostname: ${hr.getHostname()}
          >   Comments: ${hr.getComments()}
          > }
        """.stripMargin('>'))

        // resolve filter status for unix_secs
        val mapResolver = Map(NetflowV5.V5_FIELD_UNIX_SECS ->
          (hr.getStartCapture(), hr.getEndCapture()))

        var filterStatus2 = true
        for (filter <- resolvedFilters) {
          filterStatus2 = filterStatus2 && compileFilter(filter, mapResolver)
        }

        if (!filterStatus2) {
          logInfo(s"Skipping file ${elem.path}, does not pass predicate evaluation")
        }

        if (filterStatus2) {
          val recordBuffer = nr.readData(hr, resolvedColumns, elem.bufferSize)
          val iterator = recordBuffer.iterator().asScala

          // Iterator with injected statistics handling. Every record processed goes through
          // `SummaryWritable` and count is accumulated. For the last iteration summary is saved
          // into file specified.
          val statisticsIterator = if (useStatistics && !statSummary.readonly()) {
            val summaryWritable = statSummary.asInstanceOf[SummaryWritable]

            new Iterator[Array[Object]] {
              override def hasNext: Boolean = {
                val isNext = iterator.hasNext
                if (!isNext) {
                  logDebug("End of file reached, preparing and writing summary file")

                  val outputStream = statFS.create(statResolvedPath, false)
                  val writer = new StatisticsWriter(outputStream)
                  writer.write(summaryWritable.finalizeStatistics())
                }
                isNext
              }

              override def next(): Array[Object] = {
                summaryWritable.incrementCount()

                iterator.next().zipWithIndex.map { case (value, index) =>
                  if (summaryWritable.exists(index)) {
                    summaryWritable.updateForIndex(index, value.asInstanceOf[Any])
                    value
                  } else {
                    value
                  }
                }
              }
            }
          } else {
            iterator
          }

          // Conversion iterator, applies defined modification for convertable fields
          val conversionsIterator = if (conversions.nonEmpty) {
            // For each array of fields we check if current field matches list of possible
            // conversions, and convert, otherwise return unchanged field. Do not forget to check
            // field constant index to remove overlap with indices from other versions
            statisticsIterator.map(arr =>
              arr.zipWithIndex.map { case (value, index) => conversions.get(index) match {
                case Some(func) => func(value.asInstanceOf[Any])
                case None => value
              } }
            )
          } else {
            statisticsIterator
          }

          buffer = buffer ++ conversionsIterator
        }
      }
    }

    new Iterator[SQLRow] {
      def next(): SQLRow = {
        SQLRow.fromSeq(buffer.next())
      }

      def hasNext: Boolean = {
        buffer.hasNext
      }
    }
  }
}
