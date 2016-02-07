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
import com.github.sadikovi.netflowlib.statistics.StatisticsWriter
import com.github.sadikovi.netflowlib.version.NetflowV5
import com.github.sadikovi.spark.netflow.sources.SummaryWritable

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
  summary: Option[SummaryWritable]
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
    filters: Array[Filter],
    maybeStatistics: Option[String]) extends FileRDD[SQLRow](sc, Nil) {
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
   * Resolve directory for storing and looking up statistics. Directory will be resolved for Spark
   * Hadoop configuration, and will be fully qualified path without any symlinks.
   */
  private[spark] def resolveStatisticsDir(
      maybeStatistics: Option[String],
      conf: Configuration): (Boolean, Option[Path]) = maybeStatistics match {
    case Some(dir) =>
      if (dir.isEmpty) {
        (true, None)
      } else {
        val maybeDir = new Path(dir)
        val dirFileSystem = maybeDir.getFileSystem(conf)
        if (dirFileSystem.isDirectory(maybeDir)) {
          val resolvedPath = dirFileSystem.resolvePath(maybeDir)
          (true, Some(resolvedPath))
        } else {
          throw new IOException(s"Path for statistics ${maybeDir} is not a directory")
        }
      }
    case None => (false, None)
  }

  override def getPartitions: Array[Partition] = {
    val slices = this.slice(data, numSlices).toArray
    slices.indices.map(i => new NetflowFilePartition[NetflowMetadata](id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[SQLRow] = {
    val conf = getConf()
    var buffer: Iterator[Array[Object]] = Iterator.empty
    val (useStatistics, statisticsDir) = resolveStatisticsDir(maybeStatistics, conf)

    for (elem <- s.asInstanceOf[NetflowFilePartition[NetflowMetadata]].iterator) {
      // reconstruct file path
      val path = new Path(elem.path)
      val fs = path.getFileSystem(conf)
      val fileLength = elem.length
      // statistics data, update statistics directory, if use current file directory
      val (foundStatisticsFile, statisticsResolvedPath) = {
        val tempDir = statisticsDir match {
          case Some(dir) if useStatistics => dir
          case _ => path.getParent()
        }
        val fileName = s"_metadata.${path.getName()}"
        val filePath = tempDir.suffix(Path.SEPARATOR + fileName)
        (fs.exists(filePath), filePath)
      }

      logInfo(s"""
        > NetFlow statistics summary: {
        >   File: ${elem.path}
        >   Statistics: {
        >     usage: ${useStatistics}
        >     found: ${foundStatisticsFile}
        >     path: ${statisticsResolvedPath.toString()}
        >   }
        > }
      """.stripMargin('>'))

      // prepare file stream
      val stm: FSDataInputStream = fs.open(path)

      // build Netflow reader and check whether it can be read
      val nr = new NetflowReader(stm)
      val hr = nr.readHeader()
      // actual version of the file
      val actualVersion = hr.getFlowVersion()
      // conversion to apply
      val conversions = elem.conversions
      // compression flag is second bit in header flags
      val isCompressed = (hr.getHeaderFlags() & 0x2) > 0
      // statistics to apply
      val statOpts = elem.summary

      // currently we cannot resolve version and proceed with parsing, we require pre-set version.
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

      val recordBuffer = nr.readData(hr, resolvedColumns, elem.bufferSize)
      val iterator = recordBuffer.iterator().asScala

      val statisticsIterator = if (useStatistics && !foundStatisticsFile && statOpts.isDefined) {
        val resolvedStatOpts = statOpts.get

        new Iterator[Array[Object]] {
          override def hasNext: Boolean = {
            val isNext = iterator.hasNext
            if (!isNext) {
              logInfo("End of file reached, preparing and writing summary file")
              val outputStream = fs.create(statisticsResolvedPath, false)
              val writer = new StatisticsWriter(outputStream)
              writer.write(resolvedStatOpts.finalizeStatistics())
            }
            isNext
          }

          override def next(): Array[Object] = {
            // increment global count, it is safe to do it before resolving individual options,
            // since we would fail before writing partial / over-evaluated count
            resolvedStatOpts.incrementCount()

            iterator.next().zipWithIndex.map { case (value, index) =>
              if (resolvedStatOpts.exists(index)) {
                resolvedStatOpts.updateForIndex(index, value.asInstanceOf[Any])
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

      val conversionsIterator = if (conversions.isEmpty) {
        statisticsIterator
      } else {
        // for each array of fields we check if current field matches list of possible conversions,
        // and convert, otherwise return unchanged field.
        // do not forget to check field constant index to remove overlap with indices from other
        // versions
        statisticsIterator.map(arr =>
          arr.zipWithIndex.map { case (value, index) => conversions.get(index) match {
            case Some(func) => func(value.asInstanceOf[Any])
            case None => value
          } }
        )
      }

      buffer = buffer ++ conversionsIterator
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
