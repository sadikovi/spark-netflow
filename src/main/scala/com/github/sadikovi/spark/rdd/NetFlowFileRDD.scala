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
import org.apache.spark.sql.sources._

import com.github.sadikovi.netflowlib.NetFlowReader
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate
import com.github.sadikovi.spark.netflow.NetFlowFilters
import com.github.sadikovi.spark.netflow.index.AttributeBatch
import com.github.sadikovi.spark.netflow.sources._
import com.github.sadikovi.spark.util.CloseableIterator

/** NetFlowFilePartition to hold sequence of file paths */
private[spark] class NetFlowFilePartition[T <: NetFlowFileStatus : ClassTag] (
    var rddId: Long,
    var slice: Int,
    var values: Seq[T]) extends Partition with Serializable {
  def iterator: Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: NetFlowFilePartition[_] => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice
}

/**
 * [[NetFlowFileRDD]] is designed to process NetFlow file of specific version and return iterator
 * of SQL rows back. Used internally solely for the purpose of datasource API. We assume that we
 * process files of the same version, and prune common fields. `NetFlowFileRDD` operates on already
 * resolved columns, the same applies to filters.
 */
private[spark] class NetFlowFileRDD[T <: SQLRow : ClassTag] (
    @transient sc: SparkContext,
    @transient data: Seq[NetFlowFileStatus],
    val partitionMode: PartitionMode,
    val applyConversion: Boolean,
    val resolvedColumns: Array[MappedColumn],
    val resolvedFilter: Option[FilterPredicate],
    val statisticsIndex: Map[Int, MappedColumn]) extends FileRDD[SQLRow](sc, Nil) {
  override def getPartitions: Array[Partition] = {
    val slices = partitionMode.tryToPartition(data)
    slices.indices.map(i => new NetFlowFilePartition[NetFlowFileStatus](id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[SQLRow] = {
    // Hadoop configuration
    val conf = getConf()
    // Number of columns to process
    val numColumns = resolvedColumns.length
    // Array of internal columns for library
    val internalColumns = resolvedColumns.map(_.internalColumn)
    // Total buffer of records
    var buffer: Iterator[Array[Object]] = Iterator.empty

    for (elem <- s.asInstanceOf[NetFlowFilePartition[NetFlowFileStatus]].iterator) {
      // Check if statistics file status is available. Only read statistics, if filter is provided
      val updatedFilter = if (resolvedFilter.nonEmpty && elem.statisticsPathStatus.nonEmpty) {
        val statStatus = elem.statisticsPathStatus.get
        if (statStatus.exists) {
          logDebug("Applying statistics to update filter")
          val attributes = AttributeBatch.read(statStatus.path, conf)
          Some(NetFlowFilters.updateFilter(resolvedFilter.get, attributes))
        } else {
          resolvedFilter
        }
      } else {
        resolvedFilter
      }

      logDebug(s"Updated filter: $updatedFilter")

      // Reconstruct file status: file path, length in bytes
      val path = new Path(elem.path)
      val fs = path.getFileSystem(conf)
      val fileLength = elem.length

      // Prepare file stream
      var stm: FSDataInputStream = fs.open(path)
      val reader = NetFlowReader.prepareReader(stm, elem.bufferSize)
      val header = reader.getHeader()
      // Actual version of the file
      val actualVersion = header.getFlowVersion()
      // Compression flag
      val isCompressed = header.isCompressed()

      logDebug(s"""
          > NetFlow: {
          >   File: ${elem.path},
          >   File length: ${fileLength} bytes,
          >   Flow version: ${actualVersion},
          >   Compression: ${isCompressed},
          >   Buffer size: ${elem.bufferSize} bytes,
          >   Start capture: ${header.getStartCapture()},
          >   End capture: ${header.getEndCapture()},
          >   Hostname: ${header.getHostname()},
          >   Comments: ${header.getComments()}
          > }
        """.stripMargin('>'))

      // Currently we cannot resolve version and proceed with parsing, we require pre-set version.
      require(actualVersion == elem.version,
        s"Expected version ${elem.version}, got ${actualVersion} for file ${elem.path}. " +
          "Scan of the files with different (compatible) versions, e.g. 5, 6, and 7 is not " +
          "supported currently")

      // Build record buffer based on resolved filter, if filter is not defined use default scan
      // with trivial predicate
      val recordBuffer = if (updatedFilter.nonEmpty) {
        reader.prepareRecordBuffer(internalColumns, updatedFilter.get)
      } else {
        reader.prepareRecordBuffer(internalColumns)
      }

      val rawIterator = new CloseableIterator[Array[Object]] {
        private var delegate = recordBuffer.iterator().asScala

        override def getNext(): Array[Object] = {
          // If delegate has traversed over all elements mark it as finished
          // to allow to close stream
          if (delegate.hasNext) {
            delegate.next
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
      Option(TaskContext.get).foreach(_.addTaskCompletionListener(_ => rawIterator.closeIfNeeded))

      // Try collecting statistics before any other mode, because attributes collect raw data. If
      // file exists, it is assumed that statistics are already written
      val writableIterator = if (updatedFilter.isEmpty && elem.statisticsPathStatus.nonEmpty &&
          statisticsIndex.nonEmpty) {
        val statStatus = elem.statisticsPathStatus.get
        if (!statStatus.exists) {
          logDebug(s"Prepare statistics for a path ${statStatus.path}")
          val attributes = AttributeBatch.create()
          new Iterator[Array[Object]] {
            override def hasNext: Boolean = {
              // If raw iterator does not have any elements we assume that it is EOF and write
              // statistics into a file
              // There is a feature in Spark when iterator is invoked once to get `hasNext`, and
              // then continue to extract records. For empty files, Spark will try to write
              // statistics twice, because of double invocation of `hasNext`, we overwrite old file
              if (!rawIterator.hasNext) {
                logInfo(s"Ready to write statistics for path: ${statStatus.path}")
                attributes.write(statStatus.path, conf, overwrite = true)
              }
              rawIterator.hasNext
            }

            override def next(): Array[Object] = {
              val arr = rawIterator.next
              for (i <- 0 until numColumns) {
                if (statisticsIndex.contains(i)) {
                  val key = statisticsIndex(i).internalColumn.getColumnName
                  attributes.updateStatistics(key, arr(i))
                }
              }
              arr
            }
          }
        } else {
          logDebug(s"Statistics file ${statStatus.path} already exists, skip writing")
          rawIterator
        }
      } else {
        logDebug("Statistics are disabled, skip writing")
        rawIterator
      }

      // Conversion iterator, applies defined modification for convertable fields
      val withConversionsIterator = if (applyConversion) {
        // For each field we check if possible conversion is available. If it is we apply direct
        // conversion, otherwise return unchanged value. Note that this should be in sync with
        // `applyConversion` and updated schema from `ResolvedInterface`.
        writableIterator.map(arr => {
          for (i <- 0 until numColumns) {
            resolvedColumns(i).convertFunction match {
              case Some(func) => arr(i) = func.direct(arr(i))
              case None => // do nothing
            }
          }
          arr
        })
      } else {
        writableIterator
      }

      buffer = buffer ++ withConversionsIterator
    }

    new InterruptibleIterator(context, new Iterator[SQLRow] {
      def next(): SQLRow = {
        SQLRow.fromSeq(buffer.next())
      }

      def hasNext: Boolean = {
        buffer.hasNext
      }
    })
  }
}
