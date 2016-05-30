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
import java.nio.ByteOrder
import java.util.zip.Inflater

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import io.netty.buffer.{ByteBuf, Unpooled}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.spark.{SparkContext, Partition, TaskContext, InterruptibleIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row => SQLRow}
import org.apache.spark.sql.sources._

import com.github.sadikovi.netflowlib.NetFlowReader
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate
import com.github.sadikovi.netflowlib.util.ReadAheadInputStream
import com.github.sadikovi.spark.netflow.NetFlowFilters
import com.github.sadikovi.spark.netflow.index.AttributeMap
import com.github.sadikovi.spark.netflow.sources._

/** For codegen only */
private[spark] class NetFlowFileRDD2[T <: SQLRow : ClassTag] (
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
    var globalIterator: Iterator[Array[Object]] = Iterator.empty

    for (elem <- s.asInstanceOf[NetFlowFilePartition[NetFlowFileStatus]].iterator) {
      // Check if statistics file status is available. Only read statistics, if filter is provided
      val updatedFilter = if (resolvedFilter.nonEmpty && elem.statisticsPathStatus.nonEmpty) {
        val statStatus = elem.statisticsPathStatus.get
        if (statStatus.exists) {
          logDebug("Applying statistics to update filter")
          val attributes = AttributeMap.read(statStatus.path, conf)
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
      val stm: FSDataInputStream = fs.open(path)
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


      // Build raw iterator for codegen
      // Fetching columns:
      // "unix_secs", "srcip", "dstip", "srcport", "dstport", "protocol", "octets"
      // Filtering: $"srcport" === 443 || $"srcip" === "74.125.237.221"
      val recordSize: Int = 64 // for NetFlow v5
      var numBytesRead: Int = 0
      var primary: Array[Byte] = new Array[Byte](recordSize)
      var secondary: Array[Byte] = new Array[Byte](recordSize)
      var buffer: ByteBuf = Unpooled.wrappedBuffer(primary).order(ByteOrder.LITTLE_ENDIAN)
      val compression = true
      val inflater = new Inflater()
      val bufferLength = elem.bufferSize
      val stream = new ReadAheadInputStream(stm, inflater, bufferLength)

      globalIterator = globalIterator ++ new Iterator[Array[Object]] {
        def ip(value: Any): String = {
          val num = value.asInstanceOf[Long]
          s"${(num & 4278190080L) >> 24}.${(num & 16711680L) >> 16}." +
            s"${(num & 65280L) >> 8}.${num & 255}"
        }

        def protocol(value: Any): String = {
          val num = value.asInstanceOf[Short]
          if (num == 1) "ICMP"
          else if (num == 3) "GGP"
          else if (num == 6) "TCP"
          else if (num == 8) "EGP"
          else if (num == 12) "PUP"
          else if (num == 17) "UDP"
          else if (num == 20) "HMP"
          else if (num == 27) "RDP"
          else if (num == 46) "RSVP"
          else if (num == 47) "GRE"
          else if (num == 50) "ESP"
          else if (num == 51) "AH"
          else if (num == 66) "AH"
          else if (num == 88) "IGMP"
          else if (num == 89) "OSPF"
          else value.toString
        }

        override def hasNext: Boolean = {
          var hasNext = true
          try {
            hasNext = stream.available() > 0
          } catch {
            case ioe: IOException =>
              hasNext = false
          } finally {
            if (!hasNext) {
              stream.close()
              if (buffer != null && buffer.refCnt() > 0) {
                buffer.release(buffer.refCnt())
              }
              buffer = null
            }
          }

          hasNext
        }

        override def next(): Array[Object] = {
          try {
            numBytesRead = stream.read(primary, 0, recordSize)
            if (numBytesRead < 0) {
              throw new IOException(s"EOF, $numBytesRead bytes read")
            } else if (numBytesRead < recordSize) {
              if (!compression) {
                throw new IllegalArgumentException(
                  s"Failed to read record: $numBytesRead < $recordSize")
              } else {
                val remaining = recordSize - numBytesRead
                val addBytes = stream.read(secondary, 0, remaining)
                if (addBytes != remaining) {
                  throw new IllegalArgumentException(
                    s"Failed to read record: $addBytes != $remaining")
                }
                System.arraycopy(secondary, 0, primary, numBytesRead, remaining)
              }
            }
          } catch {
            case ioe: IOException =>
              throw new IllegalArgumentException("Unexpected EOF", ioe)
          }

          // Process record
          val srcip: Long = buffer.getUnsignedInt(16)
          val srcport: Int = buffer.getUnsignedShort(48)
          if (srcip == 1249766877L || srcport == 443) {
            val newRecord: Array[Object] = new Array[Object](7)
            newRecord(0) = buffer.getUnsignedInt(0).asInstanceOf[java.lang.Long] // unix_secs
            newRecord(1) = ip(buffer.getUnsignedInt(16)) // srcip
            newRecord(2) = ip(buffer.getUnsignedInt(20)) // dstip
            newRecord(3) = buffer.getUnsignedShort(48).asInstanceOf[java.lang.Integer] // srcport
            newRecord(4) = buffer.getUnsignedShort(50).asInstanceOf[java.lang.Integer] // dstport
            newRecord(5) = protocol(buffer.getUnsignedByte(52)) // protocol
            newRecord(6) = buffer.getUnsignedInt(36).asInstanceOf[java.lang.Long] // octets
            // return new record
            newRecord
          } else {
            null
          }
        }
      }
    }

    new Iterator[SQLRow] {
      var found: Boolean = false
      var foundItem: Array[Object] = null

      def next(): SQLRow = {
        if (!found) {
          throw new NoSuchElementException(
            "Either iterator is empty, or iterator state has not been updated")
        }

        if (foundItem == null) {
          throw new IllegalStateException(s"Potential out of sync error in ${getClass.getName}")
        }

        found = false
        SQLRow.fromSeq(foundItem)
      }

      def hasNext: Boolean = {
        while (!found && globalIterator.hasNext) {
          foundItem = globalIterator.next()
          if (foundItem != null) {
            found = true
          }
        }

        found
      }
    }
  }
}
