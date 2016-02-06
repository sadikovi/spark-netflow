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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.spark.{SparkContext, Partition, TaskContext, InterruptibleIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row => SQLRow}

import com.github.sadikovi.netflowlib.{NetflowReader, NetflowHeader, RecordBuffer}
import com.github.sadikovi.netflowlib.version.NetflowV5

/** Netflow metadata includes path to the file and columns to fetch */
private[spark] case class NetflowMetadata(
  version: Short,
  path: String,
  fields: Array[Long],
  bufferSize: Int,
  conversions: Map[Int, AnyVal => String],
  maybeMetadata: Option[String]
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
 * SQL rows back. Used internally solely for the purpose of datasource API.
 */
private[spark] class NetflowFileRDD[T<:SQLRow: ClassTag] (
    @transient sc: SparkContext,
    @transient data: Seq[NetflowMetadata],
    numSlices: Int) extends FileRDD[SQLRow](sc, Nil) {

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

  override def getPartitions: Array[Partition] = {
    val slices = this.slice(data, numSlices).toArray
    slices.indices.map(i => new NetflowFilePartition[NetflowMetadata](id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[SQLRow] = {
    val conf = getConf()
    var buffer: Iterator[Array[Object]] = Iterator.empty

    for (elem <- s.asInstanceOf[NetflowFilePartition[NetflowMetadata]].iterator) {
      // reconstruct file path
      val path = new Path(elem.path)
      val fs = path.getFileSystem(conf)
      val cSummary = fs.getContentSummary(path)
      val FILE_LENGTH = cSummary.getLength()
      val stm: FSDataInputStream = fs.open(path)

      // build Netflow reader and check whether it can be read
      val nr = new NetflowReader(stm)
      val hr = nr.readHeader()
      // actual version of the file
      val actualVersion = hr.getFlowVersion()
      // fields to extract
      val fields = elem.fields
      // conversion to apply
      val conversions = elem.conversions
      // compression flag is second bit in header flags
      val isCompressed = (hr.getHeaderFlags() & 0x2) > 0

      // currently we cannot resolve version and proceed with parsing, we require pre-set version.
      require(actualVersion == elem.version,
        s"Expected version ${elem.version}, got ${actualVersion}")

      logInfo(s"""
        > Netflow: {
        >   File: ${elem.path}
        >   File length: ${FILE_LENGTH}
        >   Flow version: ${actualVersion}
        >   Compression: ${isCompressed}
        >   Buffer size: ${elem.bufferSize}
        >   Hostname: ${hr.getHostname()}
        >   Comments: ${hr.getComments()}
        > }
      """.stripMargin('>'))

      val recordBuffer = nr.readData(hr, fields, elem.bufferSize)
      val iterator = if (conversions.isEmpty) {
        recordBuffer.iterator().asScala
      } else {
        // for each array of fields we check if current field matches list of possible conversions,
        // and convert, otherwise return unchanged field.
        // do not forget to check field constant index to remove overlap with indices from other
        // versions
        recordBuffer.iterator().asScala.map(arr =>
          arr.zipWithIndex.map { case (value, index) => conversions.get(index) match {
            case Some(func) => func(value.asInstanceOf[AnyVal])
            case None => value
          } }
        )
      }

      buffer = buffer ++ iterator
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
