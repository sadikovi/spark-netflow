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

package com.github.sadikovi.spark.netflow.index

import java.io.OutputStream
import java.nio.ByteOrder

import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._

import io.netty.buffer.{ByteBuf, Unpooled}

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{Path => HadoopPath}

/**
 * [[StatisticsWriter]] writes sequence of attributes into a file provided. Note that only numeric
 * types of attributes are supported. `OutputStream` is handled and closed automatically, with
 * underlying byte buffer.
 */
private[spark] class StatisticsWriter(
    private val endianness: ByteOrder,
    private val attrs: Seq[Attribute[_]]) {
  var buffer: ByteBuf = Unpooled.buffer().order(endianness)
  init()

  /** Write initial state: magic numbers, endianness */
  private def init(): Unit = {
    StatisticsUtils.withBytes(buffer, StatisticsUtils.INITIAL_STATE_SIZE) { buf =>
      buf.writeByte(StatisticsUtils.MAGIC_1)
      buf.writeByte(StatisticsUtils.MAGIC_2)
      buf.writeByte(StatisticsUtils.getEndiannessIndex(endianness))
    }
  }

  /** Write attribute header (flags, name, etc). Exposed to a package for testing purposes */
  private[index] def writeAttributeHeader(
      klass: Class[_], flags: Byte, key: String, hasNull: Boolean): Unit = {
    buffer.writeByte(StatisticsUtils.MAGIC_1)
    buffer.writeByte(StatisticsUtils.MAGIC_2)
    buffer.writeByte(flags)
    buffer.writeByte(StatisticsUtils.getBytes(klass))
    buffer.writeBoolean(hasNull)
    // Bytes of the key based on charset, this will write length as integer and array of bytes
    StatisticsUtils.writeValue(buffer, key, classOf[String])
  }

  /** Generic write method for arbitrary sequence of elements */
  private def writeElements(
      tpe: Byte,
      klass: Class[_], size: Int, elems: GenTraversableOnce[_], hasNull: Boolean): Unit = {
    buffer.writeByte(tpe)
    buffer.writeByte(StatisticsUtils.getBytes(klass))
    buffer.writeInt(size)
    val iter = elems.toIterator
    while (iter.hasNext) {
      val elem = iter.next
      if (hasNull) {
        buffer.writeBoolean(elem == null)
      }

      if (!hasNull || elem != null) {
        StatisticsUtils.writeValue(buffer, elem, klass)
      }
    }
  }

  private def writeElements(
      tpe: Byte, klass: Class[_], elems: GenTraversableOnce[_], hasNull: Boolean): Unit = {
    writeElements(tpe, klass, elems.size, elems, hasNull)
  }

  /** Internal method to write single attribute. Exposed to a package for testing purposes */
  private[index] def writeAttribute(attr: Attribute[_]): Unit = {
    val hasNull = attr.containsNull()
    writeAttributeHeader(attr.getClassTag(), attr.flags, attr.name, hasNull)
    // Attribute "count" parameter is always of long type regardless of runtime class
    attr.getCount() match {
      case Some(count) =>
        writeElements(StatisticsUtils.TYPE_COUNT, classOf[Long], Seq[Long](count), hasNull)
      case None => // do nothing
    }

    attr.getMinMax() match {
      case Some((min, max)) =>
        writeElements(StatisticsUtils.TYPE_MINMAX, attr.getClassTag(), Seq(min, max), hasNull)
      case None => // do nothing
    }

    attr.getSet() match {
      case Some(set) =>
        writeElements(StatisticsUtils.TYPE_SET, attr.getClassTag(), set.size(),
          set.iterator.asScala, hasNull)
      case None => // do nothing
    }
  }

  /** Internal save method, write each attribute and transfer bytes into output stream */
  private def internalSave(out: OutputStream): Unit = {
    for (attr <- attrs) {
      writeAttribute(attr)
    }
    buffer.getBytes(0, out, buffer.writerIndex)
  }

  /** Close underlying buffer */
  private def close(): Unit = try {
    buffer.release(buffer.refCnt)
  } finally {
    buffer = null
  }

  /** Save provided attributes into a file */
  def save(path: String, conf: HadoopConf = new HadoopConf(true)): Unit = {
    val pt = new HadoopPath(path)
    val fs = pt.getFileSystem(conf)
    val out = fs.create(pt, false)
    internalSave(out)
    out.close()
    close()
  }
}
