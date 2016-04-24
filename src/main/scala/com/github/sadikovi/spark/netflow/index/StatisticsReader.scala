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

import java.io.InputStream

import scala.collection.mutable.ArrayBuffer

import io.netty.buffer.{ByteBuf, Unpooled}

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{Path => HadoopPath}

/**
 * [[StatisticsReader]] reads bytes from file provided and converts them into sequence of
 * parsed attributes. Note that type inference does not apply, so sequence of arbitrary attributes
 * is returned.
 */
private[spark] class StatisticsReader {
  var buffer: ByteBuf = null

  private def checkMagicNumbers(magic1: Short, magic2: Short): Unit = {
    require(magic1 == StatisticsUtils.MAGIC_1 && magic2 == StatisticsUtils.MAGIC_2,
      "Wrong magic number")
  }

  /** Verify that buffer bytes are correct */
  private def verify(buffer: ByteBuf): Unit = {
    val magic1 = buffer.readUnsignedByte()
    val magic2 = buffer.readUnsignedByte()
    checkMagicNumbers(magic1, magic2)
  }

  private[index] def getAttributeParams(buffer: ByteBuf): (Byte, Class[_], Boolean, String) = {
    verify(buffer)
    val flags = buffer.readByte()
    val klass = StatisticsUtils.getClassTag(buffer.readByte())
    val hasNull = buffer.readBoolean()
    val name = StatisticsUtils.readValue(buffer, classOf[String]).asInstanceOf[String]

    (flags, klass, hasNull, name)
  }

  /** Read property, and return type of property, class of values, and temporary buffer */
  private[index] def readProperty(buffer: ByteBuf, hasNull: Boolean): (Byte, Iterator[Any]) = {
    // Read property header
    val tpe = buffer.readByte()
    val classBytes = buffer.readByte()
    var size = buffer.readInt()
    val klass = StatisticsUtils.getClassTag(classBytes)
    // Temporary cache for values
    val temp = new ArrayBuffer[Any]()
    // Read values sequentially
    while (size > 0) {
      if (hasNull) {
        val isNullByte = buffer.readBoolean()
        if (isNullByte) {
          temp.append(null)
        } else {
          temp.append(StatisticsUtils.readValue(buffer, klass))
        }
      } else {
        temp.append(StatisticsUtils.readValue(buffer, klass))
      }
      size -= 1
    }

    (tpe, temp.toIterator)
  }

  private[index] def getAttribute(buffer: ByteBuf): Attribute[_<:Any] = {
    val params = getAttributeParams(buffer)
    val flags = params._1
    val klass = params._2
    val hasNull = params._3
    val name = params._4

    val attr = Attribute(name, flags, klass)
    // We cannot use `containsNull` when reading property, because it will account for min/max,
    // which are null for empty attribute
    attr.setNull(hasNull)
    for (anyTpe <- 0 until attr.numStatistics()) {
      val properties = readProperty(buffer, hasNull)
      val tpe = properties._1
      val iter = properties._2
      attr.setStatistics(tpe, iter)
    }

    attr
  }

  /** Internal loading method based on input stream */
  private[index] def internalLoad(in: InputStream): Seq[Attribute[_]] = {
    // Read file header and verify magic numbers, and resolve byte order
    val array = new Array[Byte](StatisticsUtils.INITIAL_STATE_SIZE)
    in.read(array)
    buffer = Unpooled.wrappedBuffer(array)
    verify(buffer)
    val endianness = StatisticsUtils.getEndianness(buffer.readByte())
    buffer.release(buffer.refCnt)

    // Fill up buffer, note we do not know the size of input stream in general case, e.g. if stream
    // is compressed, therefore fill buffer in chunks of its own capacity
    buffer = Unpooled.buffer().order(endianness)
    while (in.available > 0) {
      buffer.writeBytes(in, buffer.capacity)
    }

    // In order to know how many attributes we have in input stream, we use a trick that writer
    // index of byte buffer points to the last written byte, which is different from its capacity.
    // When everything is okay and file is written correctly, then reader index should be equal
    // writer index by the end of parsing
    val attributesBuffer = new ArrayBuffer[Attribute[_]]()
    while (buffer.readerIndex < buffer.writerIndex) {
      attributesBuffer.append(getAttribute(buffer))
    }

    attributesBuffer.toSeq
  }

  /** Release underlying byte buffer */
  private def close(): Unit = try {
    buffer.release(buffer.refCnt)
  } finally {
    buffer = null
  }

  /** Load provided path and return sequence of attributes */
  def load(path: String): Seq[Attribute[_]] = {
    val pt = new HadoopPath(path)
    val fs = pt.getFileSystem(new HadoopConf(true))
    val in = fs.open(pt)
    val attributes = try {
      internalLoad(in)
    } finally {
      close()
      in.close()
    }
    attributes
  }
}
