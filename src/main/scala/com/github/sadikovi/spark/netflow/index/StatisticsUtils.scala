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

import java.nio.ByteOrder
import java.nio.charset.Charset

import io.netty.buffer.ByteBuf

object StatisticsUtils {
  // Magic numbers as first bytes in a file
  val MAGIC_1: Short = 0xCA
  val MAGIC_2: Short = 0x11

  // Size of initial state in bytes
  val INITIAL_STATE_SIZE: Int = 3

  // Type indices of internal containers for attribute
  val TYPE_COUNT: Byte = 1
  val TYPE_MINMAX: Byte = 2
  val TYPE_SET: Byte = 4

  // Default charset for string conversion
  val DEFAULT_CHARSET: String = "UTF-8"

  /** Convert endianness into byte */
  def getEndiannessIndex(endianness: ByteOrder): Byte = {
    if (endianness == ByteOrder.LITTLE_ENDIAN) 1 else 2
  }

  /** Convert byte into endianness */
  def getEndianness(index: Byte): ByteOrder = {
    if (index == 1) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN
  }

  /** Write bytes and verify number of bytes written */
  def withBytes(buffer: ByteBuf, numBytes: Int)(func: ByteBuf => Unit): Unit = {
    val begin = buffer.writerIndex
    val result = func(buffer)
    val end = buffer.writerIndex
    require(end - begin == numBytes,
      s"Written ${end - begin} bytes does not equal to $numBytes bytes")
  }

  /** Get number of bytes for class, only supports numeric classes and strings for now */
  def getBytes(klass: Class[_]): Byte = {
    if (klass == classOf[Byte]) {
      return 1
    } else if (klass == classOf[Short]) {
      return 2
    } else if (klass == classOf[Int]) {
      return 4
    } else if (klass == classOf[Long]) {
      return 8
    } else if (klass == classOf[String]) {
      return 32
    } else {
      sys.error(s"Unsuppored type $klass")
    }
  }

  /** Get class based on number of bytes */
  def getClassTag(numBytes: Byte): Class[_] = numBytes match {
    case 1 => classOf[Byte]
    case 2 => classOf[Short]
    case 4 => classOf[Int]
    case 8 => classOf[Long]
    case 32 => classOf[String]
    case other => sys.error(s"Unsupported number of bytes $numBytes")
  }

  /** Write value based on a provided class */
  def writeValue(buffer: ByteBuf, value: Any, klass: Class[_]): Unit = {
    if (klass == classOf[Byte]) {
      buffer.writeByte(value.asInstanceOf[Byte].toInt)
    } else if (klass == classOf[Short]) {
      buffer.writeShort(value.asInstanceOf[Short].toInt)
    } else if (klass == classOf[Int]) {
      buffer.writeInt(value.asInstanceOf[Int])
    } else if (klass == classOf[Long]) {
      buffer.writeLong(value.asInstanceOf[Long])
    } else if (klass == classOf[String]) {
      val chars = value.asInstanceOf[String].getBytes(Charset.forName(DEFAULT_CHARSET))
      buffer.writeInt(chars.length)
      buffer.writeBytes(chars)
    } else {
      sys.error(s"Unsupported field type $klass")
    }
  }

  /** Read value based on provided class */
  def readValue(buffer: ByteBuf, klass: Class[_]): Any = {
    if (klass == classOf[Byte]) {
      buffer.readByte()
    } else if (klass == classOf[Short]) {
      buffer.readShort()
    } else if (klass == classOf[Int]) {
      buffer.readInt()
    } else if (klass == classOf[Long]) {
      buffer.readLong()
    } else if (klass == classOf[String]) {
      val length = buffer.readInt()
      val chars = new Array[Byte](length)
      buffer.readBytes(chars)
      new String(chars, Charset.forName(DEFAULT_CHARSET))
    } else {
      sys.error(s"Unsupported field type $klass")
    }
  }

  /** Convert java class to closely matched scala class, otherwise return itself */
  def javaToScala(klass: Class[_]): Class[_] = {
    if (klass == classOf[java.lang.Byte]) {
      classOf[Byte]
    } else if (klass == classOf[java.lang.Short]) {
      classOf[Short]
    } else if (klass == classOf[java.lang.Integer]) {
      classOf[Int]
    } else if (klass == classOf[java.lang.Long]) {
      classOf[Long]
    } else {
      klass
    }
  }

  /**
   * Compare classes and return true, if both classes match either directly or with conversion,
   * otherwise false
   */
  def softCompare(klass1: Class[_], klass2: Class[_]): Boolean = {
    klass1 == klass2 || javaToScala(klass1) == javaToScala(klass2)
  }
}
