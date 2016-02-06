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

package com.github.sadikovi.netflowlib.statistics

import java.io.{OutputStream, IOException}
import java.nio.ByteOrder

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, FSDataOutputStream}

import com.github.sadikovi.testutil.UnitTestSpec

class StatisticsWriterSuite extends UnitTestSpec {
  lazy val conf: Configuration = new Configuration(false)
  lazy val file: Path = new Path(s"file:${targetDirectory()}/_temporary.dat")
  lazy val fs: FileSystem = file.getFileSystem(conf)
  var output: FSDataOutputStream = null

  before {
    output = fs.create(file, true)
  }

  after {
    if (output != null) {
      output.close()
    }
    fs.delete(file, false)
  }

  test("fail when output is null") {
    intercept[IllegalArgumentException] {
      new StatisticsWriter(null, ByteOrder.BIG_ENDIAN)
    }
  }

  test("fail when byte order is null") {
    intercept[IllegalArgumentException] {
      new StatisticsWriter(output, null)
    }
  }

  test("fail when stream state is wrong") {
    output.writeByte(1)

    intercept[IOException] {
      new StatisticsWriter(output, ByteOrder.BIG_ENDIAN)
    }
  }

  test("write metadata for null options") {
    val writer = new StatisticsWriter(output, ByteOrder.BIG_ENDIAN)

    // fail for null array of options
    intercept[IllegalArgumentException] {
      writer.write(1, 100, null)
    }
  }

  test("write metadata for empty options") {
    val version: Short = 1
    val count: Long = 100
    val options: Array[StatisticsOption] = new Array(0)
    val writer = new StatisticsWriter(output, ByteOrder.BIG_ENDIAN)

    val numBytes = writer.write(version, count, options)
    val summary = fs.getContentSummary(file)
    val fileLength = summary.getLength()

    // metadata 3 bytes, and header 10 bytes + header length 4 bytes, length of content 8 bytes
    numBytes should be (25)
    fileLength should be (numBytes)
  }

  test("write metadata for non-empty options") {
    val version: Short = 1
    val count: Long = 100
    val options: Array[StatisticsOption] = Array(
      StatisticsOption.forField(0x00000001L, 0.toByte, Byte.MaxValue),
      StatisticsOption.forField(0x00000002L, 1.toShort, Short.MaxValue),
      StatisticsOption.forField(0x00000004L, 2, Int.MaxValue),
      StatisticsOption.forField(0x00000008L, 3, Long.MaxValue))
    val writer = new StatisticsWriter(output, ByteOrder.BIG_ENDIAN)

    val numBytes = writer.write(version, count, options)
    val summary = fs.getContentSummary(file)
    val fileLength = summary.getLength()

    // metadata 3 bytes, and header 10 bytes + header length 4 bytes, length of content 8 bytes
    // 25 + [Option 1](4 + 2 + 1 + 1) + [Option 2](4 + 2 + 2 + 2) + [Option 3](4 + 2 + 4 + 4) +
    // [Option 4](4 + 2 + 8 + 8)
    numBytes should be (79)
    fileLength should be (numBytes)
  }

  test("write metadata using Statistics") {
    val version: Short = 1
    val count: Long = 100
    val options: Array[StatisticsOption] = Array(
      StatisticsOption.forField(0x00000001L, 0.toByte, Byte.MaxValue),
      StatisticsOption.forField(0x00000002L, 1.toShort, Short.MaxValue),
      StatisticsOption.forField(0x00000004L, 2, Int.MaxValue),
      StatisticsOption.forField(0x00000008L, 3, Long.MaxValue))
    val writer = new StatisticsWriter(output, ByteOrder.BIG_ENDIAN)

    val numBytes = writer.write(new Statistics(version, count, options))
    val summary = fs.getContentSummary(file)
    val fileLength = summary.getLength()

    numBytes should be (79) // see explanation above
    fileLength should be (numBytes)
  }
}
