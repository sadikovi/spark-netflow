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

class StatisticsReaderSuite extends UnitTestSpec {
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

  test("fail when input is null") {
    intercept[IllegalArgumentException] {
      new StatisticsReader(null, ByteOrder.BIG_ENDIAN)
    }
  }

  test("fail when byte order is null") {
    val input = fs.open(file)

    intercept[IllegalArgumentException] {
      new StatisticsReader(input, null)
    }
  }

  private def write(
    output: FSDataOutputStream,
    order: ByteOrder,
    options: Array[StatisticsOption]
  ): Unit = {
    try {
      val version: Short = 5
      val count: Long = 12654
      val writer = new StatisticsWriter(output, order)
      writer.write(version, count, options)
    } finally {
      output.close()
    }
  }

  test("read metadata file in LITTLE_ENDIAN") {
    val options: Array[StatisticsOption] = Array(
      StatisticsOption.forField(0x00000001L, 0.toByte, 12.toByte),
      StatisticsOption.forField(0x00000002L, 1.toShort, 13.toShort),
      StatisticsOption.forField(0x00000004L, 2, 255),
      StatisticsOption.forField(0x00000008L, 3, 1024.toLong))

    write(output, ByteOrder.LITTLE_ENDIAN, options)

    val input = fs.open(file)
    val reader = new StatisticsReader(input)
    val stats = reader.read()

    reader.order should be (ByteOrder.LITTLE_ENDIAN)
    stats.getVersion() should be (5)
    stats.getCount() should be (12654)

    stats.getOptions().length should be (options.length)

    stats.getOptions().foreach(opt => {
      if (opt.getField() == 0x00000001L) {
        opt.getSize() should be (1)
        opt.getMin() should be (0)
        opt.getMax() should be (12)
      } else if (opt.getField() == 0x00000002L) {
        opt.getSize() should be (2)
        opt.getMin() should be (1)
        opt.getMax() should be (13)
      } else if (opt.getField() == 0x00000004L) {
        opt.getSize() should be (4)
        opt.getMin() should be (2)
        opt.getMax() should be (255)
      } else if (opt.getField() == 0x00000008L) {
        opt.getSize() should be (8)
        opt.getMin() should be (3)
        opt.getMax() should be (1024)
      }
    })
  }

  test("read metadata in BIG_ENDIAN") {
    val options: Array[StatisticsOption] = Array(
      StatisticsOption.forField(0x00000001L, 0.toByte, 12.toByte),
      StatisticsOption.forField(0x00000002L, 1.toShort, 13.toShort),
      StatisticsOption.forField(0x00000004L, 2, 255),
      StatisticsOption.forField(0x00000008L, 3, 1024.toLong))

    write(output, ByteOrder.BIG_ENDIAN, options)

    val input = fs.open(file)
    val reader = new StatisticsReader(input)
    val stats = reader.read()

    reader.order should be (ByteOrder.BIG_ENDIAN)
    stats.getVersion() should be (5)
    stats.getCount() should be (12654)

    stats.getOptions().length should be (options.length)

    stats.getOptions().foreach(opt => {
      if (opt.getField() == 0x00000001L) {
        opt.getSize() should be (1)
        opt.getMin() should be (0)
        opt.getMax() should be (12)
      } else if (opt.getField() == 0x00000002L) {
        opt.getSize() should be (2)
        opt.getMin() should be (1)
        opt.getMax() should be (13)
      } else if (opt.getField() == 0x00000004L) {
        opt.getSize() should be (4)
        opt.getMin() should be (2)
        opt.getMax() should be (255)
      } else if (opt.getField() == 0x00000008L) {
        opt.getSize() should be (8)
        opt.getMin() should be (3)
        opt.getMax() should be (1024)
      }
    })
  }

  test("read metadata with no options") {
    write(output, ByteOrder.BIG_ENDIAN, Array())

    val input = fs.open(file)
    val reader = new StatisticsReader(input)
    val stats = reader.read()

    reader.order should be (ByteOrder.BIG_ENDIAN)
    stats.getVersion() should be (5)
    stats.getCount() should be (12654)

    stats.getOptions().length should be (0)
  }
}
