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

package com.github.sadikovi.netflowlib

import java.nio.ByteOrder

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import com.github.sadikovi.testutil.UnitTestSpec

class NetFlowReaderSuite extends UnitTestSpec {
  private val baseDirectoryPath = new Path(baseDirectory())
  private val fs = baseDirectoryPath.getFileSystem(new Configuration(false))

  private val validFile1 = getClass().
    getResource("/correct/ftv5.2016-01-13.nocompress.bigend.sample").getPath()
  private val validFile2 = getClass().
    getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath()
  private val corruptFile = getClass().
    getResource("/corrupt/ftv5.2016-01-13.compress.9.sample-01").getPath()

  private val validPath1 = new Path(validFile1)
  private val validPath2 = new Path(validFile2)
  private val corruptPath = new Path(corruptFile)

  test("test metadata parsing") {
    // read uncompressed file with Big Endian byte order
    var stm = fs.open(validPath1)
    var reader = new NetFlowReader(stm)
    reader.getStreamVersion() should be (3)
    reader.getByteOrder() should be (ByteOrder.BIG_ENDIAN)

    // read uncompressed file with Little Endian byte order
    stm = fs.open(validPath2)
    reader = new NetFlowReader(stm)
    reader.getStreamVersion() should be (3)
    reader.getByteOrder() should be (ByteOrder.LITTLE_ENDIAN)

    stm = fs.open(corruptPath)
    intercept[UnsupportedOperationException] {
      new NetFlowReader(stm)
    }
  }

  test("test header parsing") {
    var stm = fs.open(validPath2)
    var reader = new NetFlowReader(stm)
    var header = reader.readHeader()

    header.getStreamVersion() should be (3)
    header.getHeaderSize() should be (48)
    header.getFlowVersion() should be (5)
    header.getHeaderFlags() should be (10)
    header.getHostname() should be ("flow-gen")
    header.getComments() should be ("flow-gen")
    (header.getFields() & NetFlowHeader.HEADER_FLAG_COMPRESS) should be (
      NetFlowHeader.HEADER_FLAG_COMPRESS)
  }

  test("test header compressed flag") {
    var stm = fs.open(validPath2)
    var reader = new NetFlowReader(stm)
    var header = reader.readHeader()

    header.isCompressed() should be (
      (header.getFields() & NetFlowHeader.HEADER_FLAG_COMPRESS) > 0)
  }

  test("test data parsing") {
    var stm = fs.open(validPath2)

    var reader = new NetFlowReader(stm)
    var header = reader.readHeader()
    var fields = Array(1L)
    val recordBuffer = reader.readData(header, fields, 64)
    recordBuffer.iterator().hasNext should be (true)
  }

  test("reading of a column specified twice in fields") {
    var stm = fs.open(validPath1)

    var reader = new NetFlowReader(stm)
    var header = reader.readHeader()
    // reading src ip address twice
    var fields = Array(0x00000010L, 0x00000010L)
    val recordBuffer = reader.readData(header, fields, 64)

    // read all records, ensure that there are two fields and they have the same columns
    val iter = recordBuffer.iterator()
    var numRecords = 0

    while (iter.hasNext) {
      val cols = iter.next()
      cols.length should be (2)
      assert(cols(0) == cols(1))

      numRecords += 1
    }

    numRecords should be (1000)
  }
}
