package com.github.sadikovi.netflow

import java.nio.ByteOrder;

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration

import com.github.sadikovi.testutil.UnitTestSpec

class NetflowReaderSuite extends UnitTestSpec {
  private val validFile1 = getClass().
    getResource("/correct/ftv5.2016-01-13.nocompress.bigend.sample").getPath()
  private val validFile2 = getClass().
    getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath()
  private val corruptFile = getClass().
    getResource("/corrupt/ftv5.2016-01-13.compress.9.sample-01").getPath()

  test("test metadata parsing") {
    // read uncompressed file with Big Endian byte order
    var path = new Path(validFile1)
    var fs = path.getFileSystem(new Configuration(false))
    var stm = fs.open(path)
    var reader = new NetflowReader(stm)
    reader.getStreamVersion() should be (3)
    reader.getByteOrder() should be (ByteOrder.BIG_ENDIAN)

    // read uncompressed file with Little Endian byte order
    path = new Path(validFile2)
    fs = path.getFileSystem(new Configuration(false))
    stm = fs.open(path)
    reader = new NetflowReader(stm)
    reader.getStreamVersion() should be (3)
    reader.getByteOrder() should be (ByteOrder.LITTLE_ENDIAN)

    path = new Path(corruptFile)
    fs = path.getFileSystem(new Configuration(false))
    stm = fs.open(path)
    intercept[UnsupportedOperationException] {
      new NetflowReader(stm)
    }
  }

  test("test header parsing") {
    var path = new Path(validFile2)
    var fs = path.getFileSystem(new Configuration(false))
    var stm = fs.open(path)
    var reader = new NetflowReader(stm)
    var header = reader.readHeader()

    header.getStreamVersion() should be (3)
    header.getHeaderSize() should be (48)
    header.getFlowVersion() should be (5)
    header.getHeaderFlags() should be (10)
    header.getHostname() should be ("flow-gen")
    header.getComments() should be ("flow-gen")
    (header.getFields() & NetflowHeader.HEADER_FLAG_COMPRESS) should be (
      NetflowHeader.HEADER_FLAG_COMPRESS)
  }

  test("test data parsing") {
    var path = new Path(validFile2)
    var fs = path.getFileSystem(new Configuration(false))
    var stm = fs.open(path)

    var reader = new NetflowReader(stm)
    var header = reader.readHeader()
    var fields = Array(1L)
    val recordBuffer = reader.readData(header, fields, 64)
    recordBuffer.iterator().hasNext should be (true)
  }
}
