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

package com.github.sadikovi.spark.netflow

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.SparkException
import org.apache.spark.sql.{SQLContext, Row}

import org.scalatest.ConfigMap

import com.github.sadikovi.netflowlib.RecordBuffer
import com.github.sadikovi.testutil.{UnitTestSpec, SparkLocal}

class NetflowSuite extends UnitTestSpec with SparkLocal {
  override def beforeAll(configMap: ConfigMap) {
    startSparkContext()
  }

  override def afterAll(configMap: ConfigMap) {
    stopSparkContext()
  }

  val path1 = getClass().getResource("/correct/ftv5.2016-01-13.nocompress.bigend.sample").getPath
  val path2 = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath
  val path3 = getClass().getResource("/corrupt/ftv5.2016-01-13.compress.9.sample-01").getPath
  val path4 = getClass().getResource("/corrupt/ftv5.2016-01-13.compress.9.sample-00").getPath
  val path5 = getClass().getResource("/unsupport/ftv8.2016-01-17.compress.7.bigend.sample").getPath

  test("read uncompressed v5 format") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      load(s"file:${path1}")

    val res = df.collect()
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0,
      65280L, 17, 0, 0, 0, 0, 0, 0, 0, 65280))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 231, 0, 0, 0, 0, 0, 999, 743))
  }

  test("read compressed v5 format") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      load(s"file:${path2}")

    val res = df.collect()
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0,
      65280L, 17, 0, 0, 0, 0, 0, 0, 0, 65280))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 231, 0, 0, 0, 0, 0, 999, 743))
  }

  test("fail reading invalid input") {
    val sqlContext = new SQLContext(sc)
    try {
      sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
        load(s"file:${path3}").count()
    } catch {
      case se: SparkException =>
        val msg = se.getMessage()
        assert(msg.contains("java.lang.UnsupportedOperationException: " +
          "Corrupt Netflow file. Wrong magic number"))
      case other: Throwable => throw other
    }
  }

  test("fail to read data of corrupt file") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      load(s"file:${path4}")
    try {
      df.select("srcip").count()
    } catch {
      case se: SparkException =>
        val msg = se.getMessage()
        assert(msg.contains("java.lang.IllegalArgumentException: Unexpected EOF"))
      case other: Throwable => throw other
    }
  }

  test("fail to read unsupported version 8") {
    val sqlContext = new SQLContext(sc)
    intercept[UnsupportedOperationException] {
      sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "8").
        load(s"file:${path4}")
    }
  }

  test("issue #5 - prune only one column when running cound directly") {
    val sqlContext = new SQLContext(sc)
    val relation = new NetflowRelation(Array(path1), None, None, Map("version" -> "5"))(sqlContext)

    val path = new Path(path1)
    val fileStatus = path.getFileSystem(new Configuration(false)).getFileStatus(path)
    val rdd = relation.buildScan(Array.empty, Array(fileStatus))
    // should be only one column (unix_secs) which is "0" for generated data
    rdd.first should be (Row(0))
  }

  test("issue #6 - test buffer size") {
    val sqlContext = new SQLContext(sc)
    // check that buffer size is default
    var params = Map("version" -> "5")
    var relation = new NetflowRelation(Array(path1), None, None, params)(sqlContext)
    relation.getBufferSize() should be (RecordBuffer.BUFFER_LENGTH_1)

    // set buffer size to be 10Kb
    params = Map("version" -> "5", "buffer" -> "10Kb")
    relation = new NetflowRelation(Array(path1), None, None, params)(sqlContext)
    relation.getBufferSize() should be (10 * 1024)

    // buffer size >> Integer.MAX_VALUE
    intercept[RuntimeException] {
      params = Map("version" -> "5", "buffer" -> "10Gb")
      relation = new NetflowRelation(Array(path1), None, None, params)(sqlContext)
    }

    // just for completeness, test on wrong buffer value
    intercept[NumberFormatException] {
      params = Map("version" -> "5", "buffer" -> "wrong")
      relation = new NetflowRelation(Array(path1), None, None, params)(sqlContext)
    }
  }
}
