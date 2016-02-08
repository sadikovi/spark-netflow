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
import org.apache.hadoop.fs.{Path, FileSystem, FileStatus}

import org.apache.spark.SparkException
import org.apache.spark.sql.{SQLContext, DataFrame, Row}

import org.scalatest.ConfigMap

import com.github.sadikovi.netflowlib.RecordBuffer
import com.github.sadikovi.netflowlib.version.NetflowV5
import com.github.sadikovi.spark.netflow.sources.MapperV5
import com.github.sadikovi.spark.rdd.NetflowFileRDD
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

  test("return empty DataFrame when input files do not exist") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      load(s"file:${baseDirectory()}/netflow-file-r*")
    df.collect().isEmpty should be (true)
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

  test("fail if no version specified") {
    intercept[RuntimeException] {
      new NetflowRelation(Array.empty, None, None, Map.empty)(new SQLContext(sc))
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

  test("prune statistics columns when statistics option is true") {
    val sqlContext = new SQLContext(sc)
    val params = Map("version" -> "5", "statistics" -> "true")
    val relation = new NetflowRelation(Array(path1), None, None, params)(sqlContext)

    val path = new Path(path1)
    val fileStatus = path.getFileSystem(new Configuration(false)).getFileStatus(path)
    val rdd = relation.buildScan(Array.empty, Array(fileStatus))
    // should be the same number of fields as statistics columns of mapper for version 5
    rdd.first.toSeq.length should be (MapperV5.getStatisticsColumns().length)
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

  test("read NetFlow files using implicit wrapper") {
    val sqlContext = new SQLContext(sc)
    import com.github.sadikovi.spark.netflow._

    var df = sqlContext.emptyDataFrame
    var msg = ""

    // test failure on passing different than "5" version
    try {
      df = sqlContext.read.netflow(s"file:${path5}")
      df.count()
    } catch {
      case se: SparkException => msg = se.getMessage()
      case other: Throwable => throw other
    }

    assert(msg.contains("java.lang.IllegalArgumentException: requirement failed: " +
      "Expected version 5, got 8"))

    // test parsing normal file
    df = sqlContext.read.netflow(s"file:${path2}")
    val expected = sqlContext.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:${path2}")
    compare(df, expected)
  }

  private def readNetflow(sqlContext: SQLContext, path: String, stringify: Boolean): DataFrame = {
    sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      option("stringify", s"${stringify}").load(s"file:${path}").
      select("srcip", "dstip", "srcport", "dstport")
  }

  test("issue #2 - mapper::getConversionsForFields") {
    // should return empty array
    val mapper = SchemaResolver.getMapperForVersion(5)
    val fields1 = Array(NetflowV5.V5_FIELD_SRCADDR, NetflowV5.V5_FIELD_DSTADDR)
    mapper.getConversionsForFields(fields1).size should be (2)

    val fields2 = Array(NetflowV5.V5_FIELD_UNIX_SECS, NetflowV5.V5_FIELD_DSTADDR)
    mapper.getConversionsForFields(fields2).size should be (1)

    val fields3 = Array(NetflowV5.V5_FIELD_UNIX_SECS, NetflowV5.V5_FIELD_UNIX_NSECS)
    mapper.getConversionsForFields(fields3).size should be (0)
  }

  test("issue #2 - fields conversion to String for uncompressed file") {
    val sqlContext = new SQLContext(sc)
    var df = readNetflow(sqlContext, path1, false)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq(999, 4294902759L, 999, 743)))

    df = readNetflow(sqlContext, path1, true)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq("0.0.3.231", "255.255.3.231", 999, 743)))
  }

  test("issue #2 - fields conversion to String for compressed file") {
    val sqlContext = new SQLContext(sc)

    var df = readNetflow(sqlContext, path2, false)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq(999, 4294902759L, 999, 743)))

    df = readNetflow(sqlContext, path2, true)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq("0.0.3.231", "255.255.3.231", 999, 743)))
  }

  test("num to ip conversion") {
    val dataset = Seq(
      ("127.0.0.1", 2130706433L),
      ("172.71.4.54", 2890335286L),
      ("147.10.8.41", 2466908201L),
      ("10.208.97.205", 181428685L),
      ("144.136.17.61", 2424836413L),
      ("139.168.155.28", 2343082780L),
      ("172.49.10.53", 2888895029L),
      ("139.168.51.129", 2343056257L),
      ("10.152.185.135", 177781127L),
      ("144.131.33.125", 2424512893L),
      ("138.217.81.41", 2329497897L),
      ("147.10.7.77", 2466907981L),
      ("10.164.0.185", 178520249L),
      ("144.136.28.121", 2424839289L),
      ("172.117.8.117", 2893351029L),
      ("139.168.164.113", 2343085169L),
      ("147.132.87.29", 2474923805L),
      ("10.111.3.73", 175047497L),
      ("255.255.255.255", (2L<<31) - 1)
    )

    for (elem <- dataset) {
      val (ip, num) = elem
      ConversionFunctions.numToIp(num) should equal (ip)
    }
  }

  test("resolve statistics directory") {
    val rdd = new NetflowFileRDD(sc, Seq.empty, 0, Array.empty, Array.empty, None)

    // is not specified
    var maybeStatistics: Option[String] = None
    rdd.resolveStatisticsDir(maybeStatistics, rdd.getConf()) should be ((false, None))

    // wrong directory specified
    maybeStatistics = Option("file:/wrong/directory")
    intercept[IOException] {
      rdd.resolveStatisticsDir(maybeStatistics, rdd.getConf())
    }

    // collect statistics for default directory
    maybeStatistics = Option("")
    rdd.resolveStatisticsDir(maybeStatistics, rdd.getConf()) should be ((true, None))

    // collect statistics into different directory
    maybeStatistics = Option(s"${baseDirectory()}")
    val (use, dir) = rdd.resolveStatisticsDir(maybeStatistics, rdd.getConf())

    use should be (true)
    dir.isEmpty should be (false)
    dir.get.toString() should be (s"file:${baseDirectory()}")
  }
}
