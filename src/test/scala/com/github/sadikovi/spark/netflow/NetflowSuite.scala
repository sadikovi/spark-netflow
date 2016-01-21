package com.github.sadikovi.spark.netflow

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.SparkException
import org.apache.spark.sql.{SQLContext, Row}

import org.scalatest.ConfigMap

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
        assert(se.getCause().isInstanceOf[UnsupportedOperationException])
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
        assert(se.getCause().getMessage() === "Unexpected EOF")
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
}
