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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._

import org.scalatest.ConfigMap

import com.github.sadikovi.netflowlib.Buffers.RecordBuffer
import com.github.sadikovi.netflowlib.version.NetFlowV5
import com.github.sadikovi.spark.netflow.sources._
import com.github.sadikovi.spark.rdd.NetFlowFileRDD
import com.github.sadikovi.spark.util.Utils
import com.github.sadikovi.testutil.{UnitTestSpec, SparkLocal}
import com.github.sadikovi.testutil.implicits._

class NetFlowSuite extends UnitTestSpec with SparkLocal {
  override def beforeAll(configMap: ConfigMap) {
    startSparkContext()
  }

  override def afterAll(configMap: ConfigMap) {
    stopSparkContext()
  }

  private def readNetFlow(
      sqlContext: SQLContext,
      version: Short,
      path: String,
      stringify: Boolean): DataFrame = {
    sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", s"${version}").
      option("stringify", s"${stringify}").load(s"file:${path}").
      select("srcip", "dstip", "srcport", "dstport", "protocol")
  }

  // version 5 correct files
  val path1 = getClass().getResource("/correct/ftv5.2016-01-13.nocompress.bigend.sample").getPath
  val path2 = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath
  val paths12 = testDirectory() / "resources" / "correct" / "ftv5*"
  // version 5 corrupt files
  val path3 = getClass().getResource("/corrupt/ftv5.2016-01-13.compress.9.sample-01").getPath
  val path4 = getClass().getResource("/corrupt/ftv5.2016-01-13.compress.9.sample-00").getPath
  // version 8 files - unsupported currently
  val path5 = getClass().getResource("/unsupport/ftv8.2016-01-17.compress.7.bigend.sample").getPath
  // version 7 correct files
  val path6 = getClass().getResource("/correct/ftv7.2016-02-14.nocompress.bigend.sample").getPath
  val path7 = getClass().getResource("/correct/ftv7.2016-02-14.compress.9.litend.sample").getPath
  val path8 = getClass().getResource("/correct/ftv7.2016-02-14.compress.9.bigend.sample").getPath
  // version 5 empty files
  val path9 = getClass().getResource("/anomaly/ftv5.2016-03-15.nocompress.bigend.empty").getPath
  val path10 = getClass().getResource("/anomaly/ftv5.2016-03-15.compress2.bigend.empty").getPath
  val path11 = getClass().getResource("/anomaly/ftv5.2016-03-15.compress9.bigend.empty").getPath
  // version 5 file with 1 record
  val path12 = getClass().getResource("/anomaly/ftv5.2016-03-15.compress9.bigend.records1").getPath

  test("read uncompressed v5 format") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      option("stringify", "false").load(s"file:${path1}")

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
      option("stringify", "false").load(s"file:${path2}")

    val res = df.collect()
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0,
      65280L, 17, 0, 0, 0, 0, 0, 0, 0, 65280))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 231, 0, 0, 0, 0, 0, 999, 743))
  }

  test("read uncompressed v7 format") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "7").
      option("stringify", "false").load(s"file:${path6}")

    val res = df.collect()
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0, 65280,
      17, 0, 0, 0, 0, 0, 0, 0, 0, 65280, 0))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 0, 0, 0, 0, 0, 0, 0, 999, 743, 999))
  }

  test("read compressed v7 LITTLE_ENDIAN format") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "7").
      option("stringify", "false").load(s"file:${path7}")

    val res = df.collect()
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0, 65280,
      17, 0, 0, 0, 0, 0, 0, 0, 0, 65280, 0))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 0, 0, 0, 0, 0, 0, 0, 999, 743, 999))
  }

  test("read compressed v7 BIG_ENDIAN format") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "7").
      option("stringify", "false").load(s"file:${path8}")

    val res = df.collect()
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0, 65280,
      17, 0, 0, 0, 0, 0, 0, 0, 0, 65280, 0))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 0, 0, 0, 0, 0, 0, 0, 999, 743, 999))
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
      assert(false, "No exception was thrown")
    } catch {
      case se: SparkException =>
        val msg = se.getMessage()
        assert(msg.contains("java.lang.UnsupportedOperationException: " +
          "Corrupt NetFlow file. Wrong magic number"))
      case other: Throwable => throw other
    }
  }

  test("fail to read data of corrupt file") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      load(s"file:${path4}")
    try {
      df.select("srcip").count()
      assert(false, "No exception was thrown")
    } catch {
      case se: SparkException =>
        val msg = se.getMessage()
        assert(msg.contains("java.lang.IllegalArgumentException: Unexpected EOF"))
      case other: Throwable => throw other
    }
  }

  test("fail to read unsupported version 8") {
    val sqlContext = new SQLContext(sc)
    intercept[ClassNotFoundException] {
      sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "8").
        load(s"file:${path4}")
    }
  }

  test("fail if no version specified") {
    intercept[RuntimeException] {
      new NetFlowRelation(Array.empty, None, None, Map.empty)(new SQLContext(sc))
    }
  }

  test("read empty non-compressed NetFlow file") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:${path9}")
    df.collect().length should be (0)
  }

  test("read empty compressed NetFlow file") {
    val sqlContext = new SQLContext(sc)
    var df: DataFrame = null

    // Read file with compression 2
    df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:${path10}")
    df.collect().length should be (0)

    df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:${path11}")
    df.collect().length should be (0)
  }

  test("read NetFlow file with 1 record only") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").option("stringify", "false").load(s"file:${path12}")
    val res = df.collect()
    res.length should be (1)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0,
      65280L, 17, 0, 0, 0, 0, 0, 0, 0, 65280))
  }

  test("issue #5 - prune only one column when running cound directly") {
    val sqlContext = new SQLContext(sc)
    val relation = new NetFlowRelation(Array(path1), None, None, Map("version" -> "5"))(sqlContext)

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
    var relation = new NetFlowRelation(Array(path1), None, None, params)(sqlContext)
    relation.getBufferSize() should be (RecordBuffer.BUFFER_LENGTH_2)

    // set buffer size to be 64Kb
    params = Map("version" -> "5", "buffer" -> "64Kb")
    relation = new NetFlowRelation(Array(path1), None, None, params)(sqlContext)
    relation.getBufferSize() should be (64 * 1024)

    // buffer size >> Integer.MAX_VALUE
    intercept[RuntimeException] {
      params = Map("version" -> "5", "buffer" -> "10Gb")
      relation = new NetFlowRelation(Array(path1), None, None, params)(sqlContext)
    }

    // negative buffer size
    intercept[NumberFormatException] {
      params = Map("version" -> "5", "buffer" -> "-1")
      relation = new NetFlowRelation(Array(path1), None, None, params)(sqlContext)
    }

    // buffer size < min buffer size should be updated to min buffer size
    params = Map("version" -> "5", "buffer" -> "10")
    relation = new NetFlowRelation(Array(path1), None, None, params)(sqlContext)
    relation.getBufferSize() should be (RecordBuffer.MIN_BUFFER_LENGTH)

    // just for completeness, test on wrong buffer value
    intercept[NumberFormatException] {
      params = Map("version" -> "5", "buffer" -> "wrong")
      relation = new NetFlowRelation(Array(path1), None, None, params)(sqlContext)
    }
  }

  test("read NetFlow files using implicit wrapper") {
    val sqlContext = new SQLContext(sc)
    import com.github.sadikovi.spark.netflow._

    var df: DataFrame = sqlContext.emptyDataFrame
    var expected: DataFrame = null
    var msg = ""

    ////////////////////////////////////////////////////////////
    // VERSION 5
    ////////////////////////////////////////////////////////////
    // test failure on passing different than "5" version
    try {
      df = sqlContext.read.netflow5(s"file:${path5}")
      df.count()
      assert(false, "No exception was thrown")
    } catch {
      case se: SparkException => msg = se.getMessage()
      case other: Throwable => throw other
    }

    assert(msg.contains("java.lang.IllegalArgumentException: requirement failed: " +
      "Expected version 5, got 8"))

    // test parsing normal file for version 5
    df = sqlContext.read.netflow5(s"file:${path2}")
    expected = sqlContext.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:${path2}")
    compare(df, expected)

    ////////////////////////////////////////////////////////////
    // VERSION 7
    ////////////////////////////////////////////////////////////
    // test failure on passing different than "7" version
    try {
      df = sqlContext.read.netflow7(s"file:${path5}")
      df.count()
      assert(false, "No exception was thrown")
    } catch {
      case se: SparkException => msg = se.getMessage()
      case other: Throwable => throw other
    }

    assert(msg.contains("java.lang.IllegalArgumentException: requirement failed: " +
      "Expected version 7, got 8"))

    // test parsing normal file for version 7
    df = sqlContext.read.netflow7(s"file:${path7}")
    expected = sqlContext.read.format("com.github.sadikovi.spark.netflow").
      option("version", "7").load(s"file:${path7}")
    compare(df, expected)
  }

  test("issue #2 - conversion of various fields combinations") {
    // should return empty array
    val interface = NetFlowRegistry.createInterface("com.github.sadikovi.spark.netflow.version5")

    val fields1 = Array(interface.getColumn("srcip"), interface.getColumn("dstip"))
    fields1.map(_.convertFunction.isDefined) should be (Array(true, true))

    val fields2 = Array(interface.getColumn("unix_secs"), interface.getColumn("srcip"))
    fields2.map(_.convertFunction.isDefined) should be (Array(false, true))

    val fields3 = Array(interface.getColumn("unix_secs"), interface.getColumn("unix_nsecs"))
    fields3.map(_.convertFunction.isDefined) should be (Array(false, false))
  }

  test("issue #2 - fields conversion to String for uncompressed file") {
    val sqlContext = new SQLContext(sc)
    var df = readNetFlow(sqlContext, 5, path1, false)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq(999, 4294902759L, 999, 743, 17)))

    df = readNetFlow(sqlContext, 5, path1, true)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq("0.0.3.231", "255.255.3.231", 999, 743, "UDP")))

    // test for version 7 - uncompressed
    df = readNetFlow(sqlContext, 7, path6, true)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq("0.0.3.231", "255.255.3.231", 999, 743, "UDP")))
  }

  test("issue #2 - fields conversion to String for compressed file") {
    val sqlContext = new SQLContext(sc)

    var df = readNetFlow(sqlContext, 5, path2, false)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq(999, 4294902759L, 999, 743, 17)))

    df = readNetFlow(sqlContext, 5, path2, true)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq("0.0.3.231", "255.255.3.231", 999, 743, "UDP")))

    // test for version 7 - compressed
    df = readNetFlow(sqlContext, 7, path8, true)
    df.count() should be (1000)
    df.collect().last should be (Row.fromSeq(Seq("0.0.3.231", "255.255.3.231", 999, 743, "UDP")))
  }

  // Resolve partition mode based on option specified, only tests `NetFlowRelation`
  test("resolve partition mode") {
    val sqlContext = new SQLContext(sc)
    var relation: NetFlowRelation = null

    relation = new NetFlowRelation(Array(path1), None, None, Map("version" -> "5"))(sqlContext)
    relation.getPartitionMode() should be (DefaultPartitionMode(None))
    // This is valid since simple partition mode just returns maximum
    relation.getPartitionMode().resolveNumPartitions(100) should be (100)

    relation = new NetFlowRelation(Array(path1), None, None,
      Map("version" -> "5", "partitions" -> "default"))(sqlContext)
    relation.getPartitionMode() should be (DefaultPartitionMode(None))

    relation = new NetFlowRelation(Array(path1), None, None,
      Map("version" -> "5", "partitions" -> "auto"))(sqlContext)
    relation.getPartitionMode() should be (AutoPartitionMode(Utils.byteStringAsBytes("144Mb"),
      sqlContext.sparkContext.defaultParallelism * 2))

    relation = new NetFlowRelation(Array(path1), None, None,
      Map("version" -> "5", "partitions" -> "100"))(sqlContext)
    relation.getPartitionMode() should be (DefaultPartitionMode(Option(100)))

    // We check invalid number of partitions when we build scan, so at the step of creating a
    // relation we can have negative number of partitions
    relation = new NetFlowRelation(Array(path1), None, None,
      Map("version" -> "5", "partitions" -> "-100"))(sqlContext)
    relation.getPartitionMode() should be (DefaultPartitionMode(Option(-100)))

    try {
      new NetFlowRelation(Array(path1), None, None,
        Map("version" -> "5", "partitions" -> "test100"))(sqlContext)
      assert(false, "No exception was thrown")
    } catch {
      case runtime: RuntimeException =>
        runtime.getMessage() should be ("Wrong number of partitions test100")
      case other: Throwable => throw other
    }
  }

  test("ignore scanning file for unix_secs out of range") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.netflow5(s"file:${path1}").filter(col("unix_secs") === -1)
    df.count() should be (0)
  }

  test("scan variations with simple predicate") {
    val sqlContext = new SQLContext(sc)
    var df: DataFrame = null

    df = sqlContext.read.netflow5(s"file:${path2}").filter(col("srcip") === "0.0.0.1").
      select("srcip", "dstip", "protocol")
    df.collect() should be (Array(Row.fromSeq(Seq("0.0.0.1", "255.255.0.1", "UDP"))))

    df = sqlContext.read.netflow7(s"file:${path7}").
      filter(col("srcport") > 0 && col("srcport") <= 5).
      select("srcip", "dstip", "protocol", "srcport")
    df.collect() should be (Array(
      Row.fromSeq(Seq("0.0.0.1", "255.255.0.1", "UDP", 1)),
      Row.fromSeq(Seq("0.0.0.2", "255.255.0.2", "UDP", 2)),
      Row.fromSeq(Seq("0.0.0.3", "255.255.0.3", "UDP", 3)),
      Row.fromSeq(Seq("0.0.0.4", "255.255.0.4", "UDP", 4)),
      Row.fromSeq(Seq("0.0.0.5", "255.255.0.5", "UDP", 5))
    ))
  }

  test("scan variations with complex predicate") {
    val sqlContext = new SQLContext(sc)
    var df: DataFrame = null

    df = sqlContext.read.netflow5(s"file:${path2}").
      select("unix_secs", "srcip", "dstip", "srcport", "dstport", "octets").
      filter(col("unix_secs").between(0L, 1L) && col("srcip") === "0.0.1.1")
    df.collect() should be (Array(Row.fromSeq(Seq(0L, "0.0.1.1", "255.255.1.1", 257, 1, 258))))

    df = sqlContext.read.netflow5(s"file:${path2}").
      select("unix_secs", "srcip", "dstip", "srcport", "dstport", "octets").
      filter(col("unix_secs").between(1L, 2L) && col("srcip") === "0.0.1.1")
    df.collect() should be (Array.empty)
  }

  test("scan with unsupported predicate and/or isNull") {
    val sqlContext = new SQLContext(sc)
    var df: DataFrame = null

    df = sqlContext.read.netflow7(s"file:${path7}").filter(col("srcip").startsWith("0.0."))
    df.count() should be (1000)
    df.distinct.count() should be (1000)

    df = sqlContext.read.netflow7(s"file:${path7}").filter(col("srcip").isNull)
    df.collect() should be (Array.empty)

    df = sqlContext.read.netflow7(s"file:${path7}").filter(col("srcip").isNotNull)
    df.count() should be (1000)
    df.distinct.count() should be (1000)
  }

  // Test scan with different number of partitions, currently do not support "auto" mode
  test("scan with number of partitions") {
    val sqlContext = new SQLContext(sc)
    var df: DataFrame = null

    df = sqlContext.read.option("partitions", "auto").netflow5(s"file:${paths12}")
    df.rdd.partitions.length should be (2)
    df.count() should be (2000)

    df = sqlContext.read.option("partitions", "default").netflow5(s"file:${paths12}")
    df.rdd.partitions.length should be (2)
    df.count() should be (2000)

    df = sqlContext.read.option("partitions", "1").netflow5(s"file:${paths12}")
    df.rdd.partitions.length should be (1)
    df.count() should be (2000)

    df = sqlContext.read.option("partitions", "10").netflow5(s"file:${paths12}")
    df.rdd.partitions.length should be (2)
    df.count() should be (2000)

    // since Spark 1.6+ resolves number of partitions when building plan, this fails with
    // TreeNodeException with a cause of wrong number of partitions, though in Spark 1.5 and before
    // it actually throws IllegalArgumentException, so we need to check for both.
    try {
      df = sqlContext.read.option("partitions", "-1").netflow5(s"file:${paths12}")
      df.count()
      assert(false, "No exception was thrown")
    } catch {
      case iae: IllegalArgumentException =>
        assert(iae.getMessage().contains("Expected at least one partition, got"),
          "Target exception mismatch")
      case other: Throwable =>
        var cause = other
        // find the actual cause of the tree node exception
        while (cause.getCause() != null) {
          cause = cause.getCause()
        }
        assert(cause.getMessage().contains("Expected at least one partition, got"),
          "Target exception mismatch")
    }
  }
}
