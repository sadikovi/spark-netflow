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
import org.apache.spark.sql.{AnalysisException, SparkSession, DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import com.github.sadikovi.netflowlib.Buffers.RecordBuffer
import com.github.sadikovi.netflowlib.version.NetFlowV5
import com.github.sadikovi.spark.netflow.sources._
import com.github.sadikovi.spark.util.Utils
import com.github.sadikovi.testutil.{UnitTestSuite, SparkLocal}
import com.github.sadikovi.testutil.implicits._

/** Common functionality to read NetFlow files */
abstract class SparkNetFlowTestSuite extends UnitTestSuite with SparkLocal {
  protected def readNetFlow(
      spark: SparkSession,
      version: Short,
      path: String,
      stringify: Boolean): DataFrame = {
    spark.read.format("com.github.sadikovi.spark.netflow").option("version", version).
      option("stringify", stringify).load(s"file:$path").
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
  // version 5 file that starts with large byte 255
  val path13 = getClass().getResource("/anomaly/ftv5.2016-04-09.compress9.large-byte-start").getPath
}

class NetFlowSuite extends SparkNetFlowTestSuite {
  override def beforeAll() {
    startSparkSession()
  }

  override def afterAll() {
    stopSparkSession()
  }

  test("read uncompressed v5 format") {
    val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      option("stringify", "false").load(s"file:$path1")

    val res = df.collect
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0,
      65280L, 17, 0, 0, 0, 0, 0, 0, 0, 65280))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 231, 0, 0, 0, 0, 0, 999, 743))
  }

  test("read compressed v5 format") {
    val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      option("stringify", "false").load(s"file:$path2")

    val res = df.collect
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0,
      65280L, 17, 0, 0, 0, 0, 0, 0, 0, 65280))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 231, 0, 0, 0, 0, 0, 999, 743))
  }

  test("read uncompressed v7 format") {
    val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "7").
      option("stringify", "false").load(s"file:$path6")

    val res = df.collect
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0, 65280,
      17, 0, 0, 0, 0, 0, 0, 0, 0, 65280, 0))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 0, 0, 0, 0, 0, 0, 0, 999, 743, 999))
  }

  test("read compressed v7 LITTLE_ENDIAN format") {
    val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "7").
      option("stringify", "false").load(s"file:$path7")

    val res = df.collect
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0, 65280,
      17, 0, 0, 0, 0, 0, 0, 0, 0, 65280, 0))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 0, 0, 0, 0, 0, 0, 0, 999, 743, 999))
  }

  test("read compressed v7 BIG_ENDIAN format") {
    val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "7").
      option("stringify", "false").load(s"file:$path8")

    val res = df.collect
    res.length should be (1000)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0, 65280,
      17, 0, 0, 0, 0, 0, 0, 0, 0, 65280, 0))
    res.last should be (Row(0, 0, 0, 0, 999, 4294902759L, 0, 999, 743, 1000, 1000, 999, 4294902759L,
      999, 743, 17, 0, 0, 0, 0, 0, 0, 0, 999, 743, 999))
  }

  test("fail loading DataFrame when input files do not exist (invalid path)") {
    val err = intercept[AnalysisException] {
      spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
        load(s"file:${baseDirectory()}" / "netflow-file-r*")
    }
    assert(err.getMessage.contains("Path does not exist"))
  }

  test("return empty DataFrame when input files do not exist (empty directory)") {
    withTempDir { dir =>
      val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
        load(s"file:$dir")
      df.collect.isEmpty should be (true)
    }
  }

  test("fail reading invalid input") {
    val err = intercept[SparkException] {
      spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
        load(s"file:$path3").count
    }
    val msg = err.getMessage()
    assert(msg.contains("Corrupt NetFlow file. Wrong magic number"))
    // check that message contains file path
    assert(msg.contains(path3))
  }

  test("fail reading invalid file with corrupt metadata (byte order/stream version)") {
    withTempDir { dir =>
      // write file of 4 bytes, first 2 should be magic numbers, byte order is set to correct
      // value and stream version is wrong, which will trigger unsupported operation exception
      val metadata = Array[Byte](0xCF.toByte, 0x10.toByte, 1, 127)
      val out = fs.create(dir / "file")
      out.write(metadata)
      out.close()

      val err = intercept[SparkException] {
        spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
          load(s"file:${dir / "file"}").count()
      }
      val msg = err.getMessage()
      assert(msg.contains("Unsupported stream version 127"))
      assert(msg.contains(s"${dir / "file"}"))
    }
  }

  test("fail to read data of corrupt file") {
    val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      load(s"file:$path4")
    val err = intercept[SparkException] {
      df.select("srcip").count
    }
    val msg = err.getMessage()
    assert(msg.contains("Unexpected EOF"))
    // check that message contains file path
    assert(msg.contains(path4))
  }

  test("fail to read unsupported version 8") {
    intercept[ClassNotFoundException] {
      spark.read.format("com.github.sadikovi.spark.netflow").option("version", "8").
        load(s"file:$path4")
    }
  }

  test("read empty non-compressed NetFlow file") {
    val df = spark.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:$path9")
    df.collect.length should be (0)
  }

  test("read empty compressed NetFlow file") {
    var df: DataFrame = null

    // Read file with compression 2
    df = spark.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:$path10")
    df.collect.length should be (0)

    df = spark.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:$path11")
    df.collect.length should be (0)
  }

  test("read NetFlow file with 1 record only") {
    val df = spark.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").option("stringify", "false").load(s"file:$path12")
    val res = df.collect
    res.length should be (1)
    res.head should be (Row(0, 0, 0, 0, 0, 4294901760L, 0, 0, 65280, 1, 1, 0, 4294901760L, 0,
      65280L, 17, 0, 0, 0, 0, 0, 0, 0, 65280))
  }

  test("issue #26 - read NetFlow file that starts with large byte") {
    val df = spark.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:$path13").
      select("srcip", "dstip", "protocol", "srcport", "dstport", "octets", "packets")
    val res = df.collect

    res.length should be (1000)
    res.take(5) should be (Array(
      Row("172.29.31.106", "141.168.161.5", "ICMP", 2048, 18358, 96L, 1L),
      Row("139.168.52.154", "172.49.40.150", "TCP", 59527, 3016, 41L, 1L),
      Row("144.136.201.10", "192.168.177.26", "UDP", 123, 123, 76L, 1),
      Row("147.132.239.10", "139.168.47.149", "TCP", 80, 64036, 40L, 1L),
      Row("172.49.40.189", "172.115.30.32", "TCP", 443, 63526, 18642, 37)
    ))

    res.takeRight(5) should be (Array(
      Row("144.132.81.10", "139.168.143.23", "UDP", 53, 50725, 164L, 1L),
      Row("10.111.198.7", "147.132.239.10", "TCP", 56190, 80, 40L, 1L),
      Row("139.168.112.131", "172.49.40.146", "TCP", 64424, 3217, 919L, 4L),
      Row("10.97.217.136", "147.10.8.55", "TCP", 8080, 53941, 431L, 3L),
      Row("10.233.198.51", "144.132.81.10", "UDP", 52865, 53, 81L, 1L)
    ))
  }

  test("fail read if there is NetFlow version mismatch using implicit wrapper for v5") {
    import com.github.sadikovi.spark.netflow._
    // test failure on passing different than "5" version
    val err = intercept[SparkException] {
      spark.read.netflow5(s"file:$path5").count
    }
    assert(err.getMessage.contains("java.lang.IllegalArgumentException: requirement failed: " +
      "Expected version 5, got 8"))
  }

  test("read NetFlow files using implicit wrapper for v5") {
    import com.github.sadikovi.spark.netflow._
    // parsing NetFlow v5 file
    val df = spark.read.netflow5(s"file:$path2")
    val expected = spark.read.format("com.github.sadikovi.spark.netflow").
      option("version", "5").load(s"file:$path2")
    checkAnswer(df, expected)
  }

  test("fail read if there is NetFlow version mismatch using implicit wrapper for v7") {
    import com.github.sadikovi.spark.netflow._
    // test failure on passing different than "7" version
    val err = intercept[SparkException] {
      spark.read.netflow7(s"file:$path5").count
    }
    assert(err.getMessage.contains("java.lang.IllegalArgumentException: requirement failed: " +
      "Expected version 7, got 8"))
  }

  test("read NetFlow files using implicit wrapper for v7") {
    import com.github.sadikovi.spark.netflow._
    // test parsing normal file for version 7
    val df = spark.read.netflow7(s"file:$path7")
    val expected = spark.read.format("com.github.sadikovi.spark.netflow").
      option("version", "7").load(s"file:$path7")
    checkAnswer(df, expected)
  }

  test("issue #2 - fields conversion to String for uncompressed file") {
    var df = readNetFlow(spark, 5, path1, false)
    df.count should be (1000)
    df.collect.last should be (Row(999, 4294902759L, 999, 743, 17))

    df = readNetFlow(spark, 5, path1, true)
    df.count should be (1000)
    df.collect.last should be (Row("0.0.3.231", "255.255.3.231", 999, 743, "UDP"))

    // test for version 7 - uncompressed
    df = readNetFlow(spark, 7, path6, true)
    df.count should be (1000)
    df.collect.last should be (Row("0.0.3.231", "255.255.3.231", 999, 743, "UDP"))
  }

  test("issue #2 - fields conversion to String for compressed file") {
    var df = readNetFlow(spark, 5, path2, false)
    df.count should be (1000)
    df.collect.last should be (Row(999, 4294902759L, 999, 743, 17))

    df = readNetFlow(spark, 5, path2, true)
    df.count should be (1000)
    df.collect.last should be (Row("0.0.3.231", "255.255.3.231", 999, 743, "UDP"))

    // test for version 7 - compressed
    df = readNetFlow(spark, 7, path8, true)
    df.count should be (1000)
    df.collect.last should be (Row("0.0.3.231", "255.255.3.231", 999, 743, "UDP"))
  }

  test("ignore scanning file for unix_secs out of range") {
    val df = spark.read.netflow5(s"file:$path1").filter(col("unix_secs") === -1)
    df.count should be (0)
  }

  test("scan variations with simple predicate") {
    var df: DataFrame = null

    df = spark.read.netflow5(s"file:$path2").filter(col("srcip") === "0.0.0.1").
      select("srcip", "dstip", "protocol")
    df.collect should be (Array(Row("0.0.0.1", "255.255.0.1", "UDP")))

    df = spark.read.netflow7(s"file:$path7").
      filter(col("srcport") > 0 && col("srcport") <= 5).
      select("srcip", "dstip", "protocol", "srcport")
    df.collect should be (Array(
      Row("0.0.0.1", "255.255.0.1", "UDP", 1),
      Row("0.0.0.2", "255.255.0.2", "UDP", 2),
      Row("0.0.0.3", "255.255.0.3", "UDP", 3),
      Row("0.0.0.4", "255.255.0.4", "UDP", 4),
      Row("0.0.0.5", "255.255.0.5", "UDP", 5)
    ))
  }

  test("scan variations with complex predicate") {
    var df: DataFrame = null

    df = spark.read.netflow5(s"file:$path2").
      select("unix_secs", "srcip", "dstip", "srcport", "dstport", "octets").
      filter(col("unix_secs").between(0L, 1L) && col("srcip") === "0.0.1.1")
    df.collect should be (Array(Row(0L, "0.0.1.1", "255.255.1.1", 257, 1, 258)))

    df = spark.read.netflow5(s"file:$path2").
      select("unix_secs", "srcip", "dstip", "srcport", "dstport", "octets").
      filter(col("unix_secs").between(1L, 2L) && col("srcip") === "0.0.1.1")
    df.collect should be (Array.empty)
  }

  test("scan with unsupported predicate and/or isNull") {
    var df: DataFrame = null

    df = spark.read.netflow7(s"file:$path7").filter(col("srcip").startsWith("0.0."))
    df.count should be (1000)
    df.distinct.count should be (1000)

    df = spark.read.netflow7(s"file:$path7").filter(col("srcip").isNull)
    df.collect should be (Array.empty)

    df = spark.read.netflow7(s"file:$path7").filter(col("srcip").isNotNull)
    df.count should be (1000)
    df.distinct.count should be (1000)
  }

  //////////////////////////////////////////////////////////////
  // DefaultSource tests
  //////////////////////////////////////////////////////////////

  test("check default opts and interface for default source") {
    val format = new DefaultSource()
    format.getInterface() should be (null)
    format.getOptions() should be (null)
  }

  test("resolve to default interface if no version and no paths are specified") {
    val format = new DefaultSource()
    val interface = format.resolveInterface(spark, Map.empty, Seq.empty)
    interface.getClass.getName should be (
      s"${DefaultSource.INTERNAL_PARTIAL_CLASSNAME}${DefaultSource.VERSION_5}.InterfaceV5")
  }

  test("resolve interface 5 if no version provided with at least one path for v5") {
    val format = new DefaultSource()
    val interface = format.resolveInterface(spark, Map.empty, Seq(new Path(s"file:$path1")))
    interface.getClass.getName should be (
      s"${DefaultSource.INTERNAL_PARTIAL_CLASSNAME}${DefaultSource.VERSION_5}.InterfaceV5")
  }

  test("resolve interface 7 if no version provided with at least one path for v7") {
    val format = new DefaultSource()
    val interface = format.resolveInterface(spark, Map.empty, Seq(new Path(s"file:$path6")))
    interface.getClass.getName should be (
      s"${DefaultSource.INTERNAL_PARTIAL_CLASSNAME}${DefaultSource.VERSION_7}.InterfaceV7")
  }

  test("infer version 5 interface from Seq(version 5, version 7) paths") {
    val format = new DefaultSource()
    val interface = format.resolveInterface(spark, Map.empty,
      Seq(new Path(s"file:$path1"), new Path(s"file:$path6")))
    interface.version() should be (5)
  }

  test("infer version 7 interface from Seq(version 7, version 5) paths") {
    val format = new DefaultSource()
    val interface = format.resolveInterface(spark, Map.empty,
      Seq(new Path(s"file:$path6"), new Path(s"file:$path1")))
    interface.version() should be (7)
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

  test("issue #5 - prune zero columns when running count directly") {
    val format = new DefaultSource()
    // run test for version 5
    val schema = format.inferSchema(spark, Map("version" -> "5"), Seq.empty)
    // schema is always resolved to Some()
    val func = format.buildReader(spark, schema.get, StructType(Nil), StructType(Nil), Seq.empty,
      Map.empty, new Configuration(false))
    val iter = func(PartitionedFile(InternalRow.empty, path1, 0, 1024))
    iter.hasNext should be (true)
    // should be no columns in internal row
    iter.next() should be (InternalRow())
  }

  test("filter values when reading file, if predicate-pushdown is enabled") {
    val format = new DefaultSource()
    // run test for version 5
    val schema = format.inferSchema(spark,
      Map("version" -> "5", "predicate-pushdown" -> "true"), Seq.empty)
    // schema is always resolved to Some()
    // predicate should return 0 records, since all records have unix_secs as 0
    val func = format.buildReader(spark, schema.get, StructType(Nil), StructType(Nil),
      Seq(EqualTo("unix_secs", 1L)), Map.empty, new Configuration(false))
    val iter = func(PartitionedFile(InternalRow.empty, path1, 0, 1024))
    val res = iter.toArray
    assert(res.length === 0)
  }

  test("filter values when reading file, if predicate-pushdown is disabled") {
    val format = new DefaultSource()
    // run test for version 5
    val schema = format.inferSchema(spark,
      Map("version" -> "5", "predicate-pushdown" -> "false"), Seq.empty)
    // schema is always resolved to Some()
    // use same predicate on unix_secs, but with disabled predicate pushdown, this should return
    // iterator with all records from the file
    val func = format.buildReader(spark, schema.get, StructType(Nil), StructType(Nil),
      Seq(EqualTo("unix_secs", 1L)), Map.empty, new Configuration(false))
    val iter = func(PartitionedFile(InternalRow.empty, path1, 0, 1024))
    val res = iter.toArray
    // file contains 1000 records (flow-gen)
    assert(res.length === 1000)
  }

  test("prepareWrite is unsupported") {
    val format = new DefaultSource()
    val err = intercept[UnsupportedOperationException] {
      format.prepareWrite(spark, null, Map.empty, StructType(Nil))
    }
    assert(err.getMessage.contains("Write is not supported in this version of package"))
  }

  test("equals for default source") {
    new DefaultSource().equals(new DefaultSource()) should be (true)
    new DefaultSource().equals(null) should be (false)
  }

  test("inferVersion - return None for empty paths") {
    val conf = spark.sparkContext.hadoopConfiguration
    DefaultSource.inferVersion(conf, Nil) should be (None)
  }

  test("inferVersion - return Some for correct NetFlow file") {
    val conf = spark.sparkContext.hadoopConfiguration
    DefaultSource.inferVersion(conf, new Path(path1) :: Nil) should be (Some(5))
    DefaultSource.inferVersion(conf, new Path(path2) :: Nil) should be (Some(5))
    DefaultSource.inferVersion(conf, new Path(path6) :: Nil) should be (Some(7))
    DefaultSource.inferVersion(conf, new Path(path7) :: Nil) should be (Some(7))
  }

  test("inferVersion - fail to infer for invalid file") {
    val conf = spark.sparkContext.hadoopConfiguration
    val err = intercept[IOException] {
      DefaultSource.inferVersion(conf, new Path(path3) :: Nil)
    }
    assert(err.getMessage.contains("Failed to infer version for provided NetFlow files"))
  }
}

/** Suite to test `ignoreCorruptFiles` option */
class NetFlowIgnoreCorruptSuite extends SparkNetFlowTestSuite {
  before {
    startSparkSession()
  }

  after {
    stopSparkSession
  }

  test("return empty iterator, when file is not a NetFlow file (header failure)") {
    withSQLConf("spark.sql.files.ignoreCorruptFiles" -> "true") {
      val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
        load(s"file:$path3")
      df.count should be (0)
    }
  }

  test("return empty iterator, when file is not a NetFlow file (metadata is wrong)") {
    withSQLConf("spark.sql.files.ignoreCorruptFiles" -> "true") {
      withTempDir { dir =>
        // write file of 4 bytes, first 2 should be magic numbers, byte order is set to correct
        // value and stream version is wrong, which will trigger unsupported operation exception
        val metadata = Array[Byte](0xCF.toByte, 0x10.toByte, 1, 127)
        val out = fs.create(dir / "file")
        out.write(metadata)
        out.close()

        val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
          load(s"file:${dir / "file"}")
        df.count should be (0)
      }
    }
  }

  test("return partial data, when NetFlow file is corrupt") {
    withSQLConf("spark.sql.files.ignoreCorruptFiles" -> "true") {
      val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
        load(s"file:$path4")
      df.count should be (553)
    }
  }

  test("return full data, when NetFlow file is correct") {
    withSQLConf("spark.sql.files.ignoreCorruptFiles" -> "true") {
      val df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
        load(s"file:$path2")
      df.count should be (1000)
    }
  }
}
