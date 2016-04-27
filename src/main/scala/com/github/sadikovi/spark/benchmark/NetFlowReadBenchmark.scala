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

package com.github.sadikovi.spark.benchmark

import java.util.{HashMap => JHashMap}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col

/** Configuration option for cli */
private case class ConfOption(name: String)

/** Configuration map for cli */
private case class Conf() {
  private val conf: JHashMap[ConfOption, String] = new JHashMap[ConfOption, String]()

  def addOption(opt: ConfOption, value: String): Unit = conf.put(opt, value)

  def get(opt: ConfOption): Option[String] = Option(conf.get(opt))
}

/**
 * NetFlow benchmarks divided into several categories:
 * - full scan without any predicate with field conversion and without
 * (result is cached and counted)
 * - predicate scan with predicate pushdown and without (result is aggregated by protocol)
 * - aggregated scan with predicate pushdown trying to replicate report
 * (result is cached and counted)
 */
object NetFlowReadBenchmark {
  // Required options
  private val ITERATIONS = ConfOption("--iterations")
  private val FILES = ConfOption("--files")
  private val VERSION = ConfOption("--version")

  // Initialize Spark context
  val sparkConf = new SparkConf()
  val sc = new SparkContext("local[*]", "test-sql-context", sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    val conf = process(args.toList, Conf())

    // Extract options
    val iterations = conf.get(ITERATIONS).getOrElse(
      sys.error("Number of iterations must be specified, e.g. '--iterations 3'")).toInt
    val files = conf.get(FILES).getOrElse(
      sys.error("Files / directory must be specified, e.g. '--files /tmp/files'"))
    val version = conf.get(VERSION).getOrElse(
      sys.error("NetFlow version must be specified, e.g. '--version 5'"))

    // scalastyle:off
    println(s"- Iterations: ${iterations}")
    println(s"- Files: ${files}")
    println(s"- Version: ${version}")
    // scalastyle:on

    // Defined benchmarks
    fullScanBenchmark(iterations, version, files)
    predicateScanBenchmark(iterations, version, files)
    aggregatedScanBenchmark(iterations, version, files)
  }

  private def process(args: List[String], conf: Conf): Conf = args match {
    case ITERATIONS.name :: value :: tail =>
      conf.addOption(ITERATIONS, value)
      process(tail, conf)
    case FILES.name :: value :: tail =>
      conf.addOption(FILES, value)
      process(tail, conf)
    case VERSION.name :: value :: tail =>
      conf.addOption(VERSION, value)
      process(tail, conf)
    case other :: tail => process(tail, conf)
    case Nil => conf
  }

  /** Test full read of files provided with or without `stringify` option */
  def fullScanBenchmark(iters: Int, version: String, files: String): Unit = {
    val sqlBenchmark = new Benchmark("NetFlow full scan", 10000, iters)

    sqlBenchmark.addCase("Scan, stringify = F") { iter =>
      val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
        option("version", version).option("stringify", "false").load(files)
      df.foreach(_ => Unit)
    }

    sqlBenchmark.addCase("Scan, stringify = T") { iter =>
      val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
        option("version", version).option("stringify", "true").load(files)
      df.foreach(_ => Unit)
    }

    sqlBenchmark.run()
  }

  /** Predicate scan benchmark, test high and low selectivity */
  def predicateScanBenchmark(iters: Int, version: String, files: String): Unit = {
    val sqlBenchmark = new Benchmark("NetFlow predicate scan", 10000, iters)

    sqlBenchmark.addCase("Predicate pushdown = F, high") { iter =>
      val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
        option("version", version).option("predicate-pushdown", "false").load(files).
        filter(col("srcport") !== 10)
      df.foreach(_ => Unit)
    }

    sqlBenchmark.addCase("Predicate pushdown = T, high") { iter =>
      val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
        option("version", version).option("predicate-pushdown", "true").load(files).
        filter(col("srcport") !== 10)
      df.foreach(_ => Unit)
    }

    sqlBenchmark.addCase("Predicate pushdown = F, low") { iter =>
      val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
        option("version", version).option("predicate-pushdown", "false").load(files).
        filter(col("srcip") === "127.0.0.1")
      df.foreach(_ => Unit)
    }

    sqlBenchmark.addCase("Predicate pushdown = T, low") { iter =>
      val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
        option("version", version).option("predicate-pushdown", "true").load(files).
        filter(col("srcip") === "127.0.0.1")
      df.foreach(_ => Unit)
    }

    sqlBenchmark.run()
  }

  /** Run simple aggregation based with filtering */
  def aggregatedScanBenchmark(iters: Int, version: String, files: String): Unit = {
    val sqlBenchmark = new Benchmark("NetFlow aggregated report", 10000, iters)

    sqlBenchmark.addCase("Aggregated report") { iter =>
      val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
        option("version", version).load(files).
        filter(col("srcport") > 10).
        select("srcip", "dstip", "srcport", "dstport", "packets", "octets")

      val agg = df.groupBy(col("srcip"), col("dstip"), col("srcport"), col("dstport")).count()
      agg.foreach(_ => Unit)
    }

    sqlBenchmark.run()
  }
}
