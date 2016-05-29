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

package com.github.sadikovi.spark.util

import java.util.UUID

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{Path => HadoopPath}

import org.apache.spark.network.util.JavaUtils

private[spark] object Utils {
  /**
   * Convert string of bytes (1024, 10Mb, 5Kb, etc.) into number of bytes.
   * Copied from Apache Spark `Utils.scala`.
   * @param str string to parse
   * @return number of bytes for corresponding string
   */
  def byteStringAsBytes(str: String): Long = {
    JavaUtils.byteStringAsBytes(str)
  }

  /**
   * Create UUID for a string as 128-bit value string.
   * @param str string to create uuid for
   * @return generated UUID as string
   */
  def uuidForString(str: String): String = {
    UUID.nameUUIDFromBytes(str.getBytes()).toString()
  }

  /** Get context class loader on this thread or, if not present, default class loader for class */
  def getContextClassLoader(): ClassLoader = {
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
  }

  /** Return updated path with suffix appended */
  def withSuffix(path: HadoopPath, suffix: String*): HadoopPath = {
    path.suffix(s"${HadoopPath.SEPARATOR}${suffix.mkString(HadoopPath.SEPARATOR)}")
  }

  /**
   * Compute histogram mode, default step is chosen as square root of data points in array, but can
   * be provided custom step function. Instead of computing average of the largest bucket (one has
   * more elements), we just return the largest element in the bucket, since bucket range can be
   * very significant. `stepFunc` takes min element, max element and number of data points and
   * expects number of buckets as a return value.
   */
  def histogramMode(arr: Array[Long], stepFunc: Option[(Long, Long, Int) => Int] = None): Long = {
    require(arr.nonEmpty, "Expected non-empty array to compute histogram mode")
    if (arr.length == 1) {
      arr.head
    } else {
      // Sort in increasing order
      val srt = arr.sortWith(_ < _)
      val min = srt.head
      val max = srt.last

      // Compute number of buckets based on step function
      val numBuckets = stepFunc match {
        case Some(func) => func(min, max, arr.length)
        case None => Math.ceil(Math.pow(arr.length, 0.5)).toInt
      }

      val buckets = for (bucket <- 0 until numBuckets) yield {
        val start = Math.ceil((max - min) * bucket.toDouble / numBuckets).toLong + min
        val end = Math.floor((max - min) * (bucket + 1).toDouble / numBuckets).toLong + min
        (start, end)
      }

      val bucketStats = buckets.map { case (start, end) => {
        var maxElem: Long = 0
        var cnt: Int = 0

        for (elem <- srt) {
          if (elem >= start && elem <= end) {
            cnt += 1
            maxElem = Math.max(elem, maxElem)
          }
        }
        (maxElem, cnt)
      } }

      // Extract largest bucket, and return maximum value from the bucket
      val largestBucket = bucketStats.sortWith(_._2 > _._2).head
      largestBucket._1
    }
  }

  /**
   * Compute truncated mean of the dataset based on sample. `1 - sample` of the dataset is
   * discarded, mean is computed on the rest and flatten across all data points. Currently only
   * tail is truncated, making assumption that skewness is positive only.
   */
  def truncatedMean(arr: Array[Long], sample: Double): Long = {
    val n = arr.length
    require(n > 0, "Expected non-empty array to compute mean")
    require(sample > 0, s"Expected positive sample, got ${sample}")

    if (n == 1) {
      arr.head
    } else {
      val srt = arr.sortWith(_ < _)

      var sum: Long = 0
      for (i <- 0 until (sample * n).toInt) {
        sum = sum + srt(i)
      }

      Math.ceil(sum / n / sample).toLong
    }
  }

  /** Create temporary directory on local file system */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "netflow"): HadoopPath = {
    val dir = Utils.withSuffix(new HadoopPath(root), namePrefix, UUID.randomUUID().toString)
    val fs = dir.getFileSystem(new HadoopConf(false))
    fs.mkdirs(dir)
    dir
  }

  /** Execute block of code with temporary hadoop path */
  private def withTempHadoopPath(path: HadoopPath)(func: HadoopPath => Unit): Unit = {
    try {
      func(path)
    } finally {
      val fs = path.getFileSystem(new HadoopConf(false))
      fs.delete(path, true)
    }
  }

  /** Execute code block with created temporary directory */
  def withTempDir(func: HadoopPath => Unit): Unit = {
    withTempHadoopPath(Utils.createTempDir())(func)
  }

  /** Execute code block with created temporary file */
  def withTempFile(func: HadoopPath => Unit): Unit = {
    val file = Utils.withSuffix(Utils.createTempDir(), UUID.randomUUID().toString)
    withTempHadoopPath(file)(func)
  }

  /** Get cleaned class name for logging */
  def getLogName(clazz: Class[_]): String = {
    clazz.getName().stripSuffix("$")
  }
}
