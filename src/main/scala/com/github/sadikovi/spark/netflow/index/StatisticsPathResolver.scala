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

package com.github.sadikovi.spark.netflow.index

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{Path => HadoopPath}

import com.github.sadikovi.spark.util.Utils

/**
 * [[StatisticsPathStatus]] is a holder of the parameters for statistics file. `path` is a
 * fully-qualified file path, either HDFS or local file system, and `exists` indicates if file for
 * that path exists.
 */
case class StatisticsPathStatus(path: String, exists: Boolean)

/**
 * [[StatisticsPathResolver]] is a simple class to find the statistics path based on a file path.
 * Also takes into account possible root to store/read statistics. Note that root may not
 * necessarily exist.
 */
case class StatisticsPathResolver(maybeRoot: Option[String]) {
  if (maybeRoot.isDefined) {
    require(maybeRoot.get != null && maybeRoot.get.nonEmpty,
      s"Root path is expected to be non-empty, got $maybeRoot")
  }

  /**
   * Return statistics path based on root and file path.
   * If root is not specified statistics file is stored side-by-side with the original file,
   * otherwise, directory structure is replicated starting with root:
   * {{{
   *    val path = "/a/b/c/file"
   *    val root = "/x/y/z"
   *    // then statistics file will stored:
   *    val stats = "/x/y/z/a/b/c/.statistics-file"
   * }}}
   */
  def getStatisticsPath(filePath: String): String = {
    val path = new HadoopPath(filePath)
    maybeRoot match {
      case Some(root) =>
        val rootPath = new HadoopPath(root)
        Utils.withSuffix(HadoopPath.mergePaths(rootPath, path).getParent(),
          getStatisticsName(path.getName)).toString
      case None =>
        Utils.withSuffix(path.getParent(), getStatisticsName(path.getName)).toString
    }
  }

  /** Return statistics path status for a provided root file path */
  def getStatisticsPathStatus(
      filePath: String,
      conf: HadoopConf = new HadoopConf(true)): StatisticsPathStatus = {
    val path = new HadoopPath(getStatisticsPath(filePath))
    val fs = path.getFileSystem(conf)
    StatisticsPathStatus(path.toString, fs.exists(path))
  }

  /** Return statistics name based on original file name */
  private def getStatisticsName(fileName: String): String = s".statistics-$fileName"
}
