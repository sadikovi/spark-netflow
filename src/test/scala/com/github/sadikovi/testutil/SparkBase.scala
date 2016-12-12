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

package com.github.sadikovi.testutil

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/** General Spark base */
private[testutil] trait SparkBase {
  @transient private[testutil] var _sc: SparkContext = null

  /** Initialize Spark context with default parameters */
  def startSparkContext() {
    startSparkContext(Map.empty)
  }

  /** Start (or init) Spark context. */
  def startSparkContext(sparkOptions: Map[String, String]) {
    // stop previous Spark context
    stopSparkContext()
    _sc = new SparkContext()
  }

  /** Stop Spark context. */
  def stopSparkContext() {
    if (_sc != null) {
      _sc.stop()
    }
    _sc = null
  }

  /**
   * Set logging level globally for all.
   * Supported log levels:
   *      Level.OFF
   *      Level.ERROR
   *      Level.WARN
   *      Level.INFO
   * @param level logging level
   */
  def setLoggingLevel(level: Level) {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
    Logger.getRootLogger().setLevel(level)
  }

  /** Returns Spark context. */
  def sc: SparkContext = _sc
}
