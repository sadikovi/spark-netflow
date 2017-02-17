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

import com.github.sadikovi.netflowlib.Buffers.RecordBuffer
import com.github.sadikovi.spark.util.Utils

/**
 * [[NetFlowOptions]] consolidates all options available for NetFlow file format and can be
 * set as datasource options for `DataFrameReader`.
 */
private[netflow] class NetFlowOptions(options: Map[String, String]) extends Serializable {
  // Conversion of some supported numeric fields into string, such as IP, enabled by default
  val applyConversion = options.get("stringify") match {
    case Some("true") => true
    case Some("false") => false
    case _ => true
  }

  // Buffer size in bytes, by default use standard record buffer size ~1Mb
  val bufferSize = options.get("buffer") match {
    case Some(str) =>
      val bytes = Utils.byteStringAsBytes(str)
      if (bytes > Integer.MAX_VALUE) {
        sys.error(s"Buffer size (${bytes}) bytes > maximum buffer size " +
          s"(${Integer.MAX_VALUE} bytes)")
      } else if (bytes < RecordBuffer.MIN_BUFFER_LENGTH) {
        sys.error(s"Buffer size (${bytes} bytes) < minimum buffer size " +
          s"(${RecordBuffer.MIN_BUFFER_LENGTH} bytes)")
      } else {
        bytes.toInt
      }
    case None =>
      RecordBuffer.BUFFER_LENGTH_2
  }

  // Whether or not to use predicate pushdown at the NetFlow library level, enabled by default
  val usePredicatePushdown = options.get("predicate-pushdown") match {
    case Some("true") => true
    case Some("false") => false
    case _ => true
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}(" +
      s"applyConversion=$applyConversion, " +
      s"bufferSize=$bufferSize, " +
      s"usePredicatePushdown=$usePredicatePushdown)"
  }
}
