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

package com.github.sadikovi.spark.netflow.sources

/**
 * NetFlow file status that describes file to process. Contains expected version of a file,
 * absolute resolved path to the file, and it's length, buffer size for a particular file
 * (currently we do not make that distinction, so buffer size is the same across all files).
 * @param version version of NetFlow, e.g. 5, or 7
 * @param path absolute file path, in case of HDFS includes host and port
 * @param length file size in bytes
 * @param bufferSize buffer size (when file stream is compressed) in bytes
 * @param statisticsPath absolute file path to the statistics file
 */
private[spark] case class NetFlowFileStatus(
  version: Short,
  path: String,
  length: Long,
  bufferSize: Int,
  statisticsPath: Option[String]
)
