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

package com.github.sadikovi.spark

import org.apache.spark.sql.{SQLContext, DataFrameReader, DataFrame}

package object netflow {

  /**
   * Adds a shortcut method `netflow()` to DataFrameReader that allows to omit specifying format
   * and specifies version "5" by default.
   */
  implicit class NetFlowDataFrameReader(reader: DataFrameReader) {
    def netflow: String => DataFrame = {
      reader.format("com.github.sadikovi.spark.netflow").option("version", "5").load
    }
  }
}
