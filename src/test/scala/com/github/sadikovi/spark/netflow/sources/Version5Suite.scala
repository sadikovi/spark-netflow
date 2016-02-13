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

import org.apache.spark.sql.types.StringType

import com.github.sadikovi.netflowlib.version.NetFlowV5
import com.github.sadikovi.testutil.UnitTestSpec

class Version5Suite extends UnitTestSpec {
  // interface for version 5
  val interface = NetFlowRegistry.createInterface("com.github.sadikovi.spark.netflow.version5")

  test("get SQL schema") {
    var schema = interface.getSQLSchema(false)

    schema.length should be (interface.getColumns().length)
    schema.forall(p => p.dataType != StringType)

    schema = interface.getSQLSchema(true)
    schema.length should be (interface.getColumns().length)
    schema.exists(p => p.dataType == StringType)
  }

  test("get mapped columns") {
    interface.getColumns().head should be (interface.getFirstColumn())
  }

  test("get mapped column as option") {
    interface.getFirstColumnOption() should be (Option(interface.getFirstColumn()))
  }

  test("get mapped column for name") {
    intercept[RuntimeException] {
      interface.getColumn("wrong-column")
    }

    interface.getColumn("unix_secs").internalColumnName should be (NetFlowV5.V5_FIELD_UNIX_SECS)
    interface.getColumn("srcip").internalColumnName should be (NetFlowV5.V5_FIELD_SRCADDR)
  }

  test("get conversions for fields") {
    var convertFunction = interface.getColumn("srcip").convertFunction
    convertFunction.isEmpty should be (false)
    convertFunction.get.isInstanceOf[IPConvertFunction] should be (true)

    convertFunction = interface.getColumn("unix_secs").convertFunction
    convertFunction.isEmpty should be (true)
  }
}
