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

import com.github.sadikovi.netflowlib.version.{NetFlowV5, NetFlowV7}
import com.github.sadikovi.testutil.UnitTestSpec

class VersionSuite extends UnitTestSpec {
  // interface for version 5
  val interface5 = NetFlowRegistry.createInterface("com.github.sadikovi.spark.netflow.version5")
  val interface7 = NetFlowRegistry.createInterface("com.github.sadikovi.spark.netflow.version7")

  test("get SQL schema v5") {
    var schema = interface5.getSQLSchema(false)

    schema.length should be (interface5.getColumns().length)
    schema.forall(p => p.dataType != StringType)

    schema = interface5.getSQLSchema(true)
    schema.length should be (interface5.getColumns().length)
    schema.exists(p => p.dataType == StringType)
  }

  test("get SQL schema v7") {
    var schema = interface7.getSQLSchema(false)

    schema.length should be (interface7.getColumns().length)
    schema.forall(p => p.dataType != StringType)

    schema = interface7.getSQLSchema(true)
    schema.length should be (interface7.getColumns().length)
    schema.exists(p => p.dataType == StringType)
  }

  test("get mapped columns") {
    interface5.getColumns().head should be (interface5.getFirstColumn())
  }

  test("get mapped column as option") {
    interface5.getFirstColumnOption() should be (Some(interface5.getFirstColumn()))
  }

  test("get mapped column for name v5") {
    intercept[RuntimeException] {
      interface5.getColumn("wrong-column")
    }

    interface5.getColumn("unix_secs").internalColumn should be (NetFlowV5.FIELD_UNIX_SECS)
    interface5.getColumn("srcip").internalColumn should be (NetFlowV5.FIELD_SRCADDR)
  }

  test("get mapped column for name v7") {
    intercept[RuntimeException] {
      interface7.getColumn("wrong-column")
    }

    interface7.getColumn("unix_secs").internalColumn should be (NetFlowV7.FIELD_UNIX_SECS)
    interface7.getColumn("srcip").internalColumn should be (NetFlowV7.FIELD_SRCADDR)
    interface7.getColumn("router_sc").internalColumn should be (NetFlowV7.FIELD_ROUTER_SC)
  }

  test("get conversions for fields") {
    var convertFunction = interface5.getColumn("srcip").convertFunction
    convertFunction.isEmpty should be (false)
    convertFunction.get.isInstanceOf[IPv4ConvertFunction] should be (true)

    convertFunction = interface5.getColumn("unix_secs").convertFunction
    convertFunction.isEmpty should be (true)

    // get conversions for "router_sc", similar to "srcip"
    convertFunction = interface7.getColumn("router_sc").convertFunction
    convertFunction.isEmpty should be (false)
  }
}
