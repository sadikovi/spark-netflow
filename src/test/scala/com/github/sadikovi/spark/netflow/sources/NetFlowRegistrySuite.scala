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

import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType, StringType}

import com.github.sadikovi.testutil.UnitTestSpec

class NetFlowRegistrySuite extends UnitTestSpec {
  test("resolve test provider") {
    // resolve full class name for provider
    var provider = NetFlowRegistry.lookupInterface(
      "com.github.sadikovi.spark.netflow.sources.DefaultProvider")
    Option(provider).nonEmpty should be (true)

    // resolve package name for provider
    provider = NetFlowRegistry.lookupInterface(
      "com.github.sadikovi.spark.netflow.sources")
    Option(provider).nonEmpty should be (true)

    intercept[ClassNotFoundException] {
      NetFlowRegistry.lookupInterface("wrong.package")
    }
  }

  test("resolve test interface") {
    // resolve full class name for provider
    var interface: ResolvedInterface = null

    try {
      interface = NetFlowRegistry.createInterface(
        "com.github.sadikovi.spark.netflow.sources.TestEmptyDefaultProvider")
    } catch {
      case iae: IllegalArgumentException =>
        assert(iae.getMessage() == "Columns are empty for Interface: " +
          "com.github.sadikovi.spark.netflow.sources.TestEmptyInterface for version -1")
      case other: Throwable => throw other
    }

    intercept[NullPointerException] {
      interface = NetFlowRegistry.createInterface(
        "com.github.sadikovi.spark.netflow.sources.DefaultProvider")
    }

    intercept[UnsupportedOperationException] {
      NetFlowRegistry.createInterface(
        "com.github.sadikovi.spark.netflow.sources.WrongProvider")
    }

    interface = NetFlowRegistry.createInterface(
      "com.github.sadikovi.spark.netflow.sources.TestFullDefaultProvider")
    Option(interface).isEmpty should be (false)
  }

  test("check column consistency") {
    var interface: ResolvedInterface = null

    intercept[IllegalArgumentException] {
      NetFlowRegistry.createInterface(
        "com.github.sadikovi.spark.netflow.sources.TestEmptyDefaultProvider")
    }

    interface = NetFlowRegistry.createInterface(
      "com.github.sadikovi.spark.netflow.sources.TestFullDefaultProvider")
    // should not raise any errors
    interface.ensureColumnConsistency()

    // should fail with assertion error on duplicate columns
    intercept[AssertionError] {
      interface = NetFlowRegistry.createInterface(
        "com.github.sadikovi.spark.netflow.sources.Test1FullDefaultProvider")
    }

    // should fail with assertion error on duplicate internal column names
    intercept[AssertionError] {
      interface = NetFlowRegistry.createInterface(
        "com.github.sadikovi.spark.netflow.sources.Test2FullDefaultProvider")
    }
  }

  test("size in bytes for different types") {
    val interface = NetFlowRegistry.createInterface(
      "com.github.sadikovi.spark.netflow.sources.TestFullDefaultProvider")

    interface.sizeInBytes(ByteType) should be (1)
    interface.sizeInBytes(ShortType) should be (2)
    interface.sizeInBytes(IntegerType) should be (4)
    interface.sizeInBytes(LongType) should be (8)

    intercept[UnsupportedOperationException] {
      interface.sizeInBytes(StringType)
    }
  }
}
