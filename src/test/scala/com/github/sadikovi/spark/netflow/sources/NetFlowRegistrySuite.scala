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

/** Test providers */
private class DefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = null
}

private class WrongProvider

private class TestEmptyDefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = new TestEmptyInterface()
}

private class TestFullDefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = new TestFullInterface()
}

/** Test interfaces */
private class TestEmptyInterface extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq.empty

  override def version(): Short = -1
}

private class TestFullInterface extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq(
    MappedColumn("a", 1L, LongType, false, None))

  override def version(): Short = -2
}

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
    var interface = NetFlowRegistry.createInterface(
      "com.github.sadikovi.spark.netflow.sources.TestEmptyDefaultProvider")
    Option(interface).isEmpty should be (false)

    interface = NetFlowRegistry.createInterface(
      "com.github.sadikovi.spark.netflow.sources.DefaultProvider")
    Option(interface).isEmpty should be (true)

    intercept[UnsupportedOperationException] {
      NetFlowRegistry.createInterface(
        "com.github.sadikovi.spark.netflow.sources.WrongProvider")
    }
  }

  test("check column consistency") {
    var interface = NetFlowRegistry.createInterface(
      "com.github.sadikovi.spark.netflow.sources.TestEmptyDefaultProvider")
    intercept[IllegalArgumentException] {
      interface.ensureColumnConsistency()
    }

    interface = NetFlowRegistry.createInterface(
      "com.github.sadikovi.spark.netflow.sources.TestFullDefaultProvider")
    // should not raise any errors
    interface.ensureColumnConsistency()
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
