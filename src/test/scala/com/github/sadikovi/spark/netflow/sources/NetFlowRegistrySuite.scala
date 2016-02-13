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

import com.github.sadikovi.testutil.UnitTestSpec

/** Test providers */
class DefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = null
}

class WrongProvider

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
    val interface = NetFlowRegistry.createInterface(
      "com.github.sadikovi.spark.netflow.sources.DefaultProvider")
    Option(interface) should be (None)

    intercept[UnsupportedOperationException] {
      NetFlowRegistry.createInterface(
        "com.github.sadikovi.spark.netflow.sources.WrongProvider")
    }
  }
}
