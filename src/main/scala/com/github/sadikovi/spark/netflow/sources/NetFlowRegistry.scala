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

import scala.util.{Failure, Success, Try}

import com.github.sadikovi.spark.util.Utils

/** NetFlow registry to resolve provider for a specific version. */
object NetFlowRegistry {
  /**
   * Look up NetFlow interface, also resolve package to full class name, similar to Spark
   * datasource API.
   */
  private[sources] def lookupInterface(provider: String): Class[_] = {
    val extendedProvider = s"${provider}.DefaultProvider"
    val loader = Utils.getContextClassLoader()

    Try(loader.loadClass(provider)).orElse(Try(loader.loadClass(extendedProvider))) match {
      case Success(someInterface) =>
        someInterface
      case Failure(error) =>
        throw new ClassNotFoundException(s"Failed to find NetFlow interface for ${provider}",
          error)
    }
  }

  /**
   * Create interface from specified provider.
   * @param providerName name of the [[NetFlowProvider]] to load
   */
  def createInterface(providerName: String): ResolvedInterface = {
    val provider = lookupInterface(providerName).newInstance() match {
      case versionProvider: NetFlowProvider => versionProvider
      case _ => throw new UnsupportedOperationException(
        s"Provider ${providerName} does not support NetFlowProvider interface")
    }
    provider.createInterface()
  }
}

trait NetFlowProvider {
  /** Return new [[ResolvedInterface]] instance. */
  def createInterface(): ResolvedInterface
}
