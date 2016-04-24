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

package com.github.sadikovi.spark.netflow.index

import java.nio.ByteOrder

import scala.collection.mutable.HashMap

/**
 * [[AttributeMap]] is a statistics container for attributes. Provides API to register attributes,
 * update statistics for individual attribute and write all attributes into a file.
 */
private[spark] class AttributeMap {
  // Map of attributes, name is considered to be unique key for attribute. Column name is usually
  // used as unique key
  private val map: HashMap[String, Attribute[_]] = HashMap.empty

  /** Return underlying map, for testing purposes only */
  private[index] def getMap() = map

  /** Register new attribute */
  def registerAttribute(attr: Attribute[_]): AttributeMap = {
    map += attr.name -> attr
    this
  }

  /** Register sequence of attributes */
  def registerAttributes(attrs: Seq[Attribute[_]]): AttributeMap = {
    map ++= attrs.map { attr => (attr.name, attr) }
    this
  }

  /** Update statistics for a specific key, no-op if key does not exist */
  def updateStatistics(key: String, value: Any): Unit = {
    if (map.contains(key)) {
      map(key).addValue(value)
    }
  }

  /** Return writer for this attribute map */
  def write(path: String): Unit = {
    val writer = new StatisticsWriter(ByteOrder.BIG_ENDIAN, map.values.toSeq)
    writer.save(path)
  }
}

/**
 * Interface to create or load attribute map.
 * When creating new attribute map returns map with predefined set of statistics.
 */
object AttributeMap {
  def create(): AttributeMap =
    new AttributeMap().registerAttributes(
      Attribute[Long]("unix_secs", 3) ::
      Attribute[Long]("srcip", 6) ::
      Attribute[Long]("dstip", 6) ::
      Attribute[Int]("srcport", 6) ::
      Attribute[Int]("dstport", 6) ::
      Attribute[Short]("protocol", 6) ::
      Nil)

  def read(path: String): AttributeMap = {
    val reader = new StatisticsReader()
    new AttributeMap().registerAttributes(reader.load(path))
  }
}
