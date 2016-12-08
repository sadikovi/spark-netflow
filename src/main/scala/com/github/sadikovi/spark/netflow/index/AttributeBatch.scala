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

import org.apache.hadoop.conf.{Configuration => HadoopConf}

/**
 * [[AttributeBatch]] is a statistics container for attributes. Provides API to register attributes,
 * update statistics for individual attribute and write all attributes into a file.
 */
private[spark] class AttributeBatch {
  // Map of attributes, name is considered to be unique key for attribute. Column name is usually
  // used as unique key
  private val map: HashMap[String, Attribute[_]] = HashMap.empty

  /** Return underlying map, for testing purposes only */
  private[index] def getMap() = map

  /** Register new attribute */
  def registerAttribute(attr: Attribute[_]): AttributeBatch = {
    map += attr.name -> attr
    this
  }

  /** Register sequence of attributes */
  def registerAttributes(attrs: Seq[Attribute[_]]): AttributeBatch = {
    map ++= attrs.map { attr => (attr.name, attr) }
    this
  }

  /** Update statistics for a specific key, no-op if key does not exist */
  def updateStatistics(key: String, value: Any): Unit = {
    if (map.contains(key)) {
      map(key).addValue(value)
    }
  }

  /** Query attribute for unresolved value */
  private def query(
      key: String, value: Any)(func: (Attribute[_], Any) => Option[Boolean]): Option[Boolean] = {
    val attribute = map.get(key)
    if (attribute.isEmpty) None else func(attribute.get, value)
  }

  /** Check if value is in attribute range for a specified key */
  def between(key: String, value: Any): Option[Boolean] = {
    query(key, value) { (attr, v) => attr.containsInRange(v) }
  }

  /** Check if value is less than attribute max for a specified key */
  def ltMax(key: String, value: Any): Option[Boolean] = {
    query(key, value) { (attr, v) => attr.lessThanMax(v) }
  }

  /** Check if value is less than or equal to attribute max for a specified key */
  def leMax(key: String, value: Any): Option[Boolean] = {
    query(key, value) { (attr, v) => attr.lessOrEqualMax(v) }
  }

  /** Check if value is greater than attribute min for a specified key */
  def gtMin(key: String, value: Any): Option[Boolean] = {
    query(key, value) { (attr, v) => attr.greaterThanMin(v) }
  }

  /** Check if value is greater than or equal to attribute min for a specified key */
  def geMin(key: String, value: Any): Option[Boolean] = {
    query(key, value) { (attr, v) => attr.greaterOrEqualMin(v) }
  }

  /** Check if value is in attribute set for a specified key */
  def in(key: String, value: Any): Option[Boolean] = {
    query(key, value) { (attr, v) => attr.containsInSet(v) }
  }

  /** Return writer for this attribute batch */
  def write(path: String, conf: HadoopConf, overwrite: Boolean = false): Unit = {
    val writer = new StatisticsWriter(ByteOrder.BIG_ENDIAN, map.values.toSeq)
    writer.save(path, conf, overwrite)
  }

  def write(path: String): Unit = {
    write(path, new HadoopConf(true), overwrite = false)
  }
}

/**
 * Interface to create or load attribute batch.
 * When creating new attribute batch returns map with predefined set of statistics.
 * Note that number, name and type of attributes should be in sync with resolved interface columns,
 * meaning that columns with attribute names should have statistics enabled, otherwise it will not
 * collect anything, resulting in incorrect filtering.
 */
object AttributeBatch {
  def create(): AttributeBatch =
    new AttributeBatch().registerAttributes(
      Attribute[Long]("unix_secs", 1) ::
      Attribute[Long]("srcip", 6) ::
      Attribute[Long]("dstip", 6) ::
      Attribute[Int]("srcport", 6) ::
      Attribute[Int]("dstport", 6) ::
      Attribute[Short]("protocol", 6) ::
      Nil)

  def empty(): AttributeBatch = new AttributeBatch()

  def read(path: String, conf: HadoopConf): AttributeBatch = {
    val reader = new StatisticsReader()
    new AttributeBatch().registerAttributes(reader.load(path, conf))
  }

  def read(path: String): AttributeBatch = {
    read(path, new HadoopConf(true))
  }
}
