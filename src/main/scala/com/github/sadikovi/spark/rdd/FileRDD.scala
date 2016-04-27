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

package com.github.sadikovi.spark.rdd

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkContext, Dependency, OneToOneDependency}
import org.apache.spark.rdd.RDD

import com.github.sadikovi.spark.util.SerializableConfiguration

/**
 * Abstract class that provides common API for RDDs that work with files. Implicitly uses default
 * configuration as `SparkContext.hadoopConfiguration` that will be serialized to send to all
 * workers.
 */
private[spark] abstract class FileRDD[T : ClassTag](
    @transient sc: SparkContext,
    @transient deps: Seq[Dependency[_]])
    (implicit _conf: Configuration = sc.hadoopConfiguration) extends RDD[T](sc, deps) {
  private val confBroadcast = sparkContext.broadcast(new SerializableConfiguration(_conf))

  def getConf(): Configuration = confBroadcast.value.value

  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))
}
