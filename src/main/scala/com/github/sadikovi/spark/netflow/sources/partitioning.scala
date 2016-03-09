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

/**
 * [[PartitionMode]] interface defines how many partitions and how data is sorted into those
 * buckets, provides common API to either return appropriate number of partitions or slice list of
 * items into partitions.
 */
private[netflow] abstract class PartitionMode {
  def resolveNumPartitions(max: Int): Int
}

/**
 * [[SimplePartitionMode]] always returns the maximum number of partitions available.
 *
 */
final case class SimplePartitionMode() extends PartitionMode {
  override def resolveNumPartitions(max: Int): Int = max
}

/**
 * [[DefaultPartitionMode]] either returns provided number of partitions or maximum slices
 * whichever is larger.
 *
 */
final case class DefaultPartitionMode(numPartitions: Int) extends PartitionMode {
  override def resolveNumPartitions(max: Int): Int = {
    require(numPartitions >= 1, s"Expected at least one partition, got ${numPartitions}")
    Math.min(numPartitions, max)
  }
}

/**
 * [[AutoPartitionMode]] allows to customize slices split based on certain constraints, such as
 * maximum number of partitions, and maximum size of each partition.
 *
 */
final case class AutoPartitionMode() extends PartitionMode {
  override def resolveNumPartitions(max: Int): Int = {
    throw new UnsupportedOperationException(
      s"${getClass().getSimpleName()} does not support partitions resolution")
  }
}
