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
private[spark] abstract class PartitionMode {
  def resolveNumPartitions(maxSlices: Int): Int

  def tryToPartition(seq: Seq[NetFlowMetadata]): Seq[Seq[NetFlowMetadata]]
}

/**
 * [[DefaultPartitionMode]] either returns provided number of partitions or maximum slices
 * whichever is larger.
 */
final case class DefaultPartitionMode(numPartitions: Option[Int]) extends PartitionMode {
  override def resolveNumPartitions(maxSlices: Int): Int = numPartitions match {
    case Some(possibleSlices) =>
      require(possibleSlices >= 1, s"Expected at least one partition, got ${possibleSlices}")
      Math.min(possibleSlices, maxSlices)
    case None =>
      maxSlices
  }

  override def tryToPartition(seq: Seq[NetFlowMetadata]): Seq[Seq[NetFlowMetadata]] = {
    val numSlices = resolveNumPartitions(seq.length)
    require(numSlices >= 1, "Positive number of slices required")

    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map(i => {
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      })
    }

    val array = seq.toArray
    positions(array.length, numSlices).map { case (start, end) =>
      array.slice(start, end).toSeq
    }.toSeq
  }
}

/**
 * [[AutoPartitionMode]] allows to customize slices split based on certain constraints, such as
 * maximum number of partitions, and maximum size of each partition.
 */
final case class AutoPartitionMode() extends PartitionMode {
  override def resolveNumPartitions(maxSlices: Int): Int = {
    throw new UnsupportedOperationException(
      s"${getClass().getSimpleName()} does not support partitions resolution")
  }

  override def tryToPartition(seq: Seq[NetFlowMetadata]): Seq[Seq[NetFlowMetadata]] = {
    throw new UnsupportedOperationException("Not supported")
  }
}
