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

import scala.collection.mutable.ArrayBuffer

import com.github.sadikovi.spark.util.Utils

/**
 * [[PartitionMode]] interface defines how many partitions and how data is sorted into those
 * buckets, provides common API to either return appropriate number of partitions or slice list of
 * items into partitions.
 */
private[spark] abstract class PartitionMode {
  /** Return appropriate number of partitions for mode */
  def resolveNumPartitions(maxSlices: Int): Int

  /** Try partitioning sequence of metadata depending on mode */
  def tryToPartition(seq: Seq[NetFlowFileStatus]): Seq[Seq[NetFlowFileStatus]]

  /** Standard slice function to partition sequence */
  protected def slice(seq: Seq[NetFlowFileStatus], numSlices: Int): Seq[Seq[NetFlowFileStatus]] = {
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
 * [[DefaultPartitionMode]] either returns provided number of partitions or maximum slices
 * whichever is larger.
 * @param numPartitions number of partitions specified, if equals to None, then each entry goes
 * into its own partition
 */
final case class DefaultPartitionMode(numPartitions: Option[Int]) extends PartitionMode {
  override def resolveNumPartitions(maxSlices: Int): Int = numPartitions match {
    case Some(possibleSlices) =>
      require(possibleSlices >= 1, s"Expected at least one partition, got ${possibleSlices}")
      Math.min(possibleSlices, maxSlices)
    case None =>
      maxSlices
  }

  override def tryToPartition(seq: Seq[NetFlowFileStatus]): Seq[Seq[NetFlowFileStatus]] = {
    val numSlices = resolveNumPartitions(seq.length)
    slice(seq, numSlices)
  }

  override def toString(): String = {
    val msg = if (numPartitions.isEmpty) {
      "None, will apply trivial partitioning"
    } else {
      s"${numPartitions.get} partitions"
    }
    s"${getClass().getSimpleName()}[${msg}]"
  }
}

/**
 * [[AutoPartitionMode]] allows to customize slices split based on certain constraints, such as
 * maximum number of partitions, and maximum size of each partition.
 * @param partitionSize partition size (best bucket size) to compare
 * @param minNumPartitions minimum number of partitions, after which auto partitioning is applied
 */
final case class AutoPartitionMode(
    partitionSize: Long,
    minNumPartitions: Int) extends PartitionMode {
  override def resolveNumPartitions(maxSlices: Int): Int = {
    Math.max(minNumPartitions, maxSlices)
  }

  override def tryToPartition(seq: Seq[NetFlowFileStatus]): Seq[Seq[NetFlowFileStatus]] = {
    val maxSlices = seq.length
    // If number of actual slices is less than given number of partitions then we partition actual
    // sequence similar to default mode, since there is no way for us to do anything more efficient,
    // than utilizing whole cluster.
    if (resolveNumPartitions(maxSlices) <= minNumPartitions) {
      slice(seq, maxSlices)
    } else {
      // Sequence of file sizes to find real mean
      val sizes = seq.map { meta => meta.length }.toArray

      // When computing real mean we assume that skewness can be at most 14% of the dataset
      def realMean(arr: Array[Long]): Long = {
        val means = Array(1.00, 0.98, 0.96, 0.94, 0.92, 0.90, 0.88, 0.86).map { sample =>
          Utils.truncatedMean(arr, sample) }
        Utils.histogramMode(means)
      }

      // Final partition size that will be used for grouping
      val bestPartitionSize = Math.max(partitionSize, realMean(sizes))
      // All buckets that will be created
      val buckets = new ArrayBuffer[ArrayBuffer[NetFlowFileStatus]]()
      // Data sorted in descending order
      val srtData = seq.toArray.sortWith(_.length > _.length)

      // Create buckets for files that are larger or equal to partition size. Because we cannot
      // split files currently, we just put them into separate buckets.
      var index = 0
      while (srtData(index).length >= bestPartitionSize && index < maxSlices) {
        val bucket = new ArrayBuffer[NetFlowFileStatus](1)
        bucket.append(srtData(index))
        buckets.append(bucket)
        index += 1
      }

      // All elements in resulting sequence are less than best partition size
      var restData = srtData.drop(index)
      // Check if we end up with trivial solution with less than or equal to 2 buckets
      var isTrivial = false
      while (!isTrivial) {
        val totalSum = restData.map(_.length).sum
        // If we either have 0 elements or 2 buckets at most, then we exit adding new buckets
        if (totalSum == 0 || totalSum <= 2 * bestPartitionSize) {
          isTrivial = true
        } else {
          val leftmostElement = restData.head
          var delta = bestPartitionSize - leftmostElement.length
          val currentBucket = new ArrayBuffer[NetFlowFileStatus]()
          currentBucket.append(leftmostElement)

          // Update data, since we removed element
          restData = restData.tail

          // See, if we can add more elements into the buffer
          while (delta > 0 && restData.nonEmpty) {
            val smallElement = restData.last
            delta = delta - smallElement.length

            if (delta >= 0) {
              currentBucket.append(smallElement)
              restData = restData.dropRight(1)
            }
          }

          // Add new bucket to the list of buckets
          buckets.append(currentBucket)
        }
      }

      // At this stage we have to resolve left elements into 2 buckets, array is still in
      // decreasing order
      if (restData.nonEmpty) {
        val bucket1 = new ArrayBuffer[NetFlowFileStatus]()
        val bucket2 = new ArrayBuffer[NetFlowFileStatus]()

        val firstElem = restData.head
        var bucket1Sum = firstElem.length
        bucket1.append(firstElem)

        // If bucket size is greater than best partition size then we put element into second
        // bucket, thus ensure that sorting into buckets is optimal, or follows rule of partition
        // being as close as possible to best size.
        for (i <- 1 until restData.length) {
          val elem = restData(i)
          if (elem.length + bucket1Sum <= bestPartitionSize) {
            bucket1Sum = bucket1Sum + elem.length
            bucket1.append(elem)
          } else {
            bucket2.append(elem)
          }
        }

        if (bucket1.nonEmpty) {
          buckets.append(bucket1)
        }

        if (bucket2.nonEmpty) {
          buckets.append(bucket2)
        }
      }

      // Convert buckets buffer into return type of sequences
      buckets.map(_.toSeq).toSeq
    }
  }

  override def toString(): String = {
    s"${getClass().getSimpleName()}[${partitionSize} bytes, ${minNumPartitions} partitions]"
  }
}
