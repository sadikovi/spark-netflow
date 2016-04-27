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

import com.github.sadikovi.spark.util.Utils
import com.github.sadikovi.testutil.UnitTestSpec

class PartitionModeSuite extends UnitTestSpec {
  private val autoMode = AutoPartitionMode(100, 4)

  test("auto partition mode - empty sequence") {
    val seq: Seq[NetFlowFileStatus] = Seq.empty
    try {
      val bins = autoMode.tryToPartition(seq)
    } catch {
      case iae: IllegalArgumentException =>
        assert(iae.getMessage().contains("Positive number of slices required"))
      case other: Throwable => throw other
    }
  }

  test("auto partition mode - single element") {
    val seq = Seq(NetFlowFileStatus(5, "", 10, 0, None))
    val bins = autoMode.tryToPartition(seq)
    bins should be (Seq(Seq(NetFlowFileStatus(5, "", 10, 0, None))))
  }

  test("auto partition mode - split file per partition, when <= best size") {
    val seq = Seq(
      NetFlowFileStatus(5, "", 10, 0, None),
      NetFlowFileStatus(5, "", 22, 0, None),
      NetFlowFileStatus(5, "", 150, 0, None)
    )
    val bins = autoMode.tryToPartition(seq)
    bins should be (Seq(Seq(seq(0)), Seq(seq(1)), Seq(seq(2))))
  }

  test("auto partition mode 2 - split file per partition, when <= best size") {
    val seq = Seq(
      NetFlowFileStatus(5, "", 10, 0, None),
      NetFlowFileStatus(5, "", 122, 0, None),
      NetFlowFileStatus(5, "", 150, 0, None),
      NetFlowFileStatus(5, "", 130, 0, None)
    )
    val bins = autoMode.tryToPartition(seq)
    bins should be (Seq(Seq(seq(0)), Seq(seq(1)), Seq(seq(2)), Seq(seq(3))))
  }

  test("auto partition mode - simple split") {
    val seq = Seq(
      NetFlowFileStatus(5, "", 10, 0, None),
      NetFlowFileStatus(5, "", 22, 0, None),
      NetFlowFileStatus(5, "", 150, 0, None),
      NetFlowFileStatus(5, "", 30, 0, None),
      NetFlowFileStatus(5, "", 30, 0, None)
    )
    val bins = autoMode.tryToPartition(seq)
    bins should be (Seq(
      Seq(NetFlowFileStatus(5, "", 150, 0, None)),
      Seq(NetFlowFileStatus(5, "", 30, 0, None),
        NetFlowFileStatus(5, "", 30, 0, None),
        NetFlowFileStatus(5, "", 22, 0, None),
        NetFlowFileStatus(5, "", 10, 0, None))
    ))
  }

  test("auto partition mode - complex split 1") {
    val seq = Seq(
      NetFlowFileStatus(5, "", 10, 0, None),
      NetFlowFileStatus(5, "", 22, 0, None),
      NetFlowFileStatus(5, "", 150, 0, None),
      NetFlowFileStatus(5, "", 200, 0, None),
      NetFlowFileStatus(5, "", 1, 0, None),
      NetFlowFileStatus(5, "", 5, 0, None),
      NetFlowFileStatus(5, "", 7, 0, None),
      NetFlowFileStatus(5, "", 19, 0, None),
      NetFlowFileStatus(5, "", 90, 0, None),
      NetFlowFileStatus(5, "", 4, 0, None),
      NetFlowFileStatus(5, "", 4, 0, None),
      NetFlowFileStatus(5, "", 4, 0, None),
      NetFlowFileStatus(5, "", 19, 0, None),
      NetFlowFileStatus(5, "", 50, 0, None),
      NetFlowFileStatus(5, "", 45, 0, None),
      NetFlowFileStatus(5, "", 39, 0, None),
      NetFlowFileStatus(5, "", 39, 0, None),
      NetFlowFileStatus(5, "", 43, 0, None),
      NetFlowFileStatus(5, "", 21, 0, None),
      NetFlowFileStatus(5, "", 45, 0, None),
      NetFlowFileStatus(5, "", 45, 0, None),
      NetFlowFileStatus(5, "", 90, 0, None),
      NetFlowFileStatus(5, "", 10, 0, None),
      NetFlowFileStatus(5, "", 11, 0, None)
    )
    val bins = autoMode.tryToPartition(seq)
    bins should be (Seq(
      Seq(NetFlowFileStatus(5, "", 200, 0, None)),
      Seq(NetFlowFileStatus(5, "", 150, 0, None)),
      Seq(NetFlowFileStatus(5, "", 90, 0, None),
        NetFlowFileStatus(5, "", 1, 0, None),
        NetFlowFileStatus(5, "", 4, 0, None),
        NetFlowFileStatus(5, "", 4, 0, None)),
      Seq(NetFlowFileStatus(5, "", 90, 0, None),
        NetFlowFileStatus(5, "", 4, 0, None),
        NetFlowFileStatus(5, "", 5, 0, None)),
      Seq(NetFlowFileStatus(5, "", 50, 0, None),
        NetFlowFileStatus(5, "", 7, 0, None),
        NetFlowFileStatus(5, "", 10, 0, None),
        NetFlowFileStatus(5, "", 10, 0, None),
        NetFlowFileStatus(5, "", 11, 0, None)),
      Seq(NetFlowFileStatus(5, "", 45, 0, None),
        NetFlowFileStatus(5, "", 19, 0, None),
        NetFlowFileStatus(5, "", 19, 0, None)),
      Seq(NetFlowFileStatus(5, "", 45, 0, None),
        NetFlowFileStatus(5, "", 21, 0, None),
        NetFlowFileStatus(5, "", 22, 0, None)),
      Seq(NetFlowFileStatus(5, "", 45, 0, None),
        NetFlowFileStatus(5, "", 43, 0, None)),
      Seq(NetFlowFileStatus(5, "", 39, 0, None),
        NetFlowFileStatus(5, "", 39, 0, None))
    ))
  }

  test("auto partition mode - complex split 2") {
    val seq = Seq(
      NetFlowFileStatus(5, "", 10, 0, None),
      NetFlowFileStatus(5, "", 22, 0, None),
      NetFlowFileStatus(5, "", 150, 0, None),
      NetFlowFileStatus(5, "", 200, 0, None),
      NetFlowFileStatus(5, "", 120, 0, None),
      NetFlowFileStatus(5, "", 500, 0, None),
      NetFlowFileStatus(5, "", 170, 0, None),
      NetFlowFileStatus(5, "", 190, 0, None),
      NetFlowFileStatus(5, "", 190, 0, None),
      NetFlowFileStatus(5, "", 4, 0, None),
      NetFlowFileStatus(5, "", 4, 0, None),
      NetFlowFileStatus(5, "", 4, 0, None),
      NetFlowFileStatus(5, "", 19, 0, None),
      NetFlowFileStatus(5, "", 50, 0, None),
      NetFlowFileStatus(5, "", 45, 0, None),
      NetFlowFileStatus(5, "", 39, 0, None),
      NetFlowFileStatus(5, "", 39, 0, None),
      NetFlowFileStatus(5, "", 430, 0, None),
      NetFlowFileStatus(5, "", 210, 0, None),
      NetFlowFileStatus(5, "", 240, 0, None),
      NetFlowFileStatus(5, "", 145, 0, None),
      NetFlowFileStatus(5, "", 190, 0, None),
      NetFlowFileStatus(5, "", 100, 0, None),
      NetFlowFileStatus(5, "", 110, 0, None)
    )
    val bins = autoMode.tryToPartition(seq)
    bins should be (Seq(
      Seq(NetFlowFileStatus(5, "", 500, 0, None)),
      Seq(NetFlowFileStatus(5, "", 430, 0, None)),
      Seq(NetFlowFileStatus(5, "", 240, 0, None)),
      Seq(NetFlowFileStatus(5, "", 210, 0, None)),
      Seq(NetFlowFileStatus(5, "", 200, 0, None)),
      Seq(NetFlowFileStatus(5, "", 190, 0, None)),
      Seq(NetFlowFileStatus(5, "", 190, 0, None)),
      Seq(NetFlowFileStatus(5, "", 190, 0, None)),
      Seq(NetFlowFileStatus(5, "", 170, 0, None)),
      Seq(NetFlowFileStatus(5, "", 150, 0, None)),
      Seq(NetFlowFileStatus(5, "", 145, 0, None)),
      Seq(NetFlowFileStatus(5, "", 120, 0, None)),
      Seq(NetFlowFileStatus(5, "", 110, 0, None)),
      Seq(NetFlowFileStatus(5, "", 100, 0, None)),
      Seq(NetFlowFileStatus(5, "", 50, 0, None),
        NetFlowFileStatus(5, "", 4, 0, None),
        NetFlowFileStatus(5, "", 4, 0, None),
        NetFlowFileStatus(5, "", 4, 0, None),
        NetFlowFileStatus(5, "", 10, 0, None),
        NetFlowFileStatus(5, "", 19, 0, None)),
      Seq(NetFlowFileStatus(5, "", 45, 0, None),
        NetFlowFileStatus(5, "", 39, 0, None)),
      Seq(NetFlowFileStatus(5, "", 39, 0, None),
        NetFlowFileStatus(5, "", 22, 0, None))
    ))
  }

  test("auto partition mode - 2 buckets split") {
    val seq = Seq(
      NetFlowFileStatus(5, "", 80, 0, None),
      NetFlowFileStatus(5, "", 40, 0, None),
      NetFlowFileStatus(5, "", 40, 0, None),
      NetFlowFileStatus(5, "", 20, 0, None),
      NetFlowFileStatus(5, "", 20, 0, None)
    )
    val bins = autoMode.tryToPartition(seq)
    bins should be (Seq(
      Seq(NetFlowFileStatus(5, "", 80, 0, None),
        NetFlowFileStatus(5, "", 20, 0, None)),
      Seq(NetFlowFileStatus(5, "", 40, 0, None),
        NetFlowFileStatus(5, "", 40, 0, None),
        NetFlowFileStatus(5, "", 20, 0, None))
    ))
  }

  test("auto partition mode - 2 buckets unequal split") {
    val seq = Seq(
      NetFlowFileStatus(5, "", 80, 0, None),
      NetFlowFileStatus(5, "", 19, 0, None),
      NetFlowFileStatus(5, "", 10, 0, None),
      NetFlowFileStatus(5, "", 4, 0, None),
      NetFlowFileStatus(5, "", 5, 0, None)
    )
    val bins = autoMode.tryToPartition(seq)
    bins should be (Seq(
      Seq(NetFlowFileStatus(5, "", 80, 0, None),
        NetFlowFileStatus(5, "", 19, 0, None)),
      Seq(NetFlowFileStatus(5, "", 10, 0, None),
        NetFlowFileStatus(5, "", 5, 0, None),
        NetFlowFileStatus(5, "", 4, 0, None))
    ))
  }

  test("default partition mode - None as number of slices") {
    val defaultMode = DefaultPartitionMode(None)
    val seq = Seq(
      NetFlowFileStatus(5, "", 84, 0, None),
      NetFlowFileStatus(5, "", 12, 0, None),
      NetFlowFileStatus(5, "", 15, 0, None),
      NetFlowFileStatus(5, "", 3, 0, None)
    )
    val bins = defaultMode.tryToPartition(seq)
    bins should be (Seq(
      Seq(NetFlowFileStatus(5, "", 84, 0, None)),
      Seq(NetFlowFileStatus(5, "", 12, 0, None)),
      Seq(NetFlowFileStatus(5, "", 15, 0, None)),
      Seq(NetFlowFileStatus(5, "", 3, 0, None))
    ))
  }

  test("default partition mode - 1 partition") {
    val defaultMode = DefaultPartitionMode(Option(1))
    val seq = Seq(
      NetFlowFileStatus(5, "", 84, 0, None),
      NetFlowFileStatus(5, "", 12, 0, None),
      NetFlowFileStatus(5, "", 15, 0, None),
      NetFlowFileStatus(5, "", 3, 0, None)
    )
    val bins = defaultMode.tryToPartition(seq)
    bins should be (Seq(
      Seq(
        NetFlowFileStatus(5, "", 84, 0, None),
        NetFlowFileStatus(5, "", 12, 0, None),
        NetFlowFileStatus(5, "", 15, 0, None),
        NetFlowFileStatus(5, "", 3, 0, None)
      )
    ))
  }

  test("default partition mode - negative number of slices") {
    try {
      val defaultMode = DefaultPartitionMode(Option(-1))
    } catch {
      case iae: IllegalArgumentException =>
        assert(iae.getMessage().contains("Expected at least one partition, got -1"))
      case other: Throwable => throw other
    }
  }
}
