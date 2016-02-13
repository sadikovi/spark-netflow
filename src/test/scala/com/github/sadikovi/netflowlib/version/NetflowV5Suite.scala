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

package com.github.sadikovi.netflowlib.version

import com.github.sadikovi.testutil.UnitTestSpec

class NetFlowV5Suite extends UnitTestSpec {

  test("test initialize of NetFlowV5") {
    intercept[UnsupportedOperationException] {
      val fields: Array[Long] = Array()
      val record = new NetFlowV5(fields)
    }

    // throw exception for unknown field
    intercept[UnsupportedOperationException] {
      val fields: Array[Long] = Array(-1L)
      val record = new NetFlowV5(fields)
    }
  }

  test("compute actual size of entire record") {
    val fields = Array(
      NetFlowV5.V5_FIELD_UNIX_SECS,
      NetFlowV5.V5_FIELD_UNIX_NSECS,
      NetFlowV5.V5_FIELD_SYSUPTIME,
      NetFlowV5.V5_FIELD_EXADDR,
      NetFlowV5.V5_FIELD_SRCADDR,
      NetFlowV5.V5_FIELD_DSTADDR,
      NetFlowV5.V5_FIELD_NEXTHOP,
      NetFlowV5.V5_FIELD_DPKTS,
      NetFlowV5.V5_FIELD_DOCTETS,
      NetFlowV5.V5_FIELD_FIRST,
      NetFlowV5.V5_FIELD_LAST,
      NetFlowV5.V5_FIELD_INPUT,
      NetFlowV5.V5_FIELD_OUTPUT,
      NetFlowV5.V5_FIELD_SRCPORT,
      NetFlowV5.V5_FIELD_DSTPORT,
      NetFlowV5.V5_FIELD_SRC_AS,
      NetFlowV5.V5_FIELD_DST_AS,
      NetFlowV5.V5_FIELD_PROT,
      NetFlowV5.V5_FIELD_TOS,
      NetFlowV5.V5_FIELD_TCP_FLAGS,
      NetFlowV5.V5_FIELD_ENGINE_TYPE,
      NetFlowV5.V5_FIELD_ENGINE_ID,
      NetFlowV5.V5_FIELD_SRC_MASK,
      NetFlowV5.V5_FIELD_DST_MASK
    )

    val record = new NetFlowV5(fields)
    // we add one byte as we skip padding field, which is included into record size
    val actualSize = record.actualSize() + 1
    actualSize should be (record.size())
  }
}
