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

class NetFlowV7Suite extends UnitTestSpec {

  test("test initialize of NetFlowV7") {
    intercept[UnsupportedOperationException] {
      val fields: Array[Long] = Array()
      val record = new NetFlowV7(fields)
    }

    // throw exception for unknown field
    intercept[UnsupportedOperationException] {
      val fields: Array[Long] = Array(-1L)
      val record = new NetFlowV7(fields)
    }
  }

  test("compute actual size of entire record") {
    val fields = Array(
      NetFlowV7.V7_FIELD_UNIX_SECS,
      NetFlowV7.V7_FIELD_UNIX_NSECS,
      NetFlowV7.V7_FIELD_SYSUPTIME,
      NetFlowV7.V7_FIELD_EXADDR,
      NetFlowV7.V7_FIELD_SRCADDR,
      NetFlowV7.V7_FIELD_DSTADDR,
      NetFlowV7.V7_FIELD_NEXTHOP,
      NetFlowV7.V7_FIELD_DPKTS,
      NetFlowV7.V7_FIELD_DOCTETS,
      NetFlowV7.V7_FIELD_FIRST,
      NetFlowV7.V7_FIELD_LAST,
      NetFlowV7.V7_FIELD_INPUT,
      NetFlowV7.V7_FIELD_OUTPUT,
      NetFlowV7.V7_FIELD_SRCPORT,
      NetFlowV7.V7_FIELD_DSTPORT,
      NetFlowV7.V7_FIELD_SRC_AS,
      NetFlowV7.V7_FIELD_DST_AS,
      NetFlowV7.V7_FIELD_PROT,
      NetFlowV7.V7_FIELD_TOS,
      NetFlowV7.V7_FIELD_TCP_FLAGS,
      NetFlowV7.V7_FIELD_FLAGS,
      NetFlowV7.V7_FIELD_ENGINE_TYPE,
      NetFlowV7.V7_FIELD_ENGINE_ID,
      NetFlowV7.V7_FIELD_SRC_MASK,
      NetFlowV7.V7_FIELD_DST_MASK,
      NetFlowV7.V7_FIELD_ROUTER_SC
    )

    val record = new NetFlowV7(fields)
    // we add one byte as we skip padding field, which is included into record size
    val actualSize = record.actualSize()
    actualSize should be (record.size())
  }
}
