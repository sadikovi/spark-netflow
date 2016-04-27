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

package com.github.sadikovi.spark.netflow.version5

import com.github.sadikovi.netflowlib.version.NetFlowV5
import com.github.sadikovi.spark.netflow.sources._

class DefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = {
    new InterfaceV5()
  }
}

/** NetFlow interface for version 5. */
private class InterfaceV5 extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq(
    MappedColumn("unix_secs", NetFlowV5.FIELD_UNIX_SECS, true, None),
    MappedColumn("unix_nsecs", NetFlowV5.FIELD_UNIX_NSECS, false, None),
    MappedColumn("sysuptime", NetFlowV5.FIELD_SYSUPTIME, false, None),
    MappedColumn("exaddr", NetFlowV5.FIELD_EXADDR, false, Some(IPv4ConvertFunction())),
    MappedColumn("srcip", NetFlowV5.FIELD_SRCADDR, true, Some(IPv4ConvertFunction())),
    MappedColumn("dstip", NetFlowV5.FIELD_DSTADDR, true, Some(IPv4ConvertFunction())),
    MappedColumn("nexthop", NetFlowV5.FIELD_NEXTHOP, false, Some(IPv4ConvertFunction())),
    MappedColumn("input", NetFlowV5.FIELD_INPUT, false, None),
    MappedColumn("output", NetFlowV5.FIELD_OUTPUT, false, None),
    MappedColumn("packets", NetFlowV5.FIELD_DPKTS, false, None),
    MappedColumn("octets", NetFlowV5.FIELD_DOCTETS, false, None),
    MappedColumn("first_flow", NetFlowV5.FIELD_FIRST, false, None),
    MappedColumn("last_flow", NetFlowV5.FIELD_LAST, false, None),
    MappedColumn("srcport", NetFlowV5.FIELD_SRCPORT, true, None),
    MappedColumn("dstport", NetFlowV5.FIELD_DSTPORT, true, None),
    MappedColumn("protocol", NetFlowV5.FIELD_PROT, true, Some(ProtocolConvertFunction())),
    MappedColumn("tos", NetFlowV5.FIELD_TOS, false, None),
    MappedColumn("tcp_flags", NetFlowV5.FIELD_TCP_FLAGS, false, None),
    MappedColumn("engine_type", NetFlowV5.FIELD_ENGINE_TYPE, false, None),
    MappedColumn("engine_id", NetFlowV5.FIELD_ENGINE_ID, false, None),
    MappedColumn("src_mask", NetFlowV5.FIELD_SRC_MASK, false, None),
    MappedColumn("dst_mask", NetFlowV5.FIELD_DST_MASK, false, None),
    MappedColumn("src_as", NetFlowV5.FIELD_SRC_AS, false, None),
    MappedColumn("dst_as", NetFlowV5.FIELD_DST_AS, false, None)
  )

  override def version(): Short = 5
}
