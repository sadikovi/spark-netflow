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

package com.github.sadikovi.spark.netflow.version7

import com.github.sadikovi.netflowlib.version.NetFlowV7
import com.github.sadikovi.spark.netflow.sources._

class DefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = {
    new InterfaceV7()
  }
}

/** NetFlow interface for version 7. */
private class InterfaceV7 extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq(
    MappedColumn("unix_secs", NetFlowV7.FIELD_UNIX_SECS, false, None),
    MappedColumn("unix_nsecs", NetFlowV7.FIELD_UNIX_NSECS, false, None),
    MappedColumn("sysuptime", NetFlowV7.FIELD_SYSUPTIME, false, None),
    MappedColumn("exaddr", NetFlowV7.FIELD_EXADDR, false, Some(IPConvertFunction())),
    MappedColumn("srcip", NetFlowV7.FIELD_SRCADDR, false, Some(IPConvertFunction())),
    MappedColumn("dstip", NetFlowV7.FIELD_DSTADDR, false, Some(IPConvertFunction())),
    MappedColumn("nexthop", NetFlowV7.FIELD_NEXTHOP, false, Some(IPConvertFunction())),
    MappedColumn("input", NetFlowV7.FIELD_INPUT, false, None),
    MappedColumn("output", NetFlowV7.FIELD_OUTPUT, false, None),
    MappedColumn("packets", NetFlowV7.FIELD_DPKTS, false, None),
    MappedColumn("octets", NetFlowV7.FIELD_DOCTETS, false, None),
    MappedColumn("first_flow", NetFlowV7.FIELD_FIRST, false, None),
    MappedColumn("last_flow", NetFlowV7.FIELD_LAST, false, None),
    MappedColumn("srcport", NetFlowV7.FIELD_SRCPORT, false, None),
    MappedColumn("dstport", NetFlowV7.FIELD_DSTPORT, false, None),
    MappedColumn("protocol", NetFlowV7.FIELD_PROT, false, Some(ProtocolConvertFunction())),
    MappedColumn("tos", NetFlowV7.FIELD_TOS, false, None),
    MappedColumn("tcp_flags", NetFlowV7.FIELD_TCP_FLAGS, false, None),
    MappedColumn("flags", NetFlowV7.FIELD_FLAGS, false, None),
    MappedColumn("engine_type", NetFlowV7.FIELD_ENGINE_TYPE, false, None),
    MappedColumn("engine_id", NetFlowV7.FIELD_ENGINE_ID, false, None),
    MappedColumn("src_mask", NetFlowV7.FIELD_SRC_MASK, false, None),
    MappedColumn("dst_mask", NetFlowV7.FIELD_DST_MASK, false, None),
    MappedColumn("src_as", NetFlowV7.FIELD_SRC_AS, false, None),
    MappedColumn("dst_as", NetFlowV7.FIELD_DST_AS, false, None),
    MappedColumn("router_sc", NetFlowV7.FIELD_ROUTER_SC, false, Some(IPConvertFunction()))
  )

  override def version(): Short = 7
}
