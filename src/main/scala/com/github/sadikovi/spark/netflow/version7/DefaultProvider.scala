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

import org.apache.spark.sql.types.{IntegerType, LongType, ShortType}

import com.github.sadikovi.netflowlib.version.NetFlowV7
import com.github.sadikovi.spark.netflow.sources.{MappedColumn, NetFlowProvider, ResolvedInterface, IPConvertFunction}

class DefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = {
    new InterfaceV7()
  }
}

/** NetFlow interface for version 7. */
private class InterfaceV7 extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq(
    MappedColumn("unix_secs", NetFlowV7.V7_FIELD_UNIX_SECS, LongType, false, None),
    MappedColumn("unix_nsecs", NetFlowV7.V7_FIELD_UNIX_NSECS, LongType, false, None),
    MappedColumn("sysuptime", NetFlowV7.V7_FIELD_SYSUPTIME, LongType, false, None),
    MappedColumn("exaddr", NetFlowV7.V7_FIELD_EXADDR, LongType, false, Some(IPConvertFunction())),
    MappedColumn("srcip", NetFlowV7.V7_FIELD_SRCADDR, LongType, false, Some(IPConvertFunction())),
    MappedColumn("dstip", NetFlowV7.V7_FIELD_DSTADDR, LongType, false, Some(IPConvertFunction())),
    MappedColumn("nexthop", NetFlowV7.V7_FIELD_NEXTHOP, LongType, false, Some(IPConvertFunction())),
    MappedColumn("input", NetFlowV7.V7_FIELD_INPUT, IntegerType, false, None),
    MappedColumn("output", NetFlowV7.V7_FIELD_OUTPUT, IntegerType, false, None),
    MappedColumn("packets", NetFlowV7.V7_FIELD_DPKTS, LongType, false, None),
    MappedColumn("octets", NetFlowV7.V7_FIELD_DOCTETS, LongType, false, None),
    MappedColumn("first_flow", NetFlowV7.V7_FIELD_FIRST, LongType, false, None),
    MappedColumn("last_flow", NetFlowV7.V7_FIELD_LAST, LongType, false, None),
    MappedColumn("srcport", NetFlowV7.V7_FIELD_SRCPORT, IntegerType, false, None),
    MappedColumn("dstport", NetFlowV7.V7_FIELD_DSTPORT, IntegerType, false, None),
    MappedColumn("protocol", NetFlowV7.V7_FIELD_PROT, ShortType, false, None),
    MappedColumn("tos", NetFlowV7.V7_FIELD_TOS, ShortType, false, None),
    MappedColumn("tcp_flags", NetFlowV7.V7_FIELD_TCP_FLAGS, ShortType, false, None),
    MappedColumn("flags", NetFlowV7.V7_FIELD_FLAGS, ShortType, false, None),
    MappedColumn("engine_type", NetFlowV7.V7_FIELD_ENGINE_TYPE, ShortType, false, None),
    MappedColumn("engine_id", NetFlowV7.V7_FIELD_ENGINE_ID, ShortType, false, None),
    MappedColumn("src_mask", NetFlowV7.V7_FIELD_SRC_MASK, ShortType, false, None),
    MappedColumn("dst_mask", NetFlowV7.V7_FIELD_DST_MASK, ShortType, false, None),
    MappedColumn("src_as", NetFlowV7.V7_FIELD_SRC_AS, IntegerType, false, None),
    MappedColumn("dst_as", NetFlowV7.V7_FIELD_DST_AS, IntegerType, false, None),
    MappedColumn("router_sc", NetFlowV7.V7_FIELD_ROUTER_SC, LongType, false,
      Some(IPConvertFunction()))
  )

  override def version(): Short = 7
}
