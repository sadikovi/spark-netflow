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

import org.apache.spark.sql.types._

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
    MappedColumn("unix_secs", NetFlowV7.FIELD_UNIX_SECS, LongType, None),
    MappedColumn("unix_nsecs", NetFlowV7.FIELD_UNIX_NSECS, LongType, None),
    MappedColumn("sysuptime", NetFlowV7.FIELD_SYSUPTIME, LongType, None),
    MappedColumn("exaddr", NetFlowV7.FIELD_EXADDR, LongType, Some(IPv4ConvertFunction())),
    MappedColumn("srcip", NetFlowV7.FIELD_SRCADDR, LongType, Some(IPv4ConvertFunction())),
    MappedColumn("dstip", NetFlowV7.FIELD_DSTADDR, LongType, Some(IPv4ConvertFunction())),
    MappedColumn("nexthop", NetFlowV7.FIELD_NEXTHOP, LongType, Some(IPv4ConvertFunction())),
    MappedColumn("input", NetFlowV7.FIELD_INPUT, IntegerType, None),
    MappedColumn("output", NetFlowV7.FIELD_OUTPUT, IntegerType, None),
    MappedColumn("packets", NetFlowV7.FIELD_DPKTS, LongType, None),
    MappedColumn("octets", NetFlowV7.FIELD_DOCTETS, LongType, None),
    MappedColumn("first_flow", NetFlowV7.FIELD_FIRST, LongType, None),
    MappedColumn("last_flow", NetFlowV7.FIELD_LAST, LongType, None),
    MappedColumn("srcport", NetFlowV7.FIELD_SRCPORT, IntegerType, None),
    MappedColumn("dstport", NetFlowV7.FIELD_DSTPORT, IntegerType, None),
    MappedColumn("protocol", NetFlowV7.FIELD_PROT, ShortType, Some(ProtocolConvertFunction())),
    MappedColumn("tos", NetFlowV7.FIELD_TOS, ShortType, None),
    MappedColumn("tcp_flags", NetFlowV7.FIELD_TCP_FLAGS, ShortType, None),
    MappedColumn("flags", NetFlowV7.FIELD_FLAGS, ShortType, None),
    MappedColumn("engine_type", NetFlowV7.FIELD_ENGINE_TYPE, ShortType, None),
    MappedColumn("engine_id", NetFlowV7.FIELD_ENGINE_ID, ShortType, None),
    MappedColumn("src_mask", NetFlowV7.FIELD_SRC_MASK, ShortType, None),
    MappedColumn("dst_mask", NetFlowV7.FIELD_DST_MASK, ShortType, None),
    MappedColumn("src_as", NetFlowV7.FIELD_SRC_AS, IntegerType, None),
    MappedColumn("dst_as", NetFlowV7.FIELD_DST_AS, IntegerType, None),
    MappedColumn("router_sc", NetFlowV7.FIELD_ROUTER_SC, LongType, Some(IPv4ConvertFunction()))
  )

  override def version(): Short = 7
}
