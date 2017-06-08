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

import org.apache.spark.sql.types._

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
    MappedColumn("unix_secs", NetFlowV5.FIELD_UNIX_SECS, LongType, None),
    MappedColumn("unix_nsecs", NetFlowV5.FIELD_UNIX_NSECS, LongType, None),
    MappedColumn("sysuptime", NetFlowV5.FIELD_SYSUPTIME, LongType, None),
    MappedColumn("exaddr", NetFlowV5.FIELD_EXADDR, LongType, Some(IPv4ConvertFunction())),
    MappedColumn("srcip", NetFlowV5.FIELD_SRCADDR, LongType, Some(IPv4ConvertFunction())),
    MappedColumn("dstip", NetFlowV5.FIELD_DSTADDR, LongType, Some(IPv4ConvertFunction())),
    MappedColumn("nexthop", NetFlowV5.FIELD_NEXTHOP, LongType, Some(IPv4ConvertFunction())),
    MappedColumn("input", NetFlowV5.FIELD_INPUT, IntegerType, None),
    MappedColumn("output", NetFlowV5.FIELD_OUTPUT, IntegerType, None),
    MappedColumn("packets", NetFlowV5.FIELD_DPKTS, LongType, None),
    MappedColumn("octets", NetFlowV5.FIELD_DOCTETS, LongType, None),
    MappedColumn("first_flow", NetFlowV5.FIELD_FIRST, LongType, None),
    MappedColumn("last_flow", NetFlowV5.FIELD_LAST, LongType, None),
    MappedColumn("srcport", NetFlowV5.FIELD_SRCPORT, IntegerType, None),
    MappedColumn("dstport", NetFlowV5.FIELD_DSTPORT, IntegerType, None),
    MappedColumn("protocol", NetFlowV5.FIELD_PROT, ShortType, Some(ProtocolConvertFunction())),
    MappedColumn("tos", NetFlowV5.FIELD_TOS, ShortType, None),
    MappedColumn("tcp_flags", NetFlowV5.FIELD_TCP_FLAGS, ShortType, None),
    MappedColumn("engine_type", NetFlowV5.FIELD_ENGINE_TYPE, ShortType, None),
    MappedColumn("engine_id", NetFlowV5.FIELD_ENGINE_ID, ShortType, None),
    MappedColumn("src_mask", NetFlowV5.FIELD_SRC_MASK, ShortType, None),
    MappedColumn("dst_mask", NetFlowV5.FIELD_DST_MASK, ShortType, None),
    MappedColumn("src_as", NetFlowV5.FIELD_SRC_AS, IntegerType, None),
    MappedColumn("dst_as", NetFlowV5.FIELD_DST_AS, IntegerType, None)
  )

  override def version(): Short = 5
}
