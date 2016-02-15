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

import org.apache.spark.sql.types.{IntegerType, LongType, ShortType}

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
    MappedColumn("unix_secs", NetFlowV5.V5_FIELD_UNIX_SECS, LongType, false, None),
    MappedColumn("unix_nsecs", NetFlowV5.V5_FIELD_UNIX_NSECS, LongType, false, None),
    MappedColumn("sysuptime", NetFlowV5.V5_FIELD_SYSUPTIME, LongType, false, None),
    MappedColumn("exaddr", NetFlowV5.V5_FIELD_EXADDR, LongType, false, Some(IPConvertFunction())),
    MappedColumn("srcip", NetFlowV5.V5_FIELD_SRCADDR, LongType, false, Some(IPConvertFunction())),
    MappedColumn("dstip", NetFlowV5.V5_FIELD_DSTADDR, LongType, false, Some(IPConvertFunction())),
    MappedColumn("nexthop", NetFlowV5.V5_FIELD_NEXTHOP, LongType, false, Some(IPConvertFunction())),
    MappedColumn("input", NetFlowV5.V5_FIELD_INPUT, IntegerType, false, None),
    MappedColumn("output", NetFlowV5.V5_FIELD_OUTPUT, IntegerType, false, None),
    MappedColumn("packets", NetFlowV5.V5_FIELD_DPKTS, LongType, false, None),
    MappedColumn("octets", NetFlowV5.V5_FIELD_DOCTETS, LongType, false, None),
    MappedColumn("first_flow", NetFlowV5.V5_FIELD_FIRST, LongType, false, None),
    MappedColumn("last_flow", NetFlowV5.V5_FIELD_LAST, LongType, false, None),
    MappedColumn("srcport", NetFlowV5.V5_FIELD_SRCPORT, IntegerType, false, None),
    MappedColumn("dstport", NetFlowV5.V5_FIELD_DSTPORT, IntegerType, false, None),
    MappedColumn("protocol", NetFlowV5.V5_FIELD_PROT, ShortType, false,
      Some(ProtocolConvertFunction())),
    MappedColumn("tos", NetFlowV5.V5_FIELD_TOS, ShortType, false, None),
    MappedColumn("tcp_flags", NetFlowV5.V5_FIELD_TCP_FLAGS, ShortType, false, None),
    MappedColumn("engine_type", NetFlowV5.V5_FIELD_ENGINE_TYPE, ShortType, false, None),
    MappedColumn("engine_id", NetFlowV5.V5_FIELD_ENGINE_ID, ShortType, false, None),
    MappedColumn("src_mask", NetFlowV5.V5_FIELD_SRC_MASK, ShortType, false, None),
    MappedColumn("dst_mask", NetFlowV5.V5_FIELD_DST_MASK, ShortType, false, None),
    MappedColumn("src_as", NetFlowV5.V5_FIELD_SRC_AS, IntegerType, false, None),
    MappedColumn("dst_as", NetFlowV5.V5_FIELD_DST_AS, IntegerType, false, None)
  )

  override def version(): Short = 5
}
