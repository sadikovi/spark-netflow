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

package com.github.sadikovi.netflowlib.version;

import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;

public class NetFlowV5 extends NetFlow {
  // list of supported columns and size in bytes
  // Current seconds since 0000 UTC 1970, size: 4
  public static final LongColumn FIELD_UNIX_SECS = new LongColumn("unix_secs", 0);
  // Residual nanoseconds since 0000 UTC 1970, size: 4
  public static final LongColumn FIELD_UNIX_NSECS = new LongColumn("unix_nsecs", 4);
  // Current time in millisecs since router booted, size: 4
  public static final LongColumn FIELD_SYSUPTIME = new LongColumn("sys_uptime", 8);
  // Exporter IP address, size: 4
  public static final LongColumn FIELD_EXADDR = new LongColumn("export_ip", 12);
  // Source IP Address, size: 4
  public static final LongColumn FIELD_SRCADDR = new LongColumn("srcip", 16);
  // Destination IP Address, size: 4
  public static final LongColumn FIELD_DSTADDR = new LongColumn("dstip", 20);
  // Next hop router's IP Address, size: 4
  public static final LongColumn FIELD_NEXTHOP = new LongColumn("nexthop", 24);
  // Input interface index, size: 2
  public static final IntColumn FIELD_INPUT = new IntColumn("input", 28);
  // Output interface index, size: 2
  public static final IntColumn FIELD_OUTPUT = new IntColumn("output", 30);
  // Packets sent in Duration, size: 4
  public static final LongColumn FIELD_DPKTS = new LongColumn("packets", 32);
  // Octets sent in Duration, size: 4
  public static final LongColumn FIELD_DOCTETS = new LongColumn("octets", 36);
  // SysUptime at start of flow, size: 4
  public static final LongColumn FIELD_FIRST = new LongColumn("first", 40);
  // and of last packet of flow, size: 4
  public static final LongColumn FIELD_LAST = new LongColumn("last", 44);
  // TCP/UDP source port number or equivalent, size: 2
  public static final IntColumn FIELD_SRCPORT = new IntColumn("srcport", 48);
  // TCP/UDP destination port number or equiv, size: 2
  public static final IntColumn FIELD_DSTPORT = new IntColumn("dstport", 50);
  // IP protocol, e.g., 6=TCP, 17=UDP, ..., size: 1
  public static final ShortColumn FIELD_PROT = new ShortColumn("protocol", 52);
  // IP Type-of-Service, size: 1
  public static final ShortColumn FIELD_TOS = new ShortColumn("tos", 53);
  // OR of TCP header bits, size: 1
  public static final ShortColumn FIELD_TCP_FLAGS = new ShortColumn("tcp_flags", 54);
  // Type of flow switching engine (RP, VIP, etc.), size: 1
  // There is field "pad" which is unused byte in record, we skip it
  public static final ShortColumn FIELD_ENGINE_TYPE = new ShortColumn("engine_type", 56);
  // Slot number of the flow switching engine, size: 1
  public static final ShortColumn FIELD_ENGINE_ID = new ShortColumn("engine_id", 57);
  // mask length of source address, size: 1
  public static final ShortColumn FIELD_SRC_MASK = new ShortColumn("src_mask", 58);
  // mask length of destination address, size: 1
  public static final ShortColumn FIELD_DST_MASK = new ShortColumn("dst_mask", 59);
  // AS of source address, size: 2
  public static final IntColumn FIELD_SRC_AS = new IntColumn("src_as", 60);
  // AS of destination address, size: 2
  public static final IntColumn FIELD_DST_AS = new IntColumn("dst_as", 62);

  public NetFlowV5() { }

  @Override
  public int recordSize() {
    return 64;
  }
}
