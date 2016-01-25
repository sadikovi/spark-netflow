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

import java.util.HashMap;
import io.netty.buffer.ByteBuf;
import com.github.sadikovi.netflowlib.SFlow;

public class NetflowV5 extends SFlow {
  // list of supported columns and size in bytes
  // Current seconds since 0000 UTC 1970, size: 4
  public static final long V5_FIELD_UNIX_SECS = 0x00000001L;
  // Residual nanoseconds since 0000 UTC 1970, size: 4
  public static final long V5_FIELD_UNIX_NSECS = 0x00000002L;
  // Current time in millisecs since router booted, size: 4
  public static final long V5_FIELD_SYSUPTIME = 0x00000004L;
  // Exporter IP address, size: 4
  public static final long V5_FIELD_EXADDR = 0x00000008L;
  // Source IP Address, size: 4
  public static final long V5_FIELD_SRCADDR = 0x00000010L;
  // Destination IP Address, size: 4
  public static final long V5_FIELD_DSTADDR = 0x00000020L;
  // Next hop router's IP Address, size: 4
  public static final long V5_FIELD_NEXTHOP = 0x00000040L;
  // Input interface index, size: 2
  public static final long V5_FIELD_INPUT = 0x00000080L;
  // Output interface index, size: 2
  public static final long V5_FIELD_OUTPUT = 0x00000100L;
  // Packets sent in Duration, size: 4
  public static final long V5_FIELD_DPKTS = 0x00000200L;
  // Octets sent in Duration, size: 4
  public static final long V5_FIELD_DOCTETS = 0x00000400L;
  // SysUptime at start of flow, size: 4
  public static final long V5_FIELD_FIRST = 0x00000800L;
  // and of last packet of flow, size: 4
  public static final long V5_FIELD_LAST = 0x00001000L;
  // TCP/UDP source port number or equivalent, size: 2
  public static final long V5_FIELD_SRCPORT = 0x00002000L;
  // TCP/UDP destination port number or equiv, size: 2
  public static final long V5_FIELD_DSTPORT = 0x00004000L;
  // IP protocol, e.g., 6=TCP, 17=UDP, ..., size: 1
  public static final long V5_FIELD_PROT = 0x00008000L;
  // IP Type-of-Service, size: 1
  public static final long V5_FIELD_TOS = 0x00010000L;
  // OR of TCP header bits, size: 1
  public static final long V5_FIELD_TCP_FLAGS = 0x00020000L;
  // Type of flow switching engine (RP,VIP,etc.), size: 1
  public static final long V5_FIELD_ENGINE_TYPE = 0x00040000L;
  // Slot number of the flow switching engine, size: 1
  public static final long V5_FIELD_ENGINE_ID = 0x00080000L;
  // mask length of source address, size: 1
  public static final long V5_FIELD_SRC_MASK = 0x00100000L;
  // mask length of destination address, size: 1
  public static final long V5_FIELD_DST_MASK = 0x00200000L;
  // AS of source address, size: 2
  public static final long V5_FIELD_SRC_AS = 0x00400000L;
  // AS of destination address, size: 2
  public static final long V5_FIELD_DST_AS = 0x00800000L;

  public NetflowV5(long[] askedFields) {
    int len = askedFields.length;
    if (len == 0) {
      throw new UnsupportedOperationException("Fields required, found empty array");
    }

    fields = askedFields;
    long fld;
    for (int i=0; i<len; i++) {
      fld = this.fields[i];
      // increment size
      if (fld == V5_FIELD_UNIX_SECS || fld == V5_FIELD_UNIX_NSECS || fld == V5_FIELD_SYSUPTIME ||
          fld == V5_FIELD_EXADDR || fld == V5_FIELD_SRCADDR || fld == V5_FIELD_DSTADDR ||
          fld == V5_FIELD_NEXTHOP || fld == V5_FIELD_DPKTS || fld == V5_FIELD_DOCTETS ||
          fld == V5_FIELD_FIRST || fld == V5_FIELD_LAST) {
        this.actualSize += 4;
      } else if (fld == V5_FIELD_INPUT || fld == V5_FIELD_OUTPUT || fld == V5_FIELD_SRCPORT ||
          fld == V5_FIELD_DSTPORT || fld == V5_FIELD_SRC_AS || fld == V5_FIELD_DST_AS) {
        this.actualSize += 2;
      } else if (fld == V5_FIELD_PROT || fld == V5_FIELD_TOS || fld == V5_FIELD_TCP_FLAGS ||
          fld == V5_FIELD_ENGINE_TYPE || fld == V5_FIELD_ENGINE_ID ||
          fld == V5_FIELD_SRC_MASK || fld == V5_FIELD_DST_MASK) {
        this.actualSize += 1;
      } else {
        throw new UnsupportedOperationException("Field " + fld + " does not exist");
      }
    }
  }

  /** Process record using buffer on the raw full-sized record */
  public Object[] processRecord(ByteBuf buffer) {
    int len = this.fields.length;
    // initialize a new record
    Object[] record = new Object[len];
    // go through each field and fill up buffer
    for (int i=0; i<len; i++) {
      writeField(this.fields[i], buffer, i, record);
    }
    return record;
  }

  // copy bytes from buffer to the record
  private void writeField(long fld, ByteBuf buffer, int pos, Object[] record) {
    if (fld == V5_FIELD_UNIX_SECS) {
      record[pos] = buffer.getUnsignedInt(0);
    } else if (fld == V5_FIELD_UNIX_NSECS) {
      record[pos] = buffer.getUnsignedInt(4);
    } else if (fld == V5_FIELD_SYSUPTIME) {
      record[pos] = buffer.getUnsignedInt(8);
    } else if (fld == V5_FIELD_EXADDR) {
      record[pos] = buffer.getUnsignedInt(12);
    } else if (fld == V5_FIELD_SRCADDR) {
      record[pos] = buffer.getUnsignedInt(16);
    } else if (fld == V5_FIELD_DSTADDR) {
      record[pos] = buffer.getUnsignedInt(20);
    } else if (fld == V5_FIELD_NEXTHOP) {
      record[pos] = buffer.getUnsignedInt(24);
    } else if (fld == V5_FIELD_INPUT) {
      record[pos] = buffer.getUnsignedShort(28);
    } else if (fld == V5_FIELD_OUTPUT) {
      record[pos] = buffer.getUnsignedShort(30);
    } else if (fld == V5_FIELD_DPKTS) {
      record[pos] = buffer.getUnsignedInt(32);
    } else if (fld == V5_FIELD_DOCTETS) {
      record[pos] = buffer.getUnsignedInt(36);
    } else if (fld == V5_FIELD_FIRST) {
      record[pos] = buffer.getUnsignedInt(40);
    } else if (fld == V5_FIELD_LAST) {
      record[pos] = buffer.getUnsignedInt(44);
    } else if (fld == V5_FIELD_SRCPORT) {
      record[pos] = buffer.getUnsignedShort(48);
    } else if (fld == V5_FIELD_DSTPORT) {
      record[pos] = buffer.getUnsignedShort(50);
    } else if (fld == V5_FIELD_PROT) {
      record[pos] = buffer.getUnsignedByte(52);
    } else if (fld == V5_FIELD_TOS) {
      record[pos] = buffer.getUnsignedByte(53);
    } else if (fld == V5_FIELD_TCP_FLAGS) {
      record[pos] = buffer.getUnsignedByte(54);
    } else if (fld == V5_FIELD_ENGINE_TYPE) {
      // there is field "pad" which is unused byte in record, we skip it
      record[pos] = buffer.getUnsignedByte(56);
    } else if (fld == V5_FIELD_ENGINE_ID) {
      record[pos] = buffer.getUnsignedByte(57);
    } else if (fld == V5_FIELD_SRC_MASK) {
      record[pos] = buffer.getUnsignedByte(58);
    } else if (fld == V5_FIELD_DST_MASK) {
      record[pos] = buffer.getUnsignedByte(59);
    } else if (fld == V5_FIELD_SRC_AS) {
      record[pos] = buffer.getUnsignedShort(60);
    } else if (fld == V5_FIELD_DST_AS) {
      record[pos] = buffer.getUnsignedShort(62);
    }
  }

  /** Size in bytes of the Netflow V5 record */
  public short size() {
    return 64;
  }
}
