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

package com.github.sadikovi.netflowlib;

import java.nio.ByteOrder;

import com.github.sadikovi.netflowlib.fields.InterfaceAlias;
import com.github.sadikovi.netflowlib.fields.InterfaceName;

/** Header for stream version of NetFlow file */
public class NetFlowHeader {
  // number of bytes for stream version 1 header
  public static final short S1_HEADER_SIZE = 358;
  // length of hostname field for stream version 1
  public static final short S1_HEADER_HN_LEN = 68;
  // length of comments field for stream version 1
  public static final short S1_HEADER_CMNT_LEN = 256;

  // index of fields to set, used to map bit vector
  public static final long FIELD_VENDOR = 0x00000001L;
  public static final long FIELD_FLOW_VER = 0x00000002L;
  public static final long FIELD_AGG_VER = 0x00000004L;
  public static final long FIELD_AGG_METHOD = 0x00000008L;
  public static final long FIELD_EXPORTER_IP = 0x00000010L;
  public static final long FIELD_CAP_START = 0x00000020L;
  public static final long FIELD_CAP_END = 0x00000040L;
  public static final long FIELD_HEADER_FLAGS = 0x00000080L;
  public static final long FIELD_ROT_SCHEDULE = 0x00000100L;
  public static final long FIELD_FLOW_COUNT = 0x00000200L;
  public static final long FIELD_FLOW_LOST = 0x00000400L;
  public static final long FIELD_FLOW_MISORDERED = 0x00000800L;
  public static final long FIELD_PKT_CORRUPT = 0x00001000L;
  public static final long FIELD_SEQ_RESET = 0x00002000L;
  public static final long FIELD_CAP_HOSTNAME = 0x00004000L;
  public static final long FIELD_COMMENTS = 0x00008000L;
  public static final long FIELD_IF_NAME = 0x00010000L;
  public static final long FIELD_IF_ALIAS = 0x00020000L;
  public static final long FIELD_INTERRUPT = 0x00040000L;

  // header flag positions
  // complete, safe to read
  public static final long HEADER_FLAG_DONE = 0x1;
  // compression enabled
  public static final long HEADER_FLAG_COMPRESS = 0x2;
  // multiple PDU's [NOT USED]
  public static final long HEADER_FLAG_MULT_PDU = 0x4;
  // stream, e.g. flow-cat
  public static final long HEADER_FLAG_STREAMING = 0x8;
  // stream translated from old fmt
  public static final long HEADER_FLAG_XLATE = 0x10;
  // streaming & preloaded header
  public static final long HEADER_FLAG_PRELOADED = 0x20;

  public NetFlowHeader(short streamVersion, ByteOrder byteOrder) {
    this.sversion = streamVersion;
    this.order = byteOrder;
  }

  public NetFlowHeader(short streamVersion, ByteOrder byteOrder, int headerSize) {
    this(streamVersion, byteOrder);
    this.headerSize = headerSize;
  }

  /** For subclasses to overwrite */
  protected NetFlowHeader() { }

  ////////////////////////////////////////////////////////////
  // Setters API
  ////////////////////////////////////////////////////////////
  /** Set NetFlow flow version, either V1, V3, V5, V7, etc */
  public void setFlowVersion(short version) {
    this.FIELDS |= this.FIELD_FLOW_VER;
    this.dversion = version;
  }

  /** Set start time of capture in milliseconds */
  public void setStartCapture(long value) {
    this.FIELDS |= this.FIELD_CAP_START;
    this.start = value;
  }

  /** Set end time of capture in milliseconds */
  public void setEndCapture(long value) {
    this.FIELDS |= this.FIELD_CAP_END;
    this.end = value;
  }

  /** Set header flags as long (bit vector). We will use patterns to resolve flags as we go */
  public void setHeaderFlags(long flags) {
    this.FIELDS |= this.FIELD_HEADER_FLAGS;
    this.flags = flags;
  }

  /** Set rotation schedule */
  public void setRotation(long value) {
    this.FIELDS |= this.FIELD_ROT_SCHEDULE;
    this.rotation = value;
  }

  /** Set total number of flows in this stream */
  public void setNumFlows(long value) {
    this.FIELDS |= this.FIELD_FLOW_COUNT;
    this.numFlows = value;
  }

  /** Set number of dropped packets / flows for this stream */
  public void setNumDropped(long value) {
    this.FIELDS |= this.FIELD_FLOW_LOST;
    this.numDropped = value;
  }

  /** Set number of misordered packets / flows for this stream */
  public void setNumMisordered(long value) {
    this.FIELDS |= this.FIELD_FLOW_MISORDERED;
    this.numMisordered = value;
  }

  /** Set name of capture device */
  public void setHostname(String hostname) {
    this.FIELDS |= this.FIELD_CAP_HOSTNAME;
    this.hostname = hostname;
  }

  /** Set comments */
  public void setComments(String comments) {
    this.FIELDS |= this.FIELD_COMMENTS;
    this.comments = comments;
  }

  /** Set vendor */
  public void setVendor(int value) {
    this.FIELDS |= this.FIELD_VENDOR;
    this.vendor = value;
  }

  /** Set aggregation version */
  public void setAggVersion(short value) {
    this.FIELDS |= this.FIELD_AGG_VER;
    this.aggregationVersion = value;
  }

  /** Set aggregation method */
  public void setAggMethod(short value) {
    this.FIELDS |= this.FIELD_AGG_METHOD;
    this.aggregationMethod = value;
  }

  /** Set exporter IP */
  public void setExporterIP(long value) {
    this.FIELDS |= this.FIELD_EXPORTER_IP;
    this.exporterIP = value;
  }

  /** Set number of corrupt packets */
  public void setNumCorrupt(long value) {
    this.FIELDS |= this.FIELD_PKT_CORRUPT;
    this.numCorrupt = value;
  }

  /** Set sequence reset */
  public void setSeqReset(long value) {
    this.FIELDS |= this.FIELD_SEQ_RESET;
    this.seqReset = value;
  }

  /** Set interface name */
  public void setInterfaceName(long ip, int ifIndex, String name) {
    this.FIELDS |= this.FIELD_IF_NAME;
    this.interfaceName = new InterfaceName(ip, ifIndex, name);
  }

  /** Set interface alias */
  public void setInterfaceAlias(long ip, int ifIndexCount, int ifIndex, String alias) {
    this.FIELDS |= this.FIELD_IF_ALIAS;
    this.interfaceAlias = new InterfaceAlias(ip, ifIndexCount, ifIndex, alias);
  }

  ////////////////////////////////////////////////////////////
  // Getters API
  ////////////////////////////////////////////////////////////
  /** Get stream version */
  public short getStreamVersion() {
    return this.sversion;
  }

  public ByteOrder getByteOrder() {
    return this.order;
  }

  /** Get actual header size */
  public int getHeaderSize() {
    // header size is fixed for stream version 1
    if (this.sversion == 1) {
      return this.S1_HEADER_SIZE;
    } else {
      return this.headerSize;
    }
  }

  /** Get Flow (export) version */
  public short getFlowVersion() {
    return this.dversion;
  }

  /** Get start time of capture */
  public long getStartCapture() {
    return this.start;
  }

  /** Get end time of capture */
  public long getEndCapture() {
    return this.end;
  }

  /** Get header flags */
  public long getHeaderFlags() {
    return this.flags;
  }

  /** Commonly used, conversion flag */
  public boolean isCompressed() {
    return (getHeaderFlags() & HEADER_FLAG_COMPRESS) > 0;
  }

  /** Get hostname */
  public String getHostname() {
    return this.hostname;
  }

  /** Get header comments */
  public String getComments() {
    return this.comments;
  }

  /** Get aggregation method */
  public short getAggMethod() {
    return this.aggregationMethod;
  }

  /** Get aggregation version */
  public short getAggVersion() {
    return this.aggregationVersion;
  }

  public long getFields() {
    return this.FIELDS;
  }

  /**
   * By default standard NetFlow header is always valid. Subclasses should overwrite this method,
   * if that is not the case, or method should be conditional.
   */
  public boolean isValid() {
    return true;
  }

  // bit vector of fields
  private long FIELDS = 0;
  // flow stream format version either 1 or 3
  private short sversion = 0;
  // byte order, either big endian or little endian
  private ByteOrder order = null;
  // actual header size (will be different in case of stream version 3)
  private int headerSize = 0;
  // version of NetFlow
  private short dversion = 0;
  // start time of flow capture
  private long start = 0;
  // end time of flow capture
  private long end = 0;
  // header flags as bit vector
  private long flags = 0;
  // rotation schedule
  private long rotation = 0;
  // number of flows
  private long numFlows = 0;
  // number of dropped packets (stream version 1) / flows (stream version 3)
  private long numDropped = 0;
  // number of misordered packets (stream version 1) / flows (stream version 3)
  private long numMisordered = 0;
  // name of capture device
  private String hostname = null;
  // ascii comments
  private String comments = null;
  // vendor (cisco - 0x1)
  private int vendor = 0;
  // aggregation version
  private short aggregationVersion = 0;
  // aggregation method
  private short aggregationMethod = 0;
  // exporter IP
  private long exporterIP = 0;
  // number of corrupt packets (stream version 1) / flows (stream version 3)
  private long numCorrupt = 0;
  // times sequence # was so far off lost/misordered state could not be determined
  private long seqReset = 0;
  // interface name
  private InterfaceName interfaceName = null;
  // interface alias
  private InterfaceAlias interfaceAlias = null;
}
