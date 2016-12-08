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

/** Class to indicate that header is corrupt */
public class CorruptNetFlowHeader extends NetFlowHeader {
  public CorruptNetFlowHeader() {
    this.isValid = false;
  }

  ////////////////////////////////////////////////////////////
  // Setters API
  ////////////////////////////////////////////////////////////
  @Override
  public void setFlowVersion(short version) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setStartCapture(long value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setEndCapture(long value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setHeaderFlags(long flags) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setRotation(long value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setNumFlows(long value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setNumDropped(long value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setNumMisordered(long value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setHostname(String hostname) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setComments(String comments) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setVendor(int value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setAggVersion(short value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setAggMethod(short value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setExporterIP(long value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setNumCorrupt(long value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setSeqReset(long value) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setInterfaceName(long ip, int ifIndex, String name) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public void setInterfaceAlias(long ip, int ifIndexCount, int ifIndex, String alias) {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  ////////////////////////////////////////////////////////////
  // Getters API
  ////////////////////////////////////////////////////////////
  @Override
  public short getStreamVersion() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public ByteOrder getByteOrder() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public int getHeaderSize() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public short getFlowVersion() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public long getStartCapture() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public long getEndCapture() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public long getHeaderFlags() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public boolean isCompressed() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public String getHostname() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public String getComments() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public short getAggMethod() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public short getAggVersion() {
    throw new UnsupportedOperationException("Header is corrupt");
  }

  @Override
  public long getFields() {
    throw new UnsupportedOperationException("Header is corrupt");
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
