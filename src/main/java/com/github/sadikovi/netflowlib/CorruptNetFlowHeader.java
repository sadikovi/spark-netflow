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

/**
 * Class to indicate that header is corrupt. Overwrites parent method `isValid()` to hint on
 * incorrectness of header.
 */
public class CorruptNetFlowHeader extends NetFlowHeader {
  public CorruptNetFlowHeader() { }

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

  @Override
  public boolean isValid() {
    return false;
  }
}
