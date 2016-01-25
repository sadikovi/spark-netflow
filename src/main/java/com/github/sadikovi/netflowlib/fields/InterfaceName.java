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

package com.github.sadikovi.netflowlib.fields;

import java.nio.ByteOrder;

/** Interface name as one of the TLV */
public class InterfaceName {
  public InterfaceName(long ip, int ifIndex, String ifName) {
    this.ip = ip;
    this.ifIndex = ifIndex;
    this.ifName = ifName;
  }

  /** Get interface IP */
  public long getIP() {
    return this.ip;
  }

  /** Get interface IfIndex */
  public int getIfIndex() {
    return this.ifIndex;
  }

  /** Get interface name */
  public String getInterfaceName() {
    return this.ifName;
  }

  private long ip = 0;
  private int ifIndex = 0;
  private String ifName = null;
}
