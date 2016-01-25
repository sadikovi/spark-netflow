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

/** Interface alias as one of the TLV */
public class InterfaceAlias {
  public InterfaceAlias(long ip, int ifIndexCount, int ifIndex, String aliasName) {
    this.ip = ip;
    this.ifIndexCount = ifIndexCount;
    this.ifIndex = ifIndex;
    this.aliasName = aliasName;
  }

  /** Get IP of device */
  public long getIP() {
    return this.ip;
  }

  /** Get ifIndex count */
  public int getIfIndexCount() {
    return this.ifIndexCount;
  }

  /** Get ifIndex of interface */
  public int getIfIndex() {
    return this.ifIndex;
  }

  /** Get alias name */
  public String getAliasName() {
    return this.aliasName;
  }

  private long ip = 0;
  private int ifIndexCount = 0;
  private int ifIndex = 0;
  private String aliasName = null;
}
