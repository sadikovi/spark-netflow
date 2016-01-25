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

import io.netty.buffer.ByteBuf;

/** Generic Netflow PDU reading interface */
public abstract class SFlow {
  /**
   * Process buffer of the record and return sequence of fields in order requested. Do not handle
   * byte buffer within the method, it will be taken care of in outer caller. Buffer is a wrapped
   * buffer on constantly updating array of bytes of the size of the record.
   */
  public abstract Object[] processRecord(ByteBuf buffer);

  /** Size in bytes of the Netflow record (header + payload) */
  public abstract short size();

  /** Actual size of the record with only requested fields */
  public short actualSize() {
    return this.actualSize;
  }

  /** Get array of requested fields */
  public long[] getFields() {
    return this.fields;
  }

  // actual size of the record with requested fields only
  protected short actualSize = 0;
  // array of fields
  protected long[] fields;
}
