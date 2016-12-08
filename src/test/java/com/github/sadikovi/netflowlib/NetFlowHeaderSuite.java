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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;

import com.github.sadikovi.netflowlib.CorruptNetFlowHeader;
import com.github.sadikovi.netflowlib.NetFlowHeader;

public class NetFlowHeaderSuite {
  @Test
  public void testInitNetFlowHeader() {
    NetFlowHeader header = new NetFlowHeader((short) 5, ByteOrder.BIG_ENDIAN);
    assertEquals(header.getStreamVersion(), 5);
    assertEquals(header.getByteOrder(), ByteOrder.BIG_ENDIAN);
    assertEquals(header.isValid(), true);
  }

  @Test
  public void testInitCorruptNetFlowHeader() {
    NetFlowHeader header = new CorruptNetFlowHeader();
    assertEquals(header.isValid(), false);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testFailOnCorruptHeaderMethod1() {
    NetFlowHeader header = new CorruptNetFlowHeader();
    header.getStreamVersion();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testFailOnCorruptHeaderMethod2() {
    NetFlowHeader header = new CorruptNetFlowHeader();
    header.getByteOrder();
  }
}
