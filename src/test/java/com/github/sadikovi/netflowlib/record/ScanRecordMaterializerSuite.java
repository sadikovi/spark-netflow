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

package com.github.sadikovi.netflowlib.record;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;

import com.github.sadikovi.netflowlib.codegen.CodeGenContext;
import com.github.sadikovi.netflowlib.codegen.CodeGenNode;
import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Columns.ByteColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;

public class ScanRecordMaterializerSuite {
  // Test buffer node
  private CodeGenNode buffer = new CodeGenNode() {
    @Override
    public String nodeName(CodeGenContext ctx) {
      return "byteBuffer";
    }

    @Override
    public String nodeType(CodeGenContext ctx) {
      return "io.netty.buffer.ByteBuf";
    }

    @Override
    public Class<?> nodeClass(CodeGenContext ctx) {
      return ByteBuf.class;
    }
  };

  // Generate processing of record for empty columns
  @Test
  public void testGenerateProcessRecord1() {
    Column[] columns = new Column[0];
    ScanRecordMaterializer rm = new ScanRecordMaterializer(columns);
    CodeGenContext.reset();
    CodeGenContext ctx = CodeGenContext.getOrCreate();
    String code = rm.generateProcessRecord(ctx, buffer);
    assertEquals(code,
      "java.lang.Object[] newRecord0 = new java.lang.Object[0];\n" +
      "return newRecord0;\n");
  }

  // Generate processing of record for one column
  @Test
  public void testGenerateProcessRecord2() {
    Column[] columns = new Column[]{new IntColumn("col1", 0)};
    ScanRecordMaterializer rm = new ScanRecordMaterializer(columns);
    CodeGenContext.reset();
    CodeGenContext ctx = CodeGenContext.getOrCreate();
    String code = rm.generateProcessRecord(ctx, buffer);
    assertEquals(code,
      "java.lang.Object[] newRecord0 = new java.lang.Object[1];\n" +
      "newRecord0[0] = byteBuffer.getUnsignedShort(0);\n" +
      "return newRecord0;\n");
  }

  // Generate processing of record for 4 columns
  @Test
  public void testGenerateProcessRecord3() {
    Column[] columns = new Column[]{new ByteColumn("col0", 0), new IntColumn("col1", 1),
      new LongColumn("col2", 5), new ShortColumn("col3", 13)};
    ScanRecordMaterializer rm = new ScanRecordMaterializer(columns);
    CodeGenContext.reset();
    CodeGenContext ctx = CodeGenContext.getOrCreate();
    String code = rm.generateProcessRecord(ctx, buffer);
    assertEquals(code,
      "java.lang.Object[] newRecord0 = new java.lang.Object[4];\n" +
      "newRecord0[0] = byteBuffer.getByte(0);\n" +
      "newRecord0[1] = byteBuffer.getUnsignedShort(1);\n" +
      "newRecord0[2] = byteBuffer.getUnsignedInt(5);\n" +
      "newRecord0[3] = byteBuffer.getUnsignedByte(13);\n" +
      "return newRecord0;\n");
  }

  // Check that new record was registered as node
  @Test
  public void testGenerateProcessRecord4() {
    Column[] columns = new Column[]{new IntColumn("col1", 0)};
    ScanRecordMaterializer rm = new ScanRecordMaterializer(columns);
    CodeGenContext.reset();
    CodeGenContext ctx = CodeGenContext.getOrCreate();
    rm.generateProcessRecord(ctx, buffer);
    assertTrue(ctx.getNode("newRecord") != null);
  }
}
