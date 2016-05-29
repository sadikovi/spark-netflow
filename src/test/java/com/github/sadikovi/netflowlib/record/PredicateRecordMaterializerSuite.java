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

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;

import com.github.sadikovi.netflowlib.codegen.CodeGenContext;
import com.github.sadikovi.netflowlib.codegen.CodeGenNode;
import com.github.sadikovi.netflowlib.predicate.FilterApi;
import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Columns.ByteColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;
import com.github.sadikovi.netflowlib.predicate.Inspectors.Inspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;

public class PredicateRecordMaterializerSuite {
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

  @Test
  public void testGenerateProcessRecord2() {
    Column fcol1 = new IntColumn("fcol1", 0);
    Column col1 = new IntColumn("col1", 8);
    Column col2 = new IntColumn("col2", 12);
    Column[] columns = new Column[]{col1, col2, col2};
    Inspector inspector = null;
    FilterPredicate predicate =
      FilterApi.or(
        FilterApi.eq(fcol1, 7),
        FilterApi.and(
          FilterApi.ge(fcol1, 1),
          FilterApi.le(fcol1, 5)
        )
      );
    HashMap<Column, ArrayList<ValueInspector>> columnInspectors =
      new HashMap<Column, ArrayList<ValueInspector>>();
    columnInspectors.put(fcol1, null);
    PredicateRecordMaterializer rm = new PredicateRecordMaterializer(columns, inspector, predicate,
      columnInspectors);
    CodeGenContext.reset();
    CodeGenContext ctx = CodeGenContext.getOrCreate();
    ctx.registerNode("buffer", buffer);
    String code = rm.generate(ctx);
    assertEquals(code,
      "int fcol10 = byteBuffer.getUnsignedShort(0);\n" +
      "if ((fcol10 == 7) || ((fcol10 >= 1) && (fcol10 <= 5))) {\n" +
        "java.lang.Object[] newRecord0 = new java.lang.Object[3];\n" +
        "newRecord0[0] = byteBuffer.getUnsignedShort(8);\n" +
        "newRecord0[1] = byteBuffer.getUnsignedShort(12);\n" +
        "newRecord0[2] = byteBuffer.getUnsignedShort(12);\n" +
        "return newRecord0;\n" +
      "} else {\n" +
        "return null;\n" +
      "}\n");
  }
}
