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

import io.netty.buffer.ByteBuf;

import com.github.sadikovi.netflowlib.codegen.CodeGenContext;
import com.github.sadikovi.netflowlib.codegen.CodeGenExpression;
import com.github.sadikovi.netflowlib.codegen.CodeGenNode;
import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;

/**
 * [[RecordMaterializer]] interface provides all necessary methods to parse single record and return
 * either array of values that match list and order of pruned columns, or null, if record does not
 * pass predicate. Predicate check is optional and should depend on implementation.
 */
public abstract class RecordMaterializer implements CodeGenExpression {
  RecordMaterializer() { }

  public abstract Object[] processRecord(ByteBuf buffer);

  /** Read buffer bytes sequence for column offset */
  public Object readField(Column column, ByteBuf buffer) {
    Class<?> type = column.getColumnType();
    if (type.equals(Byte.class)) {
      return buffer.getByte(column.getColumnOffset());
    } else if (type.equals(Short.class)) {
      return buffer.getUnsignedByte(column.getColumnOffset());
    } else if (type.equals(Integer.class)) {
      return buffer.getUnsignedShort(column.getColumnOffset());
    } else if (type.equals(Long.class)) {
      return buffer.getUnsignedInt(column.getColumnOffset());
    } else {
      throw new UnsupportedOperationException("Unsupported read type " + type);
    }
  }

  public void updateValueInspector(Column column, ByteBuf buffer, ValueInspector vi) {
    Class<?> type = column.getColumnType();
    if (type.equals(Byte.class)) {
      vi.update(buffer.getByte(column.getColumnOffset()));
    } else if (type.equals(Short.class)) {
      vi.update(buffer.getUnsignedByte(column.getColumnOffset()));
    } else if (type.equals(Integer.class)) {
      vi.update(buffer.getUnsignedShort(column.getColumnOffset()));
    } else if (type.equals(Long.class)) {
      vi.update(buffer.getUnsignedInt(column.getColumnOffset()));
    } else {
      throw new UnsupportedOperationException("Unsupported read type " + type);
    }
  }

  /** Produce generated code for `readField` method based on nodes for column and byte buffer */
  protected String generateReadField(CodeGenContext ctx, Column column, CodeGenNode buffer) {
    Class<?> type = column.getColumnType();
    String byteBuffer = buffer.nodeName(ctx);
    if (type.equals(Byte.class)) {
      return byteBuffer + ".getByte(" + column.getColumnOffset() + ")";
    } else if (type.equals(Short.class)) {
      return byteBuffer + ".getUnsignedByte(" + column.getColumnOffset() + ")";
    } else if (type.equals(Integer.class)) {
      return byteBuffer + ".getUnsignedShort(" + column.getColumnOffset() + ")";
    } else if (type.equals(Long.class)) {
      return byteBuffer + ".getUnsignedInt(" + column.getColumnOffset() + ")";
    } else {
      throw new UnsupportedOperationException("Unsupported read type " + type +
        " for code generation");
    }
  }

  /** Generate code for `processRecord` method using node for byte buffer */
  protected abstract String generateProcessRecord(CodeGenContext ctx, CodeGenNode buffer);

  /** Extract byte buffer node, if exists */
  private CodeGenNode generateByteBufferNode(CodeGenContext ctx) {
    return ctx.getNode("buffer");
  }

  /** Create new `newRecord` node for processing */
  protected CodeGenNode generateNewRecordNode(CodeGenContext ctx) {
    // Create codegen node for new record array
    CodeGenNode newRecord = new CodeGenNode() {
      private String currentNodeName = null;

      @Override
      public String nodeName(CodeGenContext ctx) {
        if (currentNodeName == null) {
          ctx.registerNameState("newRecord");
          currentNodeName = ctx.getAttributeName("newRecord");
        }
        return currentNodeName;
      }

      @Override
      public String nodeType(CodeGenContext ctx) {
        return "java.lang.Object[]";
      }

      @Override
      public Class<?> nodeClass(CodeGenContext ctx) {
        return java.lang.Object[].class;
      }
    };
    // Register codegen node for later reuse
    ctx.registerNode("newRecord", newRecord);
    return newRecord;
  }

  @Override
  public String generate(CodeGenContext ctx) {
    CodeGenNode buffer = generateByteBufferNode(ctx);
    return generateProcessRecord(ctx, buffer);
  }
}
