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

import java.nio.ByteOrder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public class ByteBufRow extends InternalRow {
  private final int numFields;
  private final int[] offsets;
  private final ByteBuf buf;

  ByteBufRow(int[] offsets, byte[] data, ByteOrder byteOrder) {
    this.numFields = (offsets != null && offsets.length > 0) ? offsets.length : 0;
    this.offsets = offsets;
    this.buf = Unpooled.wrappedBuffer(data).order(byteOrder);
  }

  @Override
  public int numFields() {
    return this.numFields;
  }

  public void setNullAt(int ordinal) {
    throw new UnsupportedOperationException();
  }

  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  public void setBoolean(int ordinal, boolean value) {
    throw new UnsupportedOperationException();
  }

  public void setByte(int ordinal, byte value) {
    throw new UnsupportedOperationException();
  }

  public void setShort(int ordinal, short value) {
    throw new UnsupportedOperationException();
  }

  public void setInt(int ordinal, int value) {
    throw new UnsupportedOperationException();
  }

  public void setLong(int ordinal, long value) {
    throw new UnsupportedOperationException();
  }

  public void setFloat(int ordinal, float value) {
    throw new UnsupportedOperationException();
  }

  public void setDouble(int ordinal, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InternalRow copy() {
    byte[] dataCopy = new byte[buf.array().length];
    System.arraycopy(buf.array(), buf.arrayOffset(), dataCopy, 0, dataCopy.length);
    int[] offsetsCopy = new int[offsets.length];
    System.arraycopy(offsets, 0, offsetsCopy, 0, offsets.length);
    return new ByteBufRow(offsetsCopy, dataCopy, buf.order());
  }

  @Override
  public boolean anyNull() {
    // netflow data never has nulls
    return false;
  }

  //////////////////////////////////////////////////////////////
  // Specialized getters
  //////////////////////////////////////////////////////////////

  @Override
  public boolean isNullAt(int ordinal) {
    return false;
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return buf.getBoolean(offsets[ordinal]);
  }

  @Override
  public byte getByte(int ordinal) {
    return buf.getByte(offsets[ordinal]);
  }

  @Override
  public short getShort(int ordinal) {
    return buf.getUnsignedByte(offsets[ordinal]);
  }

  @Override
  public int getInt(int ordinal) {
    return buf.getUnsignedShort(offsets[ordinal]);
  }

  @Override
  public long getLong(int ordinal) {
    return buf.getUnsignedInt(offsets[ordinal]);
  }

  @Override
  public float getFloat(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrayData getArray(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MapData getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal) || dataType instanceof NullType) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType dt = (DecimalType) dataType;
      return getDecimal(ordinal, dt.precision(), dt.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof CalendarIntervalType) {
      return getInterval(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType) dataType).size());
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else if (dataType instanceof UserDefinedType) {
      return get(ordinal, ((UserDefinedType)dataType).sqlType());
    } else {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }
  }
}
