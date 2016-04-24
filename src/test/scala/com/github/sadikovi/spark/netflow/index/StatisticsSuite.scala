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

package com.github.sadikovi.spark.netflow.index

import java.nio.ByteOrder
import java.util.{HashSet => JHashSet}

import io.netty.buffer.{ByteBuf, Unpooled}

import com.github.sadikovi.spark.util.Utils
import com.github.sadikovi.testutil.UnitTestSpec

class StatisticsSuite extends UnitTestSpec {
  test("resolve statistics path without root") {
    val resolver = StatisticsPathResolver(None)
    val path = resolver.getStatisticsPath("file:/x/y/z/file")
    path should be ("file:/x/y/z/.statistics-file")
  }

  test("resolve statistics path with root") {
    val resolver = StatisticsPathResolver(Some("file:/a/b/c"))
    val path = resolver.getStatisticsPath("file:/x/y/z/file")
    path should be ("file:/a/b/c/x/y/z/.statistics-file")
  }

  test("fail if root path is null") {
    intercept[IllegalArgumentException] {
      StatisticsPathResolver(Some(null))
    }
  }

  test("fail if root path is empty") {
    intercept[IllegalArgumentException] {
      StatisticsPathResolver(Some(""))
    }
  }

  test("fail if file path is null") {
    val resolver = StatisticsPathResolver(None)
    intercept[IllegalArgumentException] {
      resolver.getStatisticsPath(null)
    }
  }

  test("fail if file path is empty") {
    val resolver = StatisticsPathResolver(None)
    intercept[IllegalArgumentException] {
      resolver.getStatisticsPath("")
    }
  }

  test("create and update attribute") {
    val attr = Attribute[Int]("a", 7)
    for (i <- 1 to 3) {
      attr.addValue(i)
    }
    attr.getCount() should be (Some(3))
    attr.containsInRange(2) should be (Some(true))
    attr.containsInSet(2) should be (Some(true))
  }

  test("get number of statistics") {
    Attribute[Int]("a", 1).numStatistics() should be (1)
    Attribute[Int]("a", 3).numStatistics() should be (2)
    Attribute[Int]("a", 5).numStatistics() should be (2)
    Attribute[Int]("a", 6).numStatistics() should be (2)
    Attribute[Int]("a", 7).numStatistics() should be (3)
  }

  test("get value from attribute for range mode") {
    val attr = Attribute[Int]("a", 2)
    attr.addValue(3)
    attr.addValue(5)
    attr.containsInRange(2) should be (Some(false))
    attr.containsInRange(3) should be (Some(true))
    attr.containsInRange(4) should be (Some(true))
    attr.containsInRange(5) should be (Some(true))
    attr.containsInRange(6) should be (Some(false))
  }

  test("get value from attribute for set mode") {
    val attr = Attribute[Int]("a", 4)
    attr.addValue(3)
    attr.addValue(5)
    attr.containsInSet(2) should be (Some(false))
    attr.containsInSet(3) should be (Some(true))
    attr.containsInSet(4) should be (Some(false))
    attr.containsInSet(5) should be (Some(true))
    attr.containsInSet(6) should be (Some(false))
  }

  test("check value using empty attribute") {
    val attr = Attribute[Int]("a", 7)
    attr.getCount() should be (Some(0))
    attr.containsInRange(5) should be (Some(false))
    attr.containsInSet(5) should be (Some(false))
  }

  test("get value from attribute for count mode") {
    val attr = Attribute[Int]("a", 1)
    attr.getCount() should be (Some(0))
    attr.addValue(3)
    attr.addValue(3)
    attr.getCount() should be (Some(2))
  }

  test("get value from attribute for incorrect mode") {
    val attr = Attribute[Int]("a", 8)
    attr.getCount() should be (None)
    attr.containsInRange(1) should be (None)
    attr.containsInSet(1) should be (None)
  }

  test("update values of attribute directly") {
    val attr = Attribute[Int]("a", 7)
    attr.setCount(10)
    attr.setMinMax(2, 5)
    val set = new JHashSet[Int]()
    set.add(2)
    set.add(5)
    attr.setSet(set)

    attr.getCount() should be (Some(10))
    attr.containsInRange(2) should be (Some(true))
    attr.containsInSet(2) should be (Some(true))
  }

  test("fail when updating attribute for unset mode") {
    val attr = Attribute[Int]("a", 8)
    intercept[IllegalArgumentException] {
      attr.setCount(1)
    }

    intercept[IllegalArgumentException] {
      attr.setMinMax(1, 2)
    }

    intercept[IllegalArgumentException] {
      attr.setSet(new JHashSet[Int]())
    }
  }

  test("get internal count, min/max, set for attribute") {
    val attr = Attribute[Int]("a", 7)
    attr.getCount() should be (Some(0))
    attr.getMinMax() should be (Some(null, null))
    attr.getSet() should be (Some(new JHashSet[Int]()))
  }

  test("get internal count for attribute") {
    val attr = Attribute[Int]("a", 1)
    attr.getCount() should be (Some(0))
    attr.getMinMax() should be (None)
    attr.getSet() should be (None)
  }

  test("get runtime class") {
    val a = Attribute[Short]("a", 7)
    val b = Attribute[Int]("b", 7)
    val c = Attribute[Long]("c", 7)
    a.getClassTag() should be (classOf[Short])
    b.getClassTag() should be (classOf[Int])
    c.getClassTag() should be (classOf[Long])
  }

  test("check null on empty attribute") {
    val attr = Attribute[Int]("a", 7)
    attr.containsNull() should be (true)
  }

  test("check null on non-null attribute") {
    val attr = Attribute[Int]("a", 7)
    attr.addValue(1)
    attr.addValue(2)
    attr.containsNull() should be (false)
  }

  test("check null on null attribute") {
    val attr = Attribute[String]("a", 7)
    attr.addValue("a")
    attr.addValue("b")
    attr.addValue(null)
    attr.containsNull() should be (true)
  }

  test("check null on manually set attribute 1") {
    val attr = Attribute[String]("a", 7)
    attr.setCount(10)
    attr.setMinMax("a", null)
    attr.containsNull() should be (true)
  }

  test("check null on manually set attribute 2") {
    val attr = Attribute[String]("a", 7)
    attr.setMinMax("a", "b")
    attr.containsNull() should be (false)
  }

  test("check null on manually set attribute 3") {
    val attr = Attribute[String]("a", 7)
    val set = new JHashSet[String]()
    set.add(null)
    attr.setMinMax("a", "b")
    attr.setSet(set)
    attr.containsNull() should be (true)
  }

  test("attribute contains null in range") {
    var attr = Attribute[String]("a", 7)
    attr.addValue("a")
    attr.containsInRange(null) should be (Some(false))

    attr = Attribute[String]("a", 1)
    attr.addValue("a")
    attr.containsInRange(null) should be (None)
  }

  test("attribute contains null in set") {
    var attr = Attribute[String]("a", 4)
    attr.addValue("a")
    attr.containsInSet(null) should be (Some(false))

    attr = Attribute[String]("a", 4)
    attr.addValue(null)
    attr.containsInSet(null) should be (Some(true))
  }

  test("statistics utils - getBytes") {
    StatisticsUtils.getBytes(classOf[Byte]) should be (1)
    StatisticsUtils.getBytes(classOf[Short]) should be (2)
    StatisticsUtils.getBytes(classOf[Int]) should be (4)
    StatisticsUtils.getBytes(classOf[Long]) should be (8)
    StatisticsUtils.getBytes(classOf[String]) should be (32)

    intercept[RuntimeException] {
      StatisticsUtils.getBytes(classOf[Char])
    }
  }

  test("statistics-utils - getClassTag") {
    StatisticsUtils.getClassTag(1) should be (classOf[Byte])
    StatisticsUtils.getClassTag(2) should be (classOf[Short])
    StatisticsUtils.getClassTag(4) should be (classOf[Int])
    StatisticsUtils.getClassTag(8) should be (classOf[Long])
    StatisticsUtils.getClassTag(32) should be (classOf[String])

    intercept[RuntimeException] {
      StatisticsUtils.getClassTag(16)
    }
  }

  test("statistics-utils - withBytes") {
    val buffer: ByteBuf = Unpooled.buffer()
    StatisticsUtils.withBytes(buffer, 4) { buf =>
      buf.writeInt(123)
    }

    // This should fail since we request 4 bytes, but write 8 bytes
    intercept[IllegalArgumentException] {
      StatisticsUtils.withBytes(buffer, 4) { buf =>
        buf.writeLong(123L)
      }
    }
  }

  test("statistics-utils - getEndianness") {
    StatisticsUtils.getEndianness(2) should be (ByteOrder.BIG_ENDIAN)
    StatisticsUtils.getEndianness(0) should be (ByteOrder.BIG_ENDIAN)
    StatisticsUtils.getEndianness(-1) should be (ByteOrder.BIG_ENDIAN)
    StatisticsUtils.getEndianness(1) should be (ByteOrder.LITTLE_ENDIAN)
  }

  test("statistics-utils - getEndiannessIndex") {
    StatisticsUtils.getEndiannessIndex(ByteOrder.BIG_ENDIAN) should be (2)
    StatisticsUtils.getEndiannessIndex(ByteOrder.LITTLE_ENDIAN) should be (1)
  }

  test("statistics-utils - getEndianness 2") {
    StatisticsUtils.getEndianness(StatisticsUtils.getEndiannessIndex(
      ByteOrder.BIG_ENDIAN)) should be (ByteOrder.BIG_ENDIAN)
    StatisticsUtils.getEndianness(StatisticsUtils.getEndiannessIndex(
      ByteOrder.LITTLE_ENDIAN)) should be (ByteOrder.LITTLE_ENDIAN)
  }

  test("statistics-utils - writeValue/readValue") {
    val buffer: ByteBuf = Unpooled.buffer()
    StatisticsUtils.writeValue(buffer, Byte.MaxValue, classOf[Byte])
    StatisticsUtils.readValue(buffer, classOf[Byte]) should be (Byte.MaxValue)

    StatisticsUtils.writeValue(buffer, Short.MaxValue, classOf[Short])
    StatisticsUtils.readValue(buffer, classOf[Short]) should be (Short.MaxValue)

    StatisticsUtils.writeValue(buffer, Int.MaxValue, classOf[Int])
    StatisticsUtils.readValue(buffer, classOf[Int]) should be (Int.MaxValue)

    StatisticsUtils.writeValue(buffer, Long.MaxValue, classOf[Long])
    StatisticsUtils.readValue(buffer, classOf[Long]) should be (Long.MaxValue)

    StatisticsUtils.writeValue(buffer, "!QAZ1qaz", classOf[String])
    StatisticsUtils.readValue(buffer, classOf[String]) should be ("!QAZ1qaz")
  }

  test("write and read empty statistics") {
    Utils.withTempFile { file =>
      val writer = new StatisticsWriter(ByteOrder.LITTLE_ENDIAN, Seq.empty)
      writer.save(file.toString)
      val reader = new StatisticsReader()
      reader.load(file.toString).length should be (0)
    }
  }

  test("write and read empty attribute") {
    val empty = Attribute[Long]("empty", 7)

    Utils.withTempFile { file =>
      val writer = new StatisticsWriter(ByteOrder.LITTLE_ENDIAN, Seq(empty))
      writer.save(file.toString)
      val reader = new StatisticsReader()
      val maybeEmpty = reader.load(file.toString).head

      empty.equals(maybeEmpty) should be (true)
    }
  }

  test("write and read non-null attributes") {
    val a = Attribute[Int]("a", 3)
    a.addValue(-1)
    a.addValue(0)
    a.addValue(2)
    val b = Attribute[Short]("b", 6)
    b.addValue(-2)
    b.addValue(-2)
    b.addValue(-1)

    Utils.withTempFile { file =>
      val writer = new StatisticsWriter(ByteOrder.BIG_ENDIAN, Seq(a, b))
      writer.save(file.toString)
      val reader = new StatisticsReader()
      val attrs = reader.load(file.toString)

      attrs.length should be (2)
      a.equals(attrs.head) should be (true)
      b.equals(attrs.last) should be (true)
    }
  }

  test("write and read mixed attributes") {
    val a = Attribute[String]("a", 3)
    a.addValue("a")
    a.addValue("b")
    a.addValue(null)
    a.addValue("c")
    val b = Attribute[Short]("b", 6)
    b.addValue(1)
    b.addValue(0)

    Utils.withTempFile { file =>
      val writer = new StatisticsWriter(ByteOrder.BIG_ENDIAN, Seq(a, b))
      writer.save(file.toString)
      val reader = new StatisticsReader()
      val attrs = reader.load(file.toString)

      attrs.length should be (2)
      a.equals(attrs.head) should be (true)
      b.equals(attrs.last) should be (true)
    }
  }
}
