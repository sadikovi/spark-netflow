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

package com.github.sadikovi.spark.netflow

import java.util.{HashSet => JHashSet}

import org.apache.spark.sql.sources._

import com.github.sadikovi.netflowlib.predicate.FilterApi
import com.github.sadikovi.netflowlib.predicate.Columns.Column
import com.github.sadikovi.netflowlib.predicate.Operators.{FilterPredicate, In => JIn}
import com.github.sadikovi.spark.netflow.index.{Attribute, AttributeMap}
import com.github.sadikovi.spark.netflow.sources.NetFlowRegistry
import com.github.sadikovi.testutil.UnitTestSpec

class NetFlowFiltersSuite extends UnitTestSpec {
  // Fake interface to resolve filters
  private val catalog = NetFlowRegistry.createInterface(
    "com.github.sadikovi.spark.netflow.sources.FakeDefaultProvider")

  // Fake attribute map with dummy columns
  private val attributes = fakeAttributeMap()

  private def fakeAttributeMap(): AttributeMap = {
    val col2 = Attribute[Short]("col2", 6)
    col2.addValue(4.toShort)
    col2.addValue(6.toShort)
    col2.addValue(8.toShort)
    val col4 = Attribute[Long]("col4", 6)
    col4.addValue(1L)
    col4.addValue(5L)
    col4.addValue(12L)
    AttributeMap.empty.registerAttributes(col2 :: col4 :: Nil)
  }

  private def compareFilter(got: FilterPredicate, expected: FilterPredicate): Unit = {
    got.getClass() should be (expected.getClass())
    got should equal (expected)
  }

  private def inFilter(column: Column, values: Array[Any]): JIn = {
    val set = new JHashSet[Any]();
    for (vl <- values) {
      set.add(vl)
    }

    FilterApi.in(column, set)
  }

  test("reduce filter") {
    // simple filter
    var filters: Array[Filter] = Array(EqualTo("unix_secs", 1L), GreaterThan("srcip", 1L))
    var resultFilter = NetFlowFilters.reduceFilter(filters).get
    resultFilter should be (And(
      EqualTo("unix_secs", 1L),
      GreaterThan("srcip", 1L)
    ))

    // complex filter with `OR` and `AND`
    filters = Array(
      Or(And(EqualTo("unix_secs", 1L), GreaterThan("srcip", 1L)), LessThanOrEqual("dstip", 0L))
    )
    resultFilter = NetFlowFilters.reduceFilter(filters).get
    resultFilter should be (Or(
        And(
          EqualTo("unix_secs", 1L),
          GreaterThan("srcip", 1L)
        ),
        LessThanOrEqual("dstip", 0L)
      )
    )

    // filter with unresolved step
    filters = Array(IsNull("unix_secs"), GreaterThan("srcip", 1L))
    resultFilter = NetFlowFilters.reduceFilter(filters).get
    resultFilter should be (And(
      IsNull("unix_secs"),
      GreaterThan("srcip", 1L)
    ))

    // no filters
    NetFlowFilters.reduceFilter(Array.empty) should be (None)
  }

  test("convert filter - EqualTo") {
    var pred = NetFlowFilters.convertFilter(EqualTo("col1", 10.toByte), catalog)
    compareFilter(pred, FilterApi.eq(catalog.getColumn("col1").internalColumn, 10.toByte))

    pred = NetFlowFilters.convertFilter(EqualTo("col2", 11.toShort), catalog)
    compareFilter(pred, FilterApi.eq(catalog.getColumn("col2").internalColumn, 11.toShort))

    pred = NetFlowFilters.convertFilter(EqualTo("col3", 12), catalog)
    compareFilter(pred, FilterApi.eq(catalog.getColumn("col3").internalColumn, 12))

    pred = NetFlowFilters.convertFilter(EqualTo("col4", 14.toLong), catalog)
    compareFilter(pred, FilterApi.eq(catalog.getColumn("col4").internalColumn, 14.toLong))
  }

  test("convert filter - GreaterThan") {
    var pred = NetFlowFilters.convertFilter(GreaterThan("col1", 10.toByte), catalog)
    compareFilter(pred, FilterApi.gt(catalog.getColumn("col1").internalColumn, 10.toByte))

    pred = NetFlowFilters.convertFilter(GreaterThan("col2", 11.toShort), catalog)
    compareFilter(pred, FilterApi.gt(catalog.getColumn("col2").internalColumn, 11.toShort))

    pred = NetFlowFilters.convertFilter(GreaterThan("col3", 12), catalog)
    compareFilter(pred, FilterApi.gt(catalog.getColumn("col3").internalColumn, 12))

    pred = NetFlowFilters.convertFilter(GreaterThan("col4", 14.toLong), catalog)
    compareFilter(pred, FilterApi.gt(catalog.getColumn("col4").internalColumn, 14.toLong))
  }

  test("convert filter - GreaterThanOrEqual") {
    var pred = NetFlowFilters.convertFilter(GreaterThanOrEqual("col1", 10.toByte), catalog)
    compareFilter(pred, FilterApi.ge(catalog.getColumn("col1").internalColumn, 10.toByte))

    pred = NetFlowFilters.convertFilter(GreaterThanOrEqual("col2", 11.toShort), catalog)
    compareFilter(pred, FilterApi.ge(catalog.getColumn("col2").internalColumn, 11.toShort))

    pred = NetFlowFilters.convertFilter(GreaterThanOrEqual("col3", 12), catalog)
    compareFilter(pred, FilterApi.ge(catalog.getColumn("col3").internalColumn, 12))

    pred = NetFlowFilters.convertFilter(GreaterThanOrEqual("col4", 14.toLong), catalog)
    compareFilter(pred, FilterApi.ge(catalog.getColumn("col4").internalColumn, 14.toLong))
  }

  test("convert filter - LessThan") {
    var pred = NetFlowFilters.convertFilter(LessThan("col1", 10.toByte), catalog)
    compareFilter(pred, FilterApi.lt(catalog.getColumn("col1").internalColumn, 10.toByte))

    pred = NetFlowFilters.convertFilter(LessThan("col2", 11.toShort), catalog)
    compareFilter(pred, FilterApi.lt(catalog.getColumn("col2").internalColumn, 11.toShort))

    pred = NetFlowFilters.convertFilter(LessThan("col3", 12), catalog)
    compareFilter(pred, FilterApi.lt(catalog.getColumn("col3").internalColumn, 12))

    pred = NetFlowFilters.convertFilter(LessThan("col4", 14.toLong), catalog)
    compareFilter(pred, FilterApi.lt(catalog.getColumn("col4").internalColumn, 14.toLong))
  }

  test("convert filter - LessThanOrEqual") {
    var pred = NetFlowFilters.convertFilter(LessThanOrEqual("col1", 10.toByte), catalog)
    compareFilter(pred, FilterApi.le(catalog.getColumn("col1").internalColumn, 10.toByte))

    pred = NetFlowFilters.convertFilter(LessThanOrEqual("col2", 11.toShort), catalog)
    compareFilter(pred, FilterApi.le(catalog.getColumn("col2").internalColumn, 11.toShort))

    pred = NetFlowFilters.convertFilter(LessThanOrEqual("col3", 12), catalog)
    compareFilter(pred, FilterApi.le(catalog.getColumn("col3").internalColumn, 12))

    pred = NetFlowFilters.convertFilter(LessThanOrEqual("col4", 14.toLong), catalog)
    compareFilter(pred, FilterApi.le(catalog.getColumn("col4").internalColumn, 14.toLong))
  }

  test("convert filter - In") {
    var values = Array[Any](1.toByte, 2.toByte, 3.toByte, 4.toByte, 5.toByte)
    var pred = NetFlowFilters.convertFilter(In("col1", values), catalog)
    compareFilter(pred, inFilter(catalog.getColumn("col1").internalColumn, values))

    values = Array[Any](1.toShort, 2.toShort, 3.toShort, 4.toShort, 5.toShort)
    pred = NetFlowFilters.convertFilter(In("col2", values), catalog)
    compareFilter(pred, inFilter(catalog.getColumn("col2").internalColumn, values))

    values = Array[Any](1, 2, 3, 4, 5)
    pred = NetFlowFilters.convertFilter(In("col3", values), catalog)
    compareFilter(pred, inFilter(catalog.getColumn("col3").internalColumn, values))

    values = Array[Any](1L, 2L, 3L, 4L, 5L)
    pred = NetFlowFilters.convertFilter(In("col4", values), catalog)
    compareFilter(pred, inFilter(catalog.getColumn("col4").internalColumn, values))
  }

  test("fail to filter In with array of values of different types") {
    intercept[ClassCastException] {
      val values = Array[Any](1.toByte, 2.toShort, 3, 4.toLong)
      val pred = NetFlowFilters.convertFilter(In("col4", values), catalog)
      compareFilter(pred, inFilter(catalog.getColumn("col4").internalColumn, values))
    }
  }

  test("convert filter - IsNull, IsNotNull") {
    var pred = NetFlowFilters.convertFilter(IsNull("col1"), catalog)
    compareFilter(pred, FilterApi.trivial(false))

    pred = NetFlowFilters.convertFilter(IsNotNull("col1"), catalog)
    compareFilter(pred, FilterApi.trivial(true))
  }

  test("convert filter - And, Or, Not") {
    var filter: Filter = null
    var pred: FilterPredicate = null
    val col3 = catalog.getColumn("col3").internalColumn
    val col4 = catalog.getColumn("col4").internalColumn

    filter = And(EqualTo("col3", 10), GreaterThan("col3", 200))
    pred = NetFlowFilters.convertFilter(filter, catalog)
    compareFilter(pred, FilterApi.and(FilterApi.eq(col3, 10), FilterApi.gt(col3, 200)))

    filter = And(IsNull("col3"), GreaterThan("col3", 200))
    pred = NetFlowFilters.convertFilter(filter, catalog)
    compareFilter(pred, FilterApi.and(FilterApi.trivial(false), FilterApi.gt(col3, 200)))

    filter = Or(And(IsNull("col3"), GreaterThan("col3", 200)), Not(LessThan("col4", 10L)))
    pred = NetFlowFilters.convertFilter(filter, catalog)
    compareFilter(pred,
      FilterApi.or(
        FilterApi.and(
          FilterApi.trivial(false),
          FilterApi.gt(col3, 200)
        ),
        FilterApi.not(
          FilterApi.lt(col4, 10L)
        )
      )
    )
  }

  test("convert unsupported filter") {
    var pred = NetFlowFilters.convertFilter(StringStartsWith("col1", "a"), catalog)
    compareFilter(pred, FilterApi.trivial(true))

    pred = NetFlowFilters.convertFilter(StringEndsWith("col1", "a"), catalog)
    compareFilter(pred, FilterApi.trivial(true))

    pred = NetFlowFilters.convertFilter(StringContains("col1", "a"), catalog)
    compareFilter(pred, FilterApi.trivial(true))
  }

  test("unconvertable filter value") {
    intercept[ClassCastException] {
      NetFlowFilters.convertFilter(GreaterThan("col1", "10"), catalog)
    }
  }

  //////////////////////////////////////////////////////////////
  // Update filter based on statistics
  //////////////////////////////////////////////////////////////
  test("update filter - eq") {
    val col4 = catalog.getColumn("col4").internalColumn
    var filter: FilterPredicate = null
    // Equality
    filter = FilterApi.eq(col4, 5L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), filter)

    // Not in the set
    filter = FilterApi.eq(col4, 6L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(false))
  }

  test("update filter - in") {
    val col4 = catalog.getColumn("col4").internalColumn
    var filter: FilterPredicate = null
    // In equality
    filter = inFilter(col4, Array(1L, 2L, 3L))
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), inFilter(col4, Array(1L)))

    // Not in the set
    filter = inFilter(col4, Array(100L, 101L))
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), inFilter(col4, Array()))
  }

  test("update filter - gt") {
    val col4 = catalog.getColumn("col4").internalColumn
    var filter: FilterPredicate = null

    // Value is within the range
    filter = FilterApi.gt(col4, 4L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), filter)

    // Value is less than min
    filter = FilterApi.gt(col4, -1L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(true))

    // Value equals min
    filter = FilterApi.gt(col4, 1L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), filter)

    // Value is greater than max
    filter = FilterApi.gt(col4, 15L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(false))

    // Value equals max
    filter = FilterApi.gt(col4, 12L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(false))
  }

  test("update filter - ge") {
    val col4 = catalog.getColumn("col4").internalColumn
    var filter: FilterPredicate = null

    // Value is within the range
    filter = FilterApi.ge(col4, 4L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), filter)

    // Value is less than min
    filter = FilterApi.ge(col4, -1L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(true))

    // Value equals min
    filter = FilterApi.ge(col4, 1L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(true))

    // Value is greater than max
    filter = FilterApi.ge(col4, 15L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(false))

    // Value equals max
    filter = FilterApi.ge(col4, 12L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), filter)
  }

  test("update filter - lt") {
    val col4 = catalog.getColumn("col4").internalColumn
    var filter: FilterPredicate = null

    // Value is within the range
    filter = FilterApi.lt(col4, 4L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), filter)

    // Value is less than min
    filter = FilterApi.lt(col4, -1L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(false))

    // Value equals min
    filter = FilterApi.lt(col4, 1L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(false))

    // Value is greater than max
    filter = FilterApi.lt(col4, 15L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(true))

    // Value equals max
    filter = FilterApi.lt(col4, 12L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), filter)
  }

  test("update filter - le") {
    val col4 = catalog.getColumn("col4").internalColumn
    var filter: FilterPredicate = null

    // Value is within the range
    filter = FilterApi.le(col4, 4L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), filter)

    // Value is less than min
    filter = FilterApi.le(col4, -1L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(false))

    // Value equals min
    filter = FilterApi.le(col4, 1L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), filter)

    // Value is greater than max
    filter = FilterApi.le(col4, 15L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(true))

    // Value equals max
    filter = FilterApi.le(col4, 12L)
    compareFilter(NetFlowFilters.updateFilter(filter, attributes), FilterApi.trivial(true))
  }

  test("update filter - and") {
    val col2 = catalog.getColumn("col2").internalColumn
    val col3 = catalog.getColumn("col3").internalColumn
    val col4 = catalog.getColumn("col4").internalColumn
    var filter: FilterPredicate = null

    // Case 1
    filter = FilterApi.and(FilterApi.eq(col4, 3L), FilterApi.eq(col2, 4.toShort))
    compareFilter(NetFlowFilters.updateFilter(filter, attributes),
      FilterApi.and(FilterApi.trivial(false), FilterApi.eq(col2, 4.toShort)))

    // Case 2
    filter = FilterApi.and(FilterApi.eq(col3, 10), FilterApi.eq(col4, 3L))
    compareFilter(NetFlowFilters.updateFilter(filter, attributes),
      FilterApi.and(FilterApi.eq(col3, 10), FilterApi.trivial(false)))

    // Case 3
    filter = FilterApi.and(FilterApi.ge(col4, 2L), FilterApi.le(col4, 20L))
    compareFilter(NetFlowFilters.updateFilter(filter, attributes),
      FilterApi.and(FilterApi.ge(col4, 2L), FilterApi.trivial(true)))
  }

  test("update filter - or") {
    val col2 = catalog.getColumn("col2").internalColumn
    val col3 = catalog.getColumn("col3").internalColumn
    val col4 = catalog.getColumn("col4").internalColumn
    var filter: FilterPredicate = null

    // Case 1
    filter = FilterApi.or(FilterApi.eq(col4, 3L), FilterApi.eq(col2, 5.toShort))
    compareFilter(NetFlowFilters.updateFilter(filter, attributes),
      FilterApi.or(FilterApi.trivial(false), FilterApi.trivial(false)))

    // Case 2
    filter = FilterApi.or(FilterApi.eq(col3, 10), FilterApi.eq(col4, 3L))
    compareFilter(NetFlowFilters.updateFilter(filter, attributes),
      FilterApi.or(FilterApi.eq(col3, 10), FilterApi.trivial(false)))

    // Case 3
    filter = FilterApi.or(FilterApi.lt(col4, 2L), FilterApi.gt(col4, 12L))
    compareFilter(NetFlowFilters.updateFilter(filter, attributes),
      FilterApi.or(FilterApi.lt(col4, 2L), FilterApi.trivial(false)))
  }

  test("update filter - not") {
    val col2 = catalog.getColumn("col2").internalColumn
    val col3 = catalog.getColumn("col3").internalColumn
    val col4 = catalog.getColumn("col4").internalColumn
    var filter: FilterPredicate = null

    filter = FilterApi.not(FilterApi.eq(col4, 3L))
    compareFilter(NetFlowFilters.updateFilter(filter, attributes),
      FilterApi.not(FilterApi.trivial(false)))
  }
}
