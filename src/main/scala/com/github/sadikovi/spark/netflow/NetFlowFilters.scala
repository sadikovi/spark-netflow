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

import com.github.sadikovi.netflowlib.predicate.Columns._
import com.github.sadikovi.netflowlib.predicate.FilterApi
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate
import com.github.sadikovi.netflowlib.predicate.Operators.{Eq => JEq, In => JIn}
import com.github.sadikovi.netflowlib.predicate.Operators.{Gt => JGt, Ge => JGe}
import com.github.sadikovi.netflowlib.predicate.Operators.{Lt => JLt, Le => JLe}
import com.github.sadikovi.netflowlib.predicate.Operators.{And => JAnd, Or => JOr, Not => JNot}
import com.github.sadikovi.spark.netflow.index.AttributeMap
import com.github.sadikovi.spark.netflow.sources._

/**
 * [[NetFlowFilters]] provides methods of converting Spark SQL filters into internal NetFlow
 * supported predicates. Also some filters are ignored, e.g. EqualNullSafe, and String related
 * filters, and are resolved into `TrivialPredicate` of false.
 */
private[spark] object NetFlowFilters {

  /** Reduce array of Spark filters to single `Filter` instance as option. */
  def reduceFilter(filters: Array[Filter]): Option[Filter] = {
    if (filters.isEmpty) {
      None
    } else if (filters.length == 1) {
      Option(filters.head)
    } else {
      // recursively collapse filters
      Option(filters.reduce { (left, right) => And(left, right) })
    }
  }

  /** Convert Spark SQL filters into internal NetFlow filters */
  def convertFilter(filter: Filter, catalog: ResolvedInterface): FilterPredicate = filter match {
    case EqualTo(attribute: String, value: Any) =>
      val mappedColumn = catalog.getColumn(attribute)
      makeEq(mappedColumn, maybeConvertValue(value, mappedColumn.convertFunction))
    case GreaterThan(attribute: String, value: Any) =>
      val mappedColumn = catalog.getColumn(attribute)
      makeGt(mappedColumn, maybeConvertValue(value, mappedColumn.convertFunction))
    case GreaterThanOrEqual(attribute: String, value: Any) =>
      val mappedColumn = catalog.getColumn(attribute)
      makeGe(mappedColumn, maybeConvertValue(value, mappedColumn.convertFunction))
    case LessThan(attribute: String, value: Any) =>
      val mappedColumn = catalog.getColumn(attribute)
      makeLt(mappedColumn, maybeConvertValue(value, mappedColumn.convertFunction))
    case LessThanOrEqual(attribute: String, value: Any) =>
      val mappedColumn = catalog.getColumn(attribute)
      makeLe(mappedColumn, maybeConvertValue(value, mappedColumn.convertFunction))
    case In(attribute: String, values: Array[Any]) =>
      val mappedColumn = catalog.getColumn(attribute)
      makeIn(mappedColumn, maybeConvertValues(values, mappedColumn.convertFunction))
    case IsNull(attribute: String) =>
      FilterApi.trivial(false)
    case IsNotNull(attribute: String) =>
      FilterApi.trivial(true)
    case And(left: Filter, right: Filter) =>
      FilterApi.and(convertFilter(left, catalog), convertFilter(right, catalog))
    case Or(left: Filter, right: Filter) =>
      FilterApi.or(convertFilter(left, catalog), convertFilter(right, catalog))
    case Not(child: Filter) =>
      FilterApi.not(convertFilter(child, catalog))
    // Currently list of unsupported filters includes:
    // `EqualNullSafe`, `StringStartsWith`, `StringEndsWith`, `StringContains`.
    // Full scan in NetFlow library and filtering in Spark will be done.
    case unsupportedFilter =>
      FilterApi.trivial(true)
  }

  /**
   * Update filter predicate based on attribute map. Column name of filter predicate is used as a
   * key of the attribute map to extract relevant attribute. For different predicate different
   * statistics data is used. No-op when attribute is not found, or filter is trivial.
   */
  def updateFilter(filter: FilterPredicate, attributes: AttributeMap): FilterPredicate = {
    filter match {
      case eq: JEq =>
        val key = eq.getColumn().getColumnName()
        val value = eq.getValue()
        attributes.in(key, value).orElse(attributes.between(key, value)) match {
          case Some(true) => eq
          case Some(false) => FilterApi.trivial(false)
          case None => eq
        }
      case gt: JGt =>
        val key = gt.getColumn().getColumnName()
        val value = gt.getValue()
        attributes.ltMax(key, value) match {
          case Some(true) => attributes.geMin(key, value) match {
            case Some(true) | None => gt
            case Some(false) => FilterApi.trivial(true)
          }
          case Some(false) => FilterApi.trivial(false)
          case None => gt
        }
      case ge: JGe =>
        val key = ge.getColumn().getColumnName()
        val value = ge.getValue()
        attributes.leMax(key, value) match {
          case Some(true) => attributes.gtMin(key, value) match {
            case Some(true) | None => ge
            case Some(false) => FilterApi.trivial(true)
          }
          case Some(false) => FilterApi.trivial(false)
          case None => ge
        }
      case lt: JLt =>
        val key = lt.getColumn().getColumnName()
        val value = lt.getValue()
        attributes.gtMin(key, value) match {
          case Some(true) => attributes.leMax(key, value) match {
            case Some(true) | None => lt
            case Some(false) => FilterApi.trivial(true)
          }
          case Some(false) => FilterApi.trivial(false)
          case None => lt
        }
      case le: JLe =>
        val key = le.getColumn().getColumnName()
        val value = le.getValue()
        attributes.geMin(key, value) match {
          case Some(true) => attributes.ltMax(key, value) match {
            case Some(true) | None => le
            case Some(false) => FilterApi.trivial(true)
          }
          case Some(false) => FilterApi.trivial(false)
          case None => le
        }
      case in: JIn =>
        val key = in.getColumn().getColumnName()
        val set = in.getValues()
        val iter = set.iterator()
        val updatedSet = new JHashSet[Any]()
        // Collect values that are determined to be removed
        while (iter.hasNext) {
          val value = iter.next
          attributes.in(key, value).orElse(attributes.between(key, value)) match {
            case Some(true) | None =>
              // Add values to updated set that confirm to statistics or it is no-op
              updatedSet.add(value)
            case Some(false) => // do nothing, this value is discarded
          }
        }
        FilterApi.in(in.getColumn(), updatedSet)
      case and: JAnd =>
        FilterApi.and(updateFilter(and.getLeft, attributes), updateFilter(and.getRight, attributes))
      case or: JOr =>
        FilterApi.or(updateFilter(or.getLeft, attributes), updateFilter(or.getRight, attributes))
      case not: JNot =>
        FilterApi.not(updateFilter(not.getChild, attributes))
      case otherPredicate =>
        // Return undefined filter unchanged
        otherPredicate
    }
  }

  //////////////////////////////////////////////////////////////
  // Making NetFlow filters
  //////////////////////////////////////////////////////////////

  private def makeEq(column: MappedColumn, value: Any): FilterPredicate = {
    column.internalColumn.getColumnType() match {
      case InternalType.BYTE => FilterApi.eq(column.internalColumn.asInstanceOf[ByteColumn],
        value.asInstanceOf[java.lang.Byte])
      case InternalType.SHORT => FilterApi.eq(column.internalColumn.asInstanceOf[ShortColumn],
        value.asInstanceOf[java.lang.Short])
      case InternalType.INT => FilterApi.eq(column.internalColumn.asInstanceOf[IntColumn],
        value.asInstanceOf[java.lang.Integer])
      case InternalType.LONG => FilterApi.eq(column.internalColumn.asInstanceOf[LongColumn],
        value.asInstanceOf[java.lang.Long])
      case otherInternalType =>
        throw new UnsupportedOperationException(s"Unsupported internal type $otherInternalType")
    }
  }

  private def makeGt(column: MappedColumn, value: Any): FilterPredicate = {
    column.internalColumn.getColumnType() match {
      case InternalType.BYTE => FilterApi.gt(column.internalColumn.asInstanceOf[ByteColumn],
        value.asInstanceOf[java.lang.Byte])
      case InternalType.SHORT => FilterApi.gt(column.internalColumn.asInstanceOf[ShortColumn],
        value.asInstanceOf[java.lang.Short])
      case InternalType.INT => FilterApi.gt(column.internalColumn.asInstanceOf[IntColumn],
        value.asInstanceOf[java.lang.Integer])
      case InternalType.LONG => FilterApi.gt(column.internalColumn.asInstanceOf[LongColumn],
        value.asInstanceOf[java.lang.Long])
      case otherInternalType =>
        throw new UnsupportedOperationException(s"Unsupported internal type $otherInternalType")
    }
  }

  private def makeGe(column: MappedColumn, value: Any): FilterPredicate = {
    column.internalColumn.getColumnType() match {
      case InternalType.BYTE => FilterApi.ge(column.internalColumn.asInstanceOf[ByteColumn],
        value.asInstanceOf[java.lang.Byte])
      case InternalType.SHORT => FilterApi.ge(column.internalColumn.asInstanceOf[ShortColumn],
        value.asInstanceOf[java.lang.Short])
      case InternalType.INT => FilterApi.ge(column.internalColumn.asInstanceOf[IntColumn],
        value.asInstanceOf[java.lang.Integer])
      case InternalType.LONG => FilterApi.ge(column.internalColumn.asInstanceOf[LongColumn],
        value.asInstanceOf[java.lang.Long])
      case otherInternalType =>
        throw new UnsupportedOperationException(s"Unsupported internal type $otherInternalType")
    }
  }

  private def makeLt(column: MappedColumn, value: Any): FilterPredicate = {
    column.internalColumn.getColumnType() match {
      case InternalType.BYTE => FilterApi.lt(column.internalColumn.asInstanceOf[ByteColumn],
        value.asInstanceOf[java.lang.Byte])
      case InternalType.SHORT => FilterApi.lt(column.internalColumn.asInstanceOf[ShortColumn],
        value.asInstanceOf[java.lang.Short])
      case InternalType.INT => FilterApi.lt(column.internalColumn.asInstanceOf[IntColumn],
        value.asInstanceOf[java.lang.Integer])
      case InternalType.LONG => FilterApi.lt(column.internalColumn.asInstanceOf[LongColumn],
        value.asInstanceOf[java.lang.Long])
      case otherInternalType =>
        throw new UnsupportedOperationException(s"Unsupported internal type $otherInternalType")
    }
  }

  private def makeLe(column: MappedColumn, value: Any): FilterPredicate = {
    column.internalColumn.getColumnType() match {
      case InternalType.BYTE => FilterApi.le(column.internalColumn.asInstanceOf[ByteColumn],
        value.asInstanceOf[java.lang.Byte])
      case InternalType.SHORT => FilterApi.le(column.internalColumn.asInstanceOf[ShortColumn],
        value.asInstanceOf[java.lang.Short])
      case InternalType.INT => FilterApi.le(column.internalColumn.asInstanceOf[IntColumn],
        value.asInstanceOf[java.lang.Integer])
      case InternalType.LONG => FilterApi.le(column.internalColumn.asInstanceOf[LongColumn],
        value.asInstanceOf[java.lang.Long])
      case otherInternalType =>
        throw new UnsupportedOperationException(s"Unsupported internal type $otherInternalType")
    }
  }

  private def makeIn(column: MappedColumn, values: Array[Any]): FilterPredicate = {
    column.internalColumn.getColumnType() match {
      case InternalType.BYTE =>
        val set = new JHashSet[java.lang.Byte]()
        for (vl <- values.map(_.asInstanceOf[java.lang.Byte])) {
          set.add(vl)
        }
        FilterApi.in(column.internalColumn.asInstanceOf[ByteColumn], set)
      case InternalType.SHORT =>
        val set = new JHashSet[java.lang.Short]()
        for (vl <- values.map(_.asInstanceOf[java.lang.Short])) {
          set.add(vl)
        }
        FilterApi.in(column.internalColumn.asInstanceOf[ShortColumn], set)
      case InternalType.INT =>
        val set = new JHashSet[java.lang.Integer]()
        for (vl <- values.map(_.asInstanceOf[java.lang.Integer])) {
          set.add(vl)
        }
        FilterApi.in(column.internalColumn.asInstanceOf[IntColumn], set)
      case InternalType.LONG =>
        val set = new JHashSet[java.lang.Long]()
        for (vl <- values.map(_.asInstanceOf[java.lang.Long])) {
          set.add(vl)
        }
        FilterApi.in(column.internalColumn.asInstanceOf[LongColumn], set)
      case otherInternalType =>
        throw new UnsupportedOperationException(s"Unsupported internal type $otherInternalType")
    }
  }

  //////////////////////////////////////////////////////////////
  // Util functions
  //////////////////////////////////////////////////////////////

  /**
   * Convertion of the value into internal format. When we convert String field, we check old
   * Spark SQL format `UTF8String` as well as `String`, which is obtained from StringType.
   */
  private def maybeConvertValue(
      value: Any,
      convertFunction: Option[ConvertFunction]): Any = convertFunction match {
    case Some(func) if value.isInstanceOf[String] =>
      func.reversed(value.asInstanceOf[String])
    case Some(func) if value.getClass().getCanonicalName().endsWith("UTF8String") =>
      func.reversed(value.toString())
    case otherCases =>
      value
  }

  /** Convertion of the array of values into internal format */
  private def maybeConvertValues(
      values: Array[Any],
      convertFunction: Option[ConvertFunction]): Array[Any] = {
    values.map { that => maybeConvertValue(that, convertFunction) }
  }
}
