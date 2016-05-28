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

package com.github.sadikovi.spark.netflow.sources

import org.apache.spark.sql.types._

import com.github.sadikovi.netflowlib.predicate.Columns.Column

/**
 * Representation of columns of Spark SQL into NetFlow fields.
 * @param columnName SQL name of the column (in most cases derived from internal column)
 * @param internalColumn internal NetFlow column
 * @param collectStatistics whether or not to collect statistics on column
 * @param convertFunction possible conversion (usually to human-readable format)
 */
private[spark] case class MappedColumn(
  columnName: String,
  internalColumn: Column,
  collectStatistics: Boolean,
  convertFunction: Option[ConvertFunction]
)

/** Internal types, used to resolve SQL type for a column */
private[spark] object InternalType {
  val BYTE = classOf[java.lang.Byte]
  val SHORT = classOf[java.lang.Short]
  val INT = classOf[java.lang.Integer]
  val LONG = classOf[java.lang.Long]
}

/** Abstract interface for NetFlow version */
abstract class ResolvedInterface {

  /** Interface columns, sequence has to contain at least one column */
  protected val columns: Seq[MappedColumn]

  /** Interface version. */
  def version(): Short

  /**
   * Get Spark SQL schema for columns. If `applyConversion` then we specify `StringType` as column
   * type, since conversion is always "Any -> String". All fields are non-nullable.
   * @param applyConversion change type to `StringType` if true
   * @return Spark SQL schema
   */
  def getSQLSchema(applyConversion: Boolean): StructType = {
    val sqlColumns = columns.map(column => {
      if (applyConversion && column.convertFunction.isDefined) {
        StructField(column.columnName, StringType, false)
      } else {
        StructField(column.columnName, javaToSQLType(column.internalColumn.getColumnType()), false)
      }
    })
    StructType(sqlColumns)
  }

  /** Get all `MappedColumn` instances. */
  def getColumns(): Seq[MappedColumn] = columns

  /** Get first `MappedColumn` (mostly used for count to avoid reading entire record). */
  def getFirstColumn(): MappedColumn = columns.head

  /** Get first `MappedColumn` as `Option`. */
  def getFirstColumnOption(): Option[MappedColumn] = columns.headOption

  /** Get columns with enabled statistics */
  def getStatisticsColumns(): Seq[MappedColumn] = columns.filter { _.collectStatistics }

  /** Get `MappedColumn` for a specified column name. Fail, if column name is not present. */
  def getColumn(columnName: String): MappedColumn = {
    columnsMap.getOrElse(columnName,
      sys.error(s"Interface does not have information about column $columnName"))
  }

  /** Get size in bytes for a particular SQL data type. */
  private[sources] def sizeInBytes(dtype: DataType): Short = dtype match {
    case byte: ByteType => 1
    case short: ShortType => 2
    case int: IntegerType => 4
    case long: LongType => 8
    case other => throw new UnsupportedOperationException(s"Cannot get size for $other type")
  }

  /** Resolve internal type into SQL type */
  private[sources] def javaToSQLType(clazz: Class[_]): DataType = clazz match {
    case InternalType.BYTE => ByteType
    case InternalType.SHORT => ShortType
    case InternalType.INT => IntegerType
    case InternalType.LONG => LongType
    case otherType => throw new UnsupportedOperationException(s"Unsupported type $otherType")
  }

  private[sources] def ensureColumnConsistency(): Unit = {
    if (columns.isEmpty) {
      throw new IllegalArgumentException(s"Columns are empty for ${toString()}")
    }
    // check that columns (SQL and internal) are not duplicated
    val columnNames = columns.map(_.columnName)
    assert(columnNames.length == columnNames.distinct.length,
      s"Found duplicate column names in ${toString()}")

    val internalColumnNames = columns.map(_.internalColumn)
    assert(internalColumnNames.length == internalColumnNames.distinct.length,
      s"Found duplicate internal column names in ${toString()}")
  }

  override def toString(): String = {
    s"Interface: ${getClass.getCanonicalName} for version ${version()}"
  }

  private lazy val columnsMap: Map[String, MappedColumn] = columns.map(mappedColumn =>
    (mappedColumn.columnName, mappedColumn)).toMap
}
