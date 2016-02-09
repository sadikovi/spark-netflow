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

/**
 * Internal Netflow Filter class that is converted from Spark `Filter` using mapper.
 *
 */
abstract class NetflowFilter

/**
 * Filter that evaluates to `true` iff field value is equal to value provided.
 *
 */
case class InternalEqualTo(field: Long, value: Long) extends NetflowFilter

/**
 * Filter that evaluates to `true` iff field value is greater than value provided.
 *
 */
case class InternalGreaterThan(field: Long, value: Long) extends NetflowFilter

/**
 * Filter that evaluates to `true` iff field value is greater than or equal to value provided.
 *
 */
case class InternalGreaterThanOrEqual(field: Long, value: Long) extends NetflowFilter

/**
 * Filter that evaluates to `true` iff field value is less than value provided.
 *
 */
case class InternalLessThan(field: Long, value: Long) extends NetflowFilter

/**
 * Filter that evaluates to `true` iff field value is less than or equal to value provided.
 *
 */
case class InternalLessThanOrEqual(field: Long, value: Long) extends NetflowFilter

/**
 * Filter that evaluates to `true` iff field value takes one of the values in sequence.
 *
 */
case class InternalIn(field: Long, values: Array[Long]) extends NetflowFilter

/**
 * Filter that evaluates to `true` iff left filter and right filter are both evaluated to `true`.
 *
 */
case class InternalAnd(left: NetflowFilter, right: NetflowFilter) extends NetflowFilter

/**
 * Filter that evaluates to `true` iff either left filter or right filter is evaluated to `true`.
 *
 */
case class InternalOr(left: NetflowFilter, right: NetflowFilter) extends NetflowFilter
