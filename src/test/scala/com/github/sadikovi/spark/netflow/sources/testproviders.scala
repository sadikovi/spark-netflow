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

import com.github.sadikovi.netflowlib.predicate.Columns.{ByteColumn, ShortColumn, IntColumn, LongColumn}

/** Test providers */
private class DefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = null
}

private class WrongProvider

private class TestEmptyDefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = new TestEmptyInterface()
}

private class TestFullDefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = new TestFullInterface()
}

private class Test1FullDefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = new Test1FullInterface()
}

private class Test2FullDefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = new Test2FullInterface()
}

/** Test interfaces */
private class TestEmptyInterface extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq.empty

  override def version(): Short = -1
}

private class TestFullInterface extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq(
    MappedColumn("test", new LongColumn("test", 0), false, None))

  override def version(): Short = -2
}

private class Test1FullInterface extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq(
    MappedColumn("duplicate", new LongColumn("duplicate", 0), false, None),
    MappedColumn("duplicate", new LongColumn("duplicate", 0), false, None))

  override def version(): Short = -3
}

private class Test2FullInterface extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq(
    MappedColumn("test", new LongColumn("test1", 0), false, None),
    MappedColumn("test", new LongColumn("test2", 1), false, None))

  override def version(): Short = -3
}

////////////////////////////////////////////////////////////////
// Fake provider and interface for NetFlowFilters suite
////////////////////////////////////////////////////////////////

private class FakeDefaultProvider extends NetFlowProvider {
  def createInterface(): ResolvedInterface = new FakeInterface()
}

private class FakeInterface extends ResolvedInterface {
  override protected val columns: Seq[MappedColumn] = Seq(
    MappedColumn("col1", new ByteColumn("col1", 0), false, None),
    MappedColumn("col2", new ShortColumn("col2", 0), false, None),
    MappedColumn("col3", new IntColumn("col3", 0), false, None),
    MappedColumn("col4", new LongColumn("col4", 0), false, None))

  override def version(): Short = -1
}
