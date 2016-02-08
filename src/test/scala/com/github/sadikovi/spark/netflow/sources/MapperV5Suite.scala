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

import org.apache.spark.sql.types.StringType

import com.github.sadikovi.netflowlib.version.NetflowV5
import com.github.sadikovi.testutil.UnitTestSpec

class MapperV5Suite extends UnitTestSpec {

  test("get full schema") {
    var schema = MapperV5.getFullSchema(false)

    schema.length should be (MapperV5.getInternalColumns().length)
    schema.forall(p => p.dataType != StringType)

    schema = MapperV5.getFullSchema(true)
    schema.length should be (MapperV5.getInternalColumns().length)
    schema.exists(p => p.dataType == StringType)
  }

  test("get internal columns") {
    MapperV5.getInternalColumns().take(1) should be (MapperV5.getFirstInternalColumn())
  }

  test("get internal column for name") {
    intercept[Exception] {
      MapperV5.getInternalColumnForName("wrong-column")
    }

    MapperV5.getInternalColumnForName("unix_secs") should be (NetflowV5.V5_FIELD_UNIX_SECS)
    MapperV5.getInternalColumnForName("srcip") should be (NetflowV5.V5_FIELD_SRCADDR)
  }

  test("get conversions for fields") {
    var conversions = MapperV5.getConversionsForFields(Array.empty)
    conversions.isEmpty should be (true)

    conversions = MapperV5.getConversionsForFields(Array(NetflowV5.V5_FIELD_SRCADDR))
    conversions.isEmpty should be (false)
  }

  test("get readable summary") {
    val summary = MapperV5.getSummaryReadable("")
    val stats = summary.get.finalizeStatistics()
    stats.getOptions().length should be (MapperV5.getStatisticsColumns().length)
  }

  test("get writable summary") {
    // not enough fields to collect statistics
    var summary = MapperV5.getSummaryWritable("", Array(NetflowV5.V5_FIELD_UNIX_SECS))
    summary should be (None)

    // empty array of fields
    summary = MapperV5.getSummaryWritable("", Array.empty)
    summary should be (None)

    summary = MapperV5.getSummaryWritable("", Array(NetflowV5.V5_FIELD_UNIX_SECS,
      NetflowV5.V5_FIELD_SRCADDR, NetflowV5.V5_FIELD_SRCPORT, NetflowV5.V5_FIELD_DSTADDR,
      NetflowV5.V5_FIELD_DSTPORT))

    summary.isEmpty should be (false)
    val stats = summary.get.finalizeStatistics()
    stats.getOptions().foreach(option => {
      option.getMin() should be (Long.MinValue)
      option.getMax() should be (Long.MaxValue)
    })

    // two summaries should be different instances
    val summary1 = MapperV5.getSummaryWritable("", Array(NetflowV5.V5_FIELD_UNIX_SECS,
      NetflowV5.V5_FIELD_SRCADDR, NetflowV5.V5_FIELD_SRCPORT, NetflowV5.V5_FIELD_DSTADDR,
      NetflowV5.V5_FIELD_DSTPORT, NetflowV5.V5_FIELD_PROT))

    val summary2 = MapperV5.getSummaryWritable("", Array(NetflowV5.V5_FIELD_UNIX_SECS,
      NetflowV5.V5_FIELD_SRCADDR, NetflowV5.V5_FIELD_SRCPORT, NetflowV5.V5_FIELD_DSTADDR,
      NetflowV5.V5_FIELD_DSTPORT))

    assert(!(summary1.get eq summary2.get))

    val opts1 = summary1.get.finalizeStatistics().getOptions()
    val opts2 = summary2.get.finalizeStatistics().getOptions()

    for (i <- 0 until opts1.length) {
      assert(!(opts1(i) eq opts2(i)))
    }
  }
}
