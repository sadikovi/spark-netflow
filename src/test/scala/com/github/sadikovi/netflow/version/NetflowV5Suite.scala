package com.github.sadikovi.netflow.version

import com.github.sadikovi.testutil.UnitTestSpec

class NetflowV5Suite extends UnitTestSpec {

  test("test initialize of NetflowV5") {
    intercept[UnsupportedOperationException] {
      val fields: Array[Long] = Array()
      val record = new NetflowV5(fields)
    }

    // throw exception for unknown field
    intercept[UnsupportedOperationException] {
      val fields: Array[Long] = Array(-1L)
      val record = new NetflowV5(fields)
    }
  }

  test("compute actual size of entire record") {
    val fields = Array(
      NetflowV5.V5_FIELD_UNIX_SECS,
      NetflowV5.V5_FIELD_UNIX_NSECS,
      NetflowV5.V5_FIELD_SYSUPTIME,
      NetflowV5.V5_FIELD_EXADDR,
      NetflowV5.V5_FIELD_SRCADDR,
      NetflowV5.V5_FIELD_DSTADDR,
      NetflowV5.V5_FIELD_NEXTHOP,
      NetflowV5.V5_FIELD_DPKTS,
      NetflowV5.V5_FIELD_DOCTETS,
      NetflowV5.V5_FIELD_FIRST,
      NetflowV5.V5_FIELD_LAST,
      NetflowV5.V5_FIELD_INPUT,
      NetflowV5.V5_FIELD_OUTPUT,
      NetflowV5.V5_FIELD_SRCPORT,
      NetflowV5.V5_FIELD_DSTPORT,
      NetflowV5.V5_FIELD_SRC_AS,
      NetflowV5.V5_FIELD_DST_AS,
      NetflowV5.V5_FIELD_PROT,
      NetflowV5.V5_FIELD_TOS,
      NetflowV5.V5_FIELD_TCP_FLAGS,
      NetflowV5.V5_FIELD_ENGINE_TYPE,
      NetflowV5.V5_FIELD_ENGINE_ID,
      NetflowV5.V5_FIELD_SRC_MASK,
      NetflowV5.V5_FIELD_DST_MASK
    )

    val record = new NetflowV5(fields)
    // we add one byte as we skip padding field, which is included into record size
    val actualSize = record.actualSize() + 1
    actualSize should be (record.size())
  }
}
