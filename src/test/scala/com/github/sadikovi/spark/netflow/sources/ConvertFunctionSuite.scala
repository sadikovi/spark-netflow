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

import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.testutil.UnitTestSuite

class ConvertFunctionSuite extends UnitTestSuite {
  test("ip conversion") {
    val dataset = Seq(
      ("127.0.0.1", 2130706433L),
      ("172.71.4.54", 2890335286L),
      ("147.10.8.41", 2466908201L),
      ("10.208.97.205", 181428685L),
      ("144.136.17.61", 2424836413L),
      ("139.168.155.28", 2343082780L),
      ("172.49.10.53", 2888895029L),
      ("139.168.51.129", 2343056257L),
      ("10.152.185.135", 177781127L),
      ("144.131.33.125", 2424512893L),
      ("138.217.81.41", 2329497897L),
      ("147.10.7.77", 2466907981L),
      ("10.164.0.185", 178520249L),
      ("144.136.28.121", 2424839289L),
      ("172.117.8.117", 2893351029L),
      ("139.168.164.113", 2343085169L),
      ("147.132.87.29", 2474923805L),
      ("10.111.3.73", 175047497L),
      ("255.255.255.255", (2L<<31) - 1)
    )

    val convertFunction = IPv4ConvertFunction()

    // test direct conversion
    for (elem <- dataset) {
      val (ip, num) = elem
      convertFunction.direct(num) should equal (ip)
      convertFunction.directCatalyst(num) should equal (UTF8String.fromString(ip))
    }

    // test reversed conversion
    for (elem <- dataset) {
      val (ip, num) = elem
      convertFunction.reversed(ip) should equal (num)
    }
  }

  test("fail ip conversion for invalid input") {
    val convertFunction = IPv4ConvertFunction()
    var err = intercept[IllegalArgumentException] {
      convertFunction.reversed("123")
    }
    assert(err.getMessage.contains("Invalid IPv4: 123"))

    err = intercept[IllegalArgumentException] {
      convertFunction.reversed("1.2.3")
    }
    assert(err.getMessage.contains("Invalid IPv4: 1.2.3"))
  }

  test("protocol conversion") {
    val protocols: Array[Short] = (0 until 256).map(_.toShort).toArray

    val convertFunction = ProtocolConvertFunction()

    // test direct conversion
    for (num <- protocols) {
      val protocol = convertFunction.direct(num)
      if (!convertFunction.reversedProtocolMap.contains(protocol)) {
        protocol should be (num.toString())
      }
    }

    // test direct conversion for all indices of protocol
    convertFunction.direct(1.toShort) should be ("ICMP")
    convertFunction.directCatalyst(1.toShort) should be (UTF8String.fromString("ICMP"))

    convertFunction.direct(3.toShort) should be ("GGP")
    convertFunction.directCatalyst(3.toShort) should be (UTF8String.fromString("GGP"))

    convertFunction.direct(6.toShort) should be ("TCP")
    convertFunction.directCatalyst(6.toShort) should be (UTF8String.fromString("TCP"))

    convertFunction.direct(8.toShort) should be ("EGP")
    convertFunction.directCatalyst(8.toShort) should be (UTF8String.fromString("EGP"))

    convertFunction.direct(12.toShort) should be ("PUP")
    convertFunction.directCatalyst(12.toShort) should be (UTF8String.fromString("PUP"))

    convertFunction.direct(17.toShort) should be ("UDP")
    convertFunction.directCatalyst(17.toShort) should be (UTF8String.fromString("UDP"))

    convertFunction.direct(20.toShort) should be ("HMP")
    convertFunction.directCatalyst(20.toShort) should be (UTF8String.fromString("HMP"))

    convertFunction.direct(27.toShort) should be ("RDP")
    convertFunction.directCatalyst(27.toShort) should be (UTF8String.fromString("RDP"))

    convertFunction.direct(46.toShort) should be ("RSVP")
    convertFunction.directCatalyst(46.toShort) should be (UTF8String.fromString("RSVP"))

    convertFunction.direct(47.toShort) should be ("GRE")
    convertFunction.directCatalyst(47.toShort) should be (UTF8String.fromString("GRE"))

    convertFunction.direct(50.toShort) should be ("ESP")
    convertFunction.directCatalyst(50.toShort) should be (UTF8String.fromString("ESP"))

    convertFunction.direct(51.toShort) should be ("AH")
    convertFunction.directCatalyst(51.toShort) should be (UTF8String.fromString("AH"))

    convertFunction.direct(66.toShort) should be ("RVD")
    convertFunction.directCatalyst(66.toShort) should be (UTF8String.fromString("RVD"))

    convertFunction.direct(88.toShort) should be ("IGMP")
    convertFunction.directCatalyst(88.toShort) should be (UTF8String.fromString("IGMP"))

    convertFunction.direct(89.toShort) should be ("OSPF")
    convertFunction.directCatalyst(89.toShort) should be (UTF8String.fromString("OSPF"))

    // test reversed conversion
    convertFunction.reversed("ICMP") should be (1)
    convertFunction.reversed("TCP") should be (6)
    convertFunction.reversed("UDP") should be (17)
    convertFunction.reversed("255") should be (255)
  }

  test("fail protocol conversion, if value is invalid") {
    val convertFunction = ProtocolConvertFunction()
    intercept[RuntimeException] {
      convertFunction.reversed("udp")
    }
  }
}
