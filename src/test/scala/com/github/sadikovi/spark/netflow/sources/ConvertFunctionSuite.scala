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

import scala.collection.JavaConverters._

import com.github.sadikovi.spark.netflow.codegen.GenerateIterator
import com.github.sadikovi.testutil.UnitTestSpec

class ConvertFunctionSuite extends UnitTestSpec {
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

    val convertFunction = IPConvertFunction()

    // test direct conversion
    for (elem <- dataset) {
      val (ip, num) = elem
      convertFunction.direct(num) should equal (ip)
    }

    // test reversed conversion
    for (elem <- dataset) {
      val (ip, num) = elem
      convertFunction.reversed(ip) should equal (num)
    }

    // test codegen version
    val arr: Array[(ConvertFunction, Array[Int])] = Array(
      (convertFunction, Array(0))
    )
    val iterFunc = GenerateIterator.generate(arr)
    // apply direct conversion on second element of tuple {ip, num}
    val mappedDataIterator = dataset.map(x => Array(x._2.asInstanceOf[Object])).toIterator
    val resIter = iterFunc(mappedDataIterator.asJava).asScala
    resIter.toArray.flatten should be (dataset.map(_._1))
  }

  test("protocol conversion") {
    val protocols: Array[Short] = (0 until 256).map(_.toShort).toArray

    val convertFunction = ProtocolConvertFunction()

    // test direct conversion
    for (num <- protocols) {
      val protocol = convertFunction.direct(num)
      if (!convertFunction.reversedProtocolMap.contains(protocol)) {
        protocol should be (num.toString())
      } else {
        convertFunction.reversed(protocol) should be (num)
      }
    }

    // test reversed conversion
    convertFunction.reversed("ICMP") should be (1)
    convertFunction.reversed("TCP") should be (6)
    convertFunction.reversed("UDP") should be (17)
    convertFunction.reversed("255") should be (255)
    intercept[RuntimeException] {
      convertFunction.reversed("udp")
    }

    // test codegen version
    val arr: Array[(ConvertFunction, Array[Int])] = Array(
      (convertFunction, Array(0))
    )
    val iterFunc = GenerateIterator.generate(arr)
    // apply direct conversion on second element of tuple {ip, num}
    val mappedDataIterator = protocols.map(x => Array(x.asInstanceOf[Object])).toIterator
    val resIter = iterFunc(mappedDataIterator.asJava).asScala
    val results = resIter.toArray.flatten
    results(1) should be ("ICMP")
    results(3) should be ("GGP")
    results(6) should be ("TCP")
    results(8) should be ("EGP")
    results(12) should be ("PUP")
    results(17) should be ("UDP")
    results(20) should be ("HMP")
    results(27) should be ("RDP")
    results(46) should be ("RSVP")
    results(47) should be ("GRE")
    results(50) should be ("ESP")
    results(51) should be ("AH")
    results(66) should be ("RVD")
    results(88) should be ("IGMP")
    results(89) should be ("OSPF")
  }
}
