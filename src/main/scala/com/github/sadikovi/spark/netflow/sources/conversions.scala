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

import scala.util.Try

/**
 * [[ConvertFunction]] interface to provide direct `Any -> String` and reversed `String -> Any`
 * conversions.
 */
abstract class ConvertFunction {
  /** Direct conversion rule */
  def direct(value: Any): String

  /** Reversed conversion rule */
  def reversed(value: String): Any
}

/** Conversion function for IP values. */
case class IPConvertFunction() extends ConvertFunction {
  override def direct(value: Any): String = value match {
    case num: Long =>
      require(num >= 0 && num < (2L << 31), s"Invalid number to convert: ${num}")
      val buf = new StringBuilder()
      var i = 24
      var ip = num
      while (i >= 0) {
          val a = ip >> i
          ip = a << i ^ ip
          buf.append(a)
          if (i > 0) {
              buf.append(".")
          }
          i = i - 8
      }
      buf.toString()
    case _ =>
      value.toString()
  }

  override def reversed(value: String): Any = {
    val arr = value.split('.').map(_.toLong)
    require(arr.length == 4, s"Invalid IPv4: ${value}")
    arr(0) << 24 | arr(1) << 16 | arr(2) << 8 | arr(3)
  }
}

/** Conversion function for protocol (most common services) */
case class ProtocolConvertFunction() extends ConvertFunction {
  private[sources] val protocolMap: Map[Short, String] = Map(
    1.toShort -> "ICMP", // Internet Control Message Protocol
    3.toShort -> "GGP", // Gateway-Gateway Protocol
    6.toShort -> "TCP", // Transmission Control Protocol
    8.toShort -> "EGP", // Exterior Gateway Protocol
    12.toShort -> "PUP", // PARC Universal Packet Protocol
    17.toShort -> "UDP", // User Datagram Protocol
    20.toShort -> "HMP", // Host Monitoring Protocol
    27.toShort -> "RDP", // Reliable Datagram Protocol
    46.toShort -> "RSVP", // Reservation Protocol QoS
    47.toShort -> "GRE", // General Routing Encapsulation
    50.toShort -> "ESP", // Encapsulation Security Payload IPSec
    51.toShort -> "AH", // Authentication Header IPSec
    66.toShort -> "RVD", // MIT Remote Virtual Disk
    88.toShort -> "IGMP", // Internet Group Management Protocol
    89.toShort -> "OSPF" // Open Shortest Path First
  )

  private[sources] lazy val reversedProtocolMap = protocolMap.map{ case (key, value) =>
    (value, key) }.toMap

  override def direct(value: Any): String = value match {
    case num: Short => protocolMap.getOrElse(num, value.toString())
    case _ => value.toString()
  }

  override def reversed(value: String): Any = {
    reversedProtocolMap.getOrElse(value, Try(value.toShort).getOrElse(
      sys.error(s"Failed to convert ${value} for ${getClass().getSimpleName()}")))
  }
}
