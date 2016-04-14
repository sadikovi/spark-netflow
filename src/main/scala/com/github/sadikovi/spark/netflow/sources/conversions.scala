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

import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenContext

/**
 * [[ConvertFunction]] interface to provide direct `Any -> String` and reversed `String -> Any`
 * conversions.
 */
abstract class ConvertFunction {
  /** Direct conversion rule */
  def direct(value: Any): String

  /** Reversed conversion rule */
  def reversed(value: String): Any

  /**
   * Generated source code for direct function. Generated function should implement `eval()` from
   * [[DirectFunction]].
   */
  def gen(ctx: CodeGenContext): String
}

/** Conversion function for IPv4 values. */
case class IPv4ConvertFunction() extends ConvertFunction {
  override def direct(value: Any): String = {
    val num = value.asInstanceOf[Long]
    s"${(num & 4278190080L) >> 24}.${(num & 16711680L) >> 16}.${(num & 65280L) >> 8}.${num & 255}"
  }

  override def reversed(value: String): Any = {
    val arr = value.split('.').map(_.toLong)
    require(arr.length == 4, s"Invalid IPv4: ${value}")
    arr(0) << 24 | arr(1) << 16 | arr(2) << 8 | arr(3)
  }

  override def gen(ctx: CodeGenContext): String = {
    """
      long num = (java.lang.Long) r;
      return "" + ((num & 4278190080L) >> 24) + "." + ((num & 16711680L) >> 16) + "." +
        ((num & 65280L) >> 8) + "." + (num & 255);
    """
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

  // Note that for this function map is implemented as sequence if-else statements
  override def gen(ctx: CodeGenContext): String = {
    """
      short index = (java.lang.Short) r;
      if (index == 1) {
        // Internet Control Message Protocol
        return "ICMP";
      } else if (index == 3) {
        // Gateway-Gateway Protocol
        return "GGP";
      } else if (index == 6) {
        // Transmission Control Protocol
        return "TCP";
      } else if (index == 8) {
        // Exterior Gateway Protocol
        return "EGP";
      } else if (index == 12) {
        // PARC Universal Packet Protocol
        return "PUP";
      } else if (index == 17) {
        // User Datagram Protocol
        return "UDP";
      } else if (index == 20) {
        // Host Monitoring Protocol
        return "HMP";
      } else if (index == 27) {
        // Reliable Datagram Protocol
        return "RDP";
      } else if (index == 46) {
        // Reservation Protocol QoS
        return "RSVP";
      } else if (index == 47) {
        // General Routing Encapsulation
        return "GRE";
      } else if (index == 50) {
        // Encapsulation Security Payload IPSec
        return "ESP";
      } else if (index == 51) {
        // Authentication Header IPSec
        return "AH";
      } else if (index == 66) {
        // MIT Remote Virtual Disk
        return "RVD";
      } else if (index == 88) {
        // Internet Group Management Protocol
        return "IGMP";
      } else if (index == 89) {
        // Open Shortest Path First
        return "OSPF";
      } else {
        return "" + index;
      }
    """
  }
}
