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

import scala.collection.mutable.HashMap
import scala.util.Try

import org.apache.spark.unsafe.types.UTF8String

/**
 * [[ConvertFunction]] interface to provide direct `Any -> String` and reversed `String -> Any`
 * conversions.
 */
abstract class ConvertFunction {
  /** Direct conversion rule */
  def direct(value: Any): String

  /** Direct conversion for Catalyst internal type */
  def directCatalyst(value: Any): UTF8String = {
    UTF8String.fromString(direct(value))
  }

  /** Reversed conversion rule */
  def reversed(value: String): Any
}

/** Conversion function for IPv4 values. */
case class IPv4ConvertFunction() extends ConvertFunction {
  override def direct(value: Any): String = {
    val num = value.asInstanceOf[Long]
    val sb = new StringBuilder()
    sb.append((num & 4278190080L) >> 24)
    sb.append('.')
    sb.append((num & 16711680) >> 16)
    sb.append('.')
    sb.append((num & 65280) >> 8)
    sb.append('.')
    sb.append(num & 255)
    sb.toString
  }

  private def store(bytes: Array[Byte], i: Int, value: Long): Int = {
    var j = i
    if (value >= 100) {
      bytes(j) = ('0' + value / 100).toByte
      j += 1
      bytes(j) = ('0' + ((value % 100) / 10)).toByte
      j += 1
      bytes(j) = ('0' + (value % 10)).toByte
    } else if (value >= 10) {
      bytes(j) = ('0' + value / 10).toByte
      j += 1
      bytes(j) = ('0' + (value % 10)).toByte
    } else {
      bytes(j) = ('0' + value).toByte
    }
    j + 1
  }

  override def directCatalyst(value: Any): UTF8String = {
    val num = value.asInstanceOf[Long]
    val bytes = new Array[Byte](15)
    var i = store(bytes, 0, (num & 4278190080L) >> 24)
    bytes(i) = '.'
    i = store(bytes, i + 1, (num & 16711680) >> 16)
    bytes(i) = '.'
    i = store(bytes, i + 1, (num & 65280) >> 8)
    bytes(i) = '.'
    i = store(bytes, i + 1, num & 255)
    UTF8String.fromBytes(bytes, 0, i)
  }

  override def reversed(value: String): Any = {
    val arr = value.split('.').map(_.toLong)
    require(arr.length == 4, s"Invalid IPv4: ${value}")
    arr(0) << 24 | arr(1) << 16 | arr(2) << 8 | arr(3)
  }
}

/** Conversion function for protocol (most common services) */
case class ProtocolConvertFunction() extends ConvertFunction {
  private[sources] val protocolMap: HashMap[Short, String] = HashMap(
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

  private[sources] lazy val reversedProtocolMap = protocolMap.map { case (key, value) =>
    (value, key) }.toMap

  override def direct(value: Any): String = {
    protocolMap.getOrElse(value.asInstanceOf[Short], value.toString())
  }

  override def reversed(value: String): Any = {
    reversedProtocolMap.getOrElse(value, Try(value.toShort).getOrElse(
      sys.error(s"Failed to convert $value for ${getClass().getSimpleName()}")))
  }
}
