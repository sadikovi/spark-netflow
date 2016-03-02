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

package com.github.sadikovi.netflowlib;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.github.sadikovi.netflowlib.ScanPlanner;
import com.github.sadikovi.netflowlib.Strategies.ScanStrategy;

import com.github.sadikovi.netflowlib.Buffers.RecordBuffer;
import com.github.sadikovi.netflowlib.Buffers.EmptyRecordBuffer;
import com.github.sadikovi.netflowlib.Buffers.FilterRecordBuffer;
import com.github.sadikovi.netflowlib.Buffers.ScanRecordBuffer;

import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;

import com.github.sadikovi.netflowlib.statistics.Statistics;
import com.github.sadikovi.netflowlib.statistics.StatisticsTypes.LongStatistics;

import com.github.sadikovi.netflowlib.version.NetFlow;
import com.github.sadikovi.netflowlib.version.NetFlowV5;
import com.github.sadikovi.netflowlib.version.NetFlowV7;

/**
 * [[NetFlowReader]] is a main entry to process input stream of NetFlow file either from local
 * file system or HDFS. Provides API to retrieve header and other metadata before scanning records.
 * Uses statistics and planning based on [[ScanPlanner]] to decide whether or not the file needs
 * to be scanned.
 * [[FilterPredicate]] support is introduced to filter data on row basis.
 */
public final class NetFlowReader {
  // Internal byte offsets
  private static final short METADATA_LENGTH = 4;
  private static final short HEADER_OFFSET_LENGTH = 4;
  // Header check flags
  private static final short HEADER_MAGIC1 = 0xCF;
  private static final short HEADER_MAGIC2 = 0x10; // cisco flow
  // Byte order encoding
  private static final short HEADER_LITTLE_ENDIAN = 1;
  private static final short HEADER_BIG_ENDIAN = 2;

  public static NetFlowReader prepareReader(
      DataInputStream inputStream,
      int buffer) throws IOException {
    return new NetFlowReader(inputStream, buffer);
  }

  /**
   * [[NetFlowReader]] provides interface to get parsed header and record buffer with chosen
   * strategy based on columns, predicate and statistics. Metadata, header are parsed as part of
   * initialization.
   */
  private NetFlowReader(DataInputStream inputStream, int buffer) throws IOException {
    in = inputStream;
    bufferLength = buffer;

    byte[] metadata = new byte[METADATA_LENGTH];
    in.read(metadata, 0, METADATA_LENGTH);

    // Parse metadata, byte order does not really matter, so we go for big endian. Metadata contains
    // magic numbers to verify consistency of the NetFlow file, byte order encoded as either 1 or 2,
    // and stream version which affects header parsing (currently only 1 and 3 are supported).
    ByteBuf buf = Unpooled.wrappedBuffer(metadata).order(ByteOrder.BIG_ENDIAN);
    short magic1 = buf.getUnsignedByte(0);
    short magic2 = buf.getUnsignedByte(1);
    short order = buf.getUnsignedByte(2);
    short stream = buf.getUnsignedByte(3);

    // Verify consistency of NetFlow file, also this ensures that we are at the beginning of the
    // input stream
    if (magic1 != HEADER_MAGIC1 || magic2 != HEADER_MAGIC2) {
      throw new UnsupportedOperationException("Corrupt NetFlow file. Wrong magic number");
    }

    // Resolve byte order, last case corresponds to incorrect reading from buffer
    if (order == HEADER_BIG_ENDIAN) {
      byteOrder = ByteOrder.BIG_ENDIAN;
    } else if (order == HEADER_LITTLE_ENDIAN) {
      byteOrder = ByteOrder.LITTLE_ENDIAN;
    } else {
      throw new UnsupportedOperationException("Could not recognize byte order " + order);
    }

    streamVersion = stream;

    // Check stream version
    ensureStreamVersion();

    // Read header
    header = getHeader();

    metadata = null;
    buf.release();
    buf = null;
  }

  /** Ensure that stream version is either version 1 or version 3 */
  private void ensureStreamVersion() throws UnsupportedOperationException {
    if (streamVersion != 1 && streamVersion != 3) {
      throw new UnsupportedOperationException("Unsupported stream version " + streamVersion);
    }
  }

  /** Prepare header using provided input stream */
  private NetFlowHeader prepareHeader() throws IOException {
    NetFlowHeader internalHeader;
    int numBytesRead = 0;
    int lenRead = 0;
    ByteBuf buf;
    byte[] headerArray;

    // Read header depending on stream version (different from flow version)
    if (streamVersion == 1) {
      // Version 1 has static header
      // TODO: verify header size for stream version 1
      lenRead = NetFlowHeader.S1_HEADER_SIZE - METADATA_LENGTH;
      internalHeader = new NetFlowHeader(streamVersion, byteOrder);
    } else {
      // Version 3 with dynamic header size
      headerArray = new byte[HEADER_OFFSET_LENGTH];
      numBytesRead = in.read(headerArray, 0, HEADER_OFFSET_LENGTH);
      if (numBytesRead != HEADER_OFFSET_LENGTH) {
        throw new UnsupportedOperationException("Short read while loading header offset");
      }

      buf = Unpooled.wrappedBuffer(headerArray).order(byteOrder);
      int headerSize = (int)buf.getUnsignedInt(0);
      if (headerSize <= 0) {
        throw new UnsupportedOperationException("Failed to load header of size " + headerSize);
      }

      // Actual header length, determine how many bytes to read
      lenRead = headerSize - METADATA_LENGTH - HEADER_OFFSET_LENGTH;
      internalHeader = new NetFlowHeader(streamVersion, byteOrder, headerSize);
    }

    // allocate buffer for length to read
    headerArray = new byte[lenRead];
    numBytesRead = in.read(headerArray, 0, lenRead);
    if (numBytesRead != lenRead) {
      throw new UnsupportedOperationException("Short read while loading header data");
    }
    // build buffer
    buf = Unpooled.wrappedBuffer(headerArray).order(byteOrder);

    // resolve stream version (either 1 or 3)
    if (streamVersion == 1) {
      internalHeader.setFlowVersion((short)buf.getUnsignedShort(0));
      internalHeader.setStartCapture(buf.getUnsignedInt(2));
      internalHeader.setEndCapture(buf.getUnsignedInt(6));
      internalHeader.setHeaderFlags(buf.getUnsignedInt(10));
      internalHeader.setRotation(buf.getUnsignedInt(14));
      internalHeader.setNumFlows(buf.getUnsignedInt(18));
      internalHeader.setNumDropped(buf.getUnsignedInt(22));
      internalHeader.setNumMisordered(buf.getUnsignedInt(26));
      // Read hostname fixed bytes
      byte[] hostnameBytes = new byte[NetFlowHeader.S1_HEADER_HN_LEN];
      buf.getBytes(30, hostnameBytes, 0, hostnameBytes.length);
      internalHeader.setHostname(new String(hostnameBytes));
      // Read comments fixed bytes
      byte[] commentsBytes = new byte[NetFlowHeader.S1_HEADER_CMNT_LEN];
      buf.getBytes(30 + hostnameBytes.length, commentsBytes, 0, commentsBytes.length);
      internalHeader.setComments(new String(commentsBytes));

      // Dereference arrays
      hostnameBytes = null;
      commentsBytes = null;
    } else {
      // Resolve TLV (type-length value)
      // Set decode pointer to first tlv
      int dp = 0;
      int left = lenRead;
      // Smallest TLV is 2+2+0 (null TLV)
      // tlv_t - TLV type, tlv_l - TLV length, tlv_v - TLV value
      int tlv_t = 0;
      int tlv_l = 0;
      int tlv_v = 0;

      // Byte array for holding Strings
      byte[] pr;

      while (left >= 4) {
        // Parse type, store in host byte order
        tlv_t = buf.getUnsignedShort(dp);
        dp += 2;
        left -= 2;

        // Parse len, store in host byte order
        tlv_l = buf.getUnsignedShort(dp);
        dp += 2;
        left -= 2;

        // Parse val
        tlv_v = dp;

        // Point decode buffer at next tlv
        dp += tlv_l;
        left -= tlv_l;

        // TLV length check
        if (left < 0) {
          break;
        }

        switch(tlv_t) {
          // FT_TLV_VENDOR
          case 0x1:
            internalHeader.setVendor(buf.getUnsignedShort(tlv_v));
            break;
          // FT_TLV_EX_VER
          case 0x2:
            internalHeader.setFlowVersion((short)buf.getUnsignedShort(tlv_v));
            break;
          // FT_TLV_AGG_VER
          case 0x3:
            internalHeader.setAggVersion(buf.getUnsignedByte(tlv_v));
            break;
          // FT_TLV_AGG_METHOD
          case 0x4:
            internalHeader.setAggMethod(buf.getUnsignedByte(tlv_v));
            break;
          // FT_TLV_EXPORTER_IP
          case 0x5:
            internalHeader.setExporterIP(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_CAP_START
          case 0x6:
            internalHeader.setStartCapture(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_CAP_END
          case 0x7:
            internalHeader.setEndCapture(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_HEADER_FLAGS
          case 0x8:
            internalHeader.setHeaderFlags(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_ROT_SCHEDULE
          case 0x9:
            internalHeader.setRotation(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_FLOW_COUNT
          case 0xA:
            internalHeader.setNumFlows(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_FLOW_LOST
          case 0xB:
            internalHeader.setNumDropped(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_FLOW_MISORDERED
          case 0xC:
            internalHeader.setNumMisordered(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_PKT_CORRUPT
          case 0xD:
            internalHeader.setNumCorrupt(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_SEQ_RESET
          case 0xE:
            internalHeader.setSeqReset(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_CAP_HOSTNAME
          case 0xF:
            pr = new byte[tlv_l];
            buf.getBytes(tlv_v, pr, 0, pr.length);
            // Expected null-terminated string
            if (pr[pr.length - 1] != 0) {
              throw new UnsupportedOperationException("Char sequence is not null-terminated");
            }

            internalHeader.setHostname(new String(pr, 0, pr.length - 1));
            break;
          // FT_TLV_COMMENTS
          case 0x10:
            pr = new byte[tlv_l];
            buf.getBytes(tlv_v, pr, 0, pr.length);
            // Expected null-terminated string
            if (pr[pr.length - 1] != 0) {
              throw new UnsupportedOperationException("Char sequence is not null-terminated");
            }
            internalHeader.setComments(new String(pr, 0, pr.length - 1));
            break;
          // FT_TLV_IF_NAME
          case 0x11:
            // uint32_t, uint16_t, string:
            // - IP address of device
            // - ifIndex of interface
            // - interface name
            long ip = buf.getUnsignedInt(tlv_v);
            int ifIndex = buf.getUnsignedShort(tlv_v + 4);
            pr = new byte[tlv_l - 4 - 2];
            buf.getBytes(tlv_v + 4 + 2, pr, 0, pr.length);
            if (pr[pr.length - 1] != 0) {
              throw new UnsupportedOperationException("Char sequence is not null-terminated");
            }
            internalHeader.setInterfaceName(ip, ifIndex, new String(pr, 0, pr.length - 1));
            break;
          // FT_TLV_IF_ALIAS
          case 0x12:
            // uint32_t, uint16_t, uint16_t, string:
            // - IP address of device
            // - ifIndex count
            // - ifIndex of interface (count times)
            // - alias name
            long aliasIP = buf.getUnsignedInt(tlv_v);
            int aliasIfIndexCnt = buf.getUnsignedShort(tlv_v + 4);
            int aliasIfIndex = buf.getUnsignedShort(tlv_v + 4 + 2);
            pr = new byte[tlv_l - 4 - 2 - 2];
            buf.getBytes(tlv_v + 4 + 2 + 2, pr, 0, pr.length);
            if (pr[pr.length - 1] != 0) {
              throw new UnsupportedOperationException("Char sequence is not null-terminated");
            }

            internalHeader.setInterfaceAlias(aliasIP, aliasIfIndexCnt, aliasIfIndex,
              new String(pr, 0, pr.length - 1));
            break;
          // Case 0x0
          default:
            break;
        }
      }

      if (buf != null && buf.refCnt() > 0) {
        buf.release(buf.refCnt());
      }

      buf = null;
      pr = null;
    }
    return internalHeader;
  }

  /** Return NetFlow header for current input stream */
  public NetFlowHeader getHeader() throws IOException {
    if (header == null) {
      header = prepareHeader();
    }

    return header;
  }

  /** Prepare record buffer for full scan */
  public RecordBuffer prepareRecordBuffer(Column[] columns) {
    return prepareRecordBuffer(columns, null);
  }

  /** Prepare record buffer with default statistics on time */
  public RecordBuffer prepareRecordBuffer(Column[] columns, FilterPredicate predicate) {
    return prepareRecordBuffer(columns, predicate, null);
  }

  /** Prepare record buffer based on input stream */
  public RecordBuffer prepareRecordBuffer(
      Column[] columns,
      FilterPredicate predicate,
      HashMap<Column, Statistics> stats) {
    // Since we are using statistics on a field, we have to make sure that it is initialized
    // properly
    if (stats == null) {
      stats = new HashMap<Column, Statistics>();
    }

    // Find out appropriate strategy for set of columns and predicate. We also update statistics
    // with start and end capture time of the file.
    NetFlow flowInterface;
    if (header.getFlowVersion() == 5) {
      flowInterface = new NetFlowV5();
      stats.put(NetFlowV5.FIELD_UNIX_SECS,
        new LongStatistics(header.getStartCapture(), header.getEndCapture()));
    } else if (header.getFlowVersion() == 7) {
      flowInterface = new NetFlowV7();
      stats.put(NetFlowV7.FIELD_UNIX_SECS,
        new LongStatistics(header.getStartCapture(), header.getEndCapture()));
    } else {
      throw new UnsupportedOperationException("Version " + header.getFlowVersion() +
        " is not supported");
    }

    ScanStrategy strategy = ScanPlanner.buildStrategy(columns, predicate, stats);
    return prepareRecordBuffer(strategy, flowInterface);
  }

  // Prepare record buffer based on strategy and flow interface. Method is currently private, so
  // there is no option to pass custom scan strategy.
  private RecordBuffer prepareRecordBuffer(ScanStrategy strategy, NetFlow flowInterface) {
    if (strategy == null) {
      throw new IllegalArgumentException("Expected ScanStrategy instance, got null");
    }

    if (flowInterface == null) {
      throw new IllegalArgumentException("Expected NetFlow instance, got null");
    }

    // Depending on different strategy we either skip file directly, return full buffer or records,
    // or return filtering buffer, if there is a FilterScan.
    boolean isCompressed = header.isCompressed();
    int recordSize = flowInterface.recordSize();

    if (strategy.skipScan()) {
      return new EmptyRecordBuffer();
    } else if (strategy.fullScan()) {
      return new ScanRecordBuffer(in, strategy.getRecordMaterializer(), recordSize, byteOrder,
        isCompressed, bufferLength);
    } else {
      return new FilterRecordBuffer(in, strategy.getRecordMaterializer(), recordSize, byteOrder,
        isCompressed, bufferLength);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[byte order: " + byteOrder + ", stream version: " +
      streamVersion + ", buffer length: " + bufferLength + "]";
  }

  // Stream of the NetFlow file
  private final DataInputStream in;
  // Byte order of the file
  private final ByteOrder byteOrder;
  // Stream version of the file
  private final short streamVersion;
  // Buffer size for record buffer
  private final int bufferLength;
  // NetFlow header
  private NetFlowHeader header = null;
}
