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

import java.io.IOException;
import java.nio.ByteOrder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.hadoop.fs.FSDataInputStream;

import com.github.sadikovi.netflowlib.version.NetFlowV5;
import com.github.sadikovi.netflowlib.version.NetFlowV7;

// Parsing NetFlow file
public class NetFlowReader {
  public static final short METADATA_LENGTH = 4;
  public static final short HEADER_OFFSET_LENGTH = 4;
  // header check flags
  private static final short HEADER_MAGIC1 = 0xCF;
  private static final short HEADER_MAGIC2 = 0x10; // cisco flow
  private static final short HEADER_LITTLE_ENDIAN = 1;
  private static final short HEADER_BIG_ENDIAN = 2;

  public NetFlowReader(FSDataInputStream in) throws IOException {
    this.in = in;
    readMetadata();
  }

  // Make sure that stream version is either version 1 or version 3
  private void ensureStreamVersion() throws UnsupportedOperationException {
    if (sversion != 1 && sversion != 3) {
      throw new UnsupportedOperationException("Unsupported stream version " + sversion);
    }
  }

  // Read and parse metadata of the stream
  private void readMetadata() throws IOException {
    byte[] metadata = new byte[METADATA_LENGTH];
    // make sure that we are at the beginning of the file
    this.in.seek(0);
    this.in.read(metadata, 0, METADATA_LENGTH);

    // parse metadata, byte order does not really matter, so we go for big endian
    ByteBuf buf = Unpooled.wrappedBuffer(metadata).order(ByteOrder.BIG_ENDIAN);
    short magic1 = buf.getUnsignedByte(0);
    short magic2 = buf.getUnsignedByte(1);
    short order = buf.getUnsignedByte(2);
    short stream = buf.getUnsignedByte(3);

    if (magic1 != HEADER_MAGIC1 || magic2 != HEADER_MAGIC2) {
      throw new UnsupportedOperationException("Corrupt NetFlow file. Wrong magic number");
    }

    if (order == HEADER_BIG_ENDIAN) {
      this.order = ByteOrder.BIG_ENDIAN;
    } else if (order == HEADER_LITTLE_ENDIAN) {
      this.order = ByteOrder.LITTLE_ENDIAN;
    } else {
      throw new UnsupportedOperationException("Could not recognize byte order " + order);
    }

    // assign stream version
    this.sversion = stream;

    // we do not need array and buffer afterwards
    metadata = null;
    buf.release();
    buf = null;
  }

  public NetFlowHeader readHeader() throws UnsupportedOperationException, IOException {
    ensureStreamVersion();

    NetFlowHeader header;
    int numBytesRead = 0;
    int lenRead = 0;
    ByteBuf buf;
    byte[] headerArray;
    // make sure that we are at right position in the stream
    if (this.in.getPos() != METADATA_LENGTH) {
      this.in.seek(METADATA_LENGTH);
    }

    // read header differently depending on stream version (not flow version)
    if (this.sversion == 1) {
      // version 1 has static header
      // TODO: verify header size for stream version 1
      lenRead = NetFlowHeader.S1_HEADER_SIZE - METADATA_LENGTH;
      header = new NetFlowHeader(this.sversion, this.order);
    } else {
      // v3 dynamic header size
      headerArray = new byte[HEADER_OFFSET_LENGTH];
      numBytesRead = this.in.read(headerArray, 0, HEADER_OFFSET_LENGTH);
      if (numBytesRead != HEADER_OFFSET_LENGTH) {
        throw new UnsupportedOperationException("Short read while loading header offset");
      }
      buf = Unpooled.wrappedBuffer(headerArray).order(this.order);
      int headerSize = (int)buf.getUnsignedInt(0);
      if (headerSize <= 0) {
        throw new UnsupportedOperationException("Failed to load header of size " + headerSize);
      }
      // actual header length, determine how many bytes to read
      lenRead = headerSize - METADATA_LENGTH - HEADER_OFFSET_LENGTH;
      header = new NetFlowHeader(this.sversion, this.order, headerSize);
    }

    // allocate buffer for length to read
    headerArray = new byte[lenRead];
    numBytesRead = this.in.read(headerArray, 0, lenRead);
    if (numBytesRead != lenRead) {
      throw new UnsupportedOperationException("Short read while loading header data");
    }
    // build buffer
    buf = Unpooled.wrappedBuffer(headerArray).order(this.order);

    // resolve stream version (either 1 or 3)
    if (this.sversion == 1) {
      // either 1, 3, 5, 7, 8, 10
      header.setFlowVersion((short)buf.getUnsignedShort(0));
      header.setStartCapture(buf.getUnsignedInt(2));
      header.setEndCapture(buf.getUnsignedInt(6));
      header.setHeaderFlags(buf.getUnsignedInt(10));
      header.setRotation(buf.getUnsignedInt(14));
      header.setNumFlows(buf.getUnsignedInt(18));
      header.setNumDropped(buf.getUnsignedInt(22));
      header.setNumMisordered(buf.getUnsignedInt(26));
      // read hostname
      byte[] hostnameBytes = new byte[NetFlowHeader.S1_HEADER_HN_LEN];
      buf.getBytes(30, hostnameBytes, 0, hostnameBytes.length);
      header.setHostname(new String(hostnameBytes));
      // read comments
      byte[] commentsBytes = new byte[NetFlowHeader.S1_HEADER_CMNT_LEN];
      buf.getBytes(30 + hostnameBytes.length, commentsBytes, 0, commentsBytes.length);
      header.setComments(new String(commentsBytes));

      // dereference arrays
      hostnameBytes = null;
      commentsBytes = null;
    } else {
      // resolve TLV (type-length value)
      // set decode pointer to first tlv
      int dp = 0;
      int left = lenRead;
      // smallest TLV is 2+2+0 (null TLV)
      // tlv_t - TLV type, tlv_l - TLV length, tlv_v - TLV value
      int tlv_t = 0;
      int tlv_l = 0;
      int tlv_v = 0;

      // byte array for holding Strings
      byte[] pr;

      while (left >= 4) {
        // parse type, store in host byte order
        tlv_t = buf.getUnsignedShort(dp);
        dp += 2;
        left -= 2;

        // parse len, store in host byte order
        tlv_l = buf.getUnsignedShort(dp);
        dp += 2;
        left -= 2;

        // parse val
        tlv_v = dp;

        // point decode buffer at next tlv
        dp += tlv_l;
        left -= tlv_l;

        // TLV length check
        if (left < 0) {
          break;
        }

        switch(tlv_t) {
          // FT_TLV_VENDOR
          case 0x1:
            header.setVendor(buf.getUnsignedShort(tlv_v));
            break;
          // FT_TLV_EX_VER
          case 0x2:
            header.setFlowVersion((short)buf.getUnsignedShort(tlv_v));
            break;
          // FT_TLV_AGG_VER
          case 0x3:
            header.setAggVersion(buf.getUnsignedByte(tlv_v));
            break;
          // FT_TLV_AGG_METHOD
          case 0x4:
            header.setAggMethod(buf.getUnsignedByte(tlv_v));
            break;
          // FT_TLV_EXPORTER_IP
          case 0x5:
            header.setExporterIP(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_CAP_START
          case 0x6:
            header.setStartCapture(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_CAP_END
          case 0x7:
            header.setEndCapture(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_HEADER_FLAGS
          case 0x8:
            header.setHeaderFlags(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_ROT_SCHEDULE
          case 0x9:
            header.setRotation(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_FLOW_COUNT
          case 0xA:
            header.setNumFlows(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_FLOW_LOST
          case 0xB:
            header.setNumDropped(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_FLOW_MISORDERED
          case 0xC:
            header.setNumMisordered(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_PKT_CORRUPT
          case 0xD:
            header.setNumCorrupt(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_SEQ_RESET
          case 0xE:
            header.setSeqReset(buf.getUnsignedInt(tlv_v));
            break;
          // FT_TLV_CAP_HOSTNAME
          case 0xF:
            pr = new byte[tlv_l];
            buf.getBytes(tlv_v, pr, 0, pr.length);
            // expected null-terminated string
            if (pr[pr.length - 1] != 0) {
              throw new UnsupportedOperationException("Char sequence is not null-terminated");
            }
            header.setHostname(new String(pr, 0, pr.length - 1));
            break;
          // FT_TLV_COMMENTS
          case 0x10:
            pr = new byte[tlv_l];
            buf.getBytes(tlv_v, pr, 0, pr.length);
            // expected null-terminated string
            if (pr[pr.length - 1] != 0) {
              throw new UnsupportedOperationException("Char sequence is not null-terminated");
            }
            header.setComments(new String(pr, 0, pr.length - 1));
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
            header.setInterfaceName(ip, ifIndex, new String(pr, 0, pr.length - 1));
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
            header.setInterfaceAlias(aliasIP, aliasIfIndexCnt, aliasIfIndex,
              new String(pr, 0, pr.length - 1));
            break;
          // case 0x0
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
    return header;
  }

  /**
   * Return buffer of Object records for specified fields. Note that values will have the same
   * order as fields in array.
   * @param NetFlowHeader header
   * @param askedFields fields wanted
   * @param bufferSize buffer size for the iterator
   * @return record buffer
   * @throws IOException, UnsupportedOperationException
   */
  public RecordBuffer readData(
      NetFlowHeader header,
      long[] askedFields,
      int bufferSize) throws IOException, UnsupportedOperationException {
    ensureStreamVersion();
    // place readerIndex to the beginning of the file's payload
    int expectedPosition = header.getHeaderSize();
    short flowVersion = header.getFlowVersion();
    this.in.seek(expectedPosition);

    // initialize record holder for a particular NetFlow version
    // in order to add new version create class in ".../version" and add another if
    // statement for that version
    FlowInterface flowInterface;
    if (flowVersion == 5) {
      flowInterface = new NetFlowV5(askedFields);
    } else if (flowVersion == 7) {
      flowInterface = new NetFlowV7(askedFields);
    } else {
      throw new UnsupportedOperationException("Unsupported flow version " + flowVersion);
    }

    boolean isCompressed = header.isCompressed();
    return new RecordBuffer(in, flowInterface, order, isCompressed, bufferSize);
  }

  ////////////////////////////////////////////////////////////
  // Public API
  ////////////////////////////////////////////////////////////
  
  public short getStreamVersion() {
    return this.sversion;
  }

  public ByteOrder getByteOrder() {
    return this.order;
  }

  // Stream of the NetFlow file
  private FSDataInputStream in = null;
  // byte order of the file
  private ByteOrder order = null;
  // stream version of the file
  private short sversion = -1;
}
