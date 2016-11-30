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
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.containsString;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.github.sadikovi.netflowlib.Buffers.RecordBuffer;
import com.github.sadikovi.netflowlib.Buffers.EmptyRecordBuffer;
import com.github.sadikovi.netflowlib.Buffers.FilterRecordBuffer;
import com.github.sadikovi.netflowlib.Buffers.ScanRecordBuffer;

import com.github.sadikovi.netflowlib.NetFlowHeader;
import com.github.sadikovi.netflowlib.NetFlowReader;
import com.github.sadikovi.netflowlib.version.NetFlowV5;
import com.github.sadikovi.netflowlib.predicate.Columns.Column;
import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.LongColumn;
import com.github.sadikovi.netflowlib.predicate.FilterApi;
import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;
import com.github.sadikovi.netflowlib.statistics.Statistics;

public class NetFlowReaderSuite {
  private FSDataInputStream getTestStream(String file) throws IOException {
    Configuration conf = new Configuration(false);
    Path path = new Path(file);
    FileSystem fs = path.getFileSystem(conf);
    return fs.open(path);
  }

  //////////////////////////////////////////////////////////////
  // Reading different streams
  //////////////////////////////////////////////////////////////

  // test reader on corrupt input
  @Test
  public void testReadingCorrupt() throws IOException {
    String file = getClass().getResource("/corrupt/ftv5.2016-01-13.compress.9.sample-01").getPath();
    FSDataInputStream stm = getTestStream(file);

    try {
      NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    } catch (UnsupportedOperationException uoe) {
      assertThat(uoe.getMessage(), containsString("Corrupt NetFlow file. Wrong magic number"));
    }
  }

  @Test
  public void testReadingCompressed() throws IOException {
    String file = getClass().
      getResource("/correct/ftv7.2016-02-14.compress.9.litend.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    NetFlowHeader header = nr.getHeader();

    assertEquals(header.getFlowVersion(), (short) 7);
    assertEquals(header.isCompressed(), true);
    assertEquals(header.getStartCapture(), 0L);
    assertEquals(header.getEndCapture(), 0L);
    assertEquals(header.getComments(), "flow-gen");
    assertEquals(header.getByteOrder(), ByteOrder.LITTLE_ENDIAN);

    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR};
    RecordBuffer rb = nr.prepareRecordBuffer(cols);
    assertEquals(rb.iterator().hasNext(), true);
  }

  @Test
  public void testReadingUncompressed() throws IOException {
    String file = getClass().
      getResource("/correct/ftv5.2016-01-13.nocompress.bigend.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    NetFlowHeader header = nr.getHeader();

    assertEquals(header.getFlowVersion(), (short) 5);
    assertEquals(header.isCompressed(), false);
    assertEquals(header.getStartCapture(), 0L);
    assertEquals(header.getEndCapture(), 0L);
    assertEquals(header.getComments(), "flow-gen");
    assertEquals(header.getByteOrder(), ByteOrder.BIG_ENDIAN);

    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR};
    RecordBuffer rb = nr.prepareRecordBuffer(cols);
    assertEquals(rb.iterator().hasNext(), true);
  }

  @Test
  public void testReadingUncompressedEmpty() throws IOException {
    String file = getClass().
      getResource("/anomaly/ftv5.2016-03-15.nocompress.bigend.empty").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    NetFlowHeader header = nr.getHeader();

    assertEquals(header.getFlowVersion(), (short) 5);
    assertEquals(header.isCompressed(), false);
    assertEquals(header.getStartCapture(), 0L);
    assertEquals(header.getEndCapture(), 0L);
    assertEquals(header.getComments(), "flow-gen");
    assertEquals(header.getByteOrder(), ByteOrder.BIG_ENDIAN);

    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR};
    RecordBuffer rb = nr.prepareRecordBuffer(cols);
    assertEquals(rb.iterator().hasNext(), false);
  }

  @Test
  public void testReadingCompressedEmpty() throws IOException {
    String file = getClass().
      getResource("/anomaly/ftv5.2016-03-15.compress9.bigend.empty").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    NetFlowHeader header = nr.getHeader();

    assertEquals(header.getFlowVersion(), (short) 5);
    assertEquals(header.isCompressed(), true);
    assertEquals(header.getStartCapture(), 0L);
    assertEquals(header.getEndCapture(), 0L);
    assertEquals(header.getComments(), "flow-gen");
    assertEquals(header.getByteOrder(), ByteOrder.BIG_ENDIAN);

    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR};
    RecordBuffer rb = nr.prepareRecordBuffer(cols);
    assertEquals(rb.iterator().hasNext(), false);
  }

  //////////////////////////////////////////////////////////////
  // Initialization of reader
  //////////////////////////////////////////////////////////////

  @Test
  public void testInitializeStreamWithCustomSize() throws IOException {
    String file = getClass().
      getResource("/anomaly/ftv5.2016-03-15.compress9.bigend.empty").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    assertEquals(nr.getBufferLength(), 30000);
  }

  @Test
  public void testInitializeStreamWithDefaultSize() throws IOException {
    String file = getClass().
      getResource("/anomaly/ftv5.2016-03-15.compress9.bigend.empty").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm);
    assertEquals(nr.getBufferLength(), RecordBuffer.BUFFER_LENGTH_2);
  }

  //////////////////////////////////////////////////////////////
  // Testing different scan strategies
  //////////////////////////////////////////////////////////////

  @Test
  public void testStrategySkip() throws IOException {
    String file = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR, NetFlowV5.FIELD_SRCPORT};
    FilterPredicate filter = FilterApi.eq(NetFlowV5.FIELD_SRCADDR, null);
    RecordBuffer rb = nr.prepareRecordBuffer(cols, filter);

    assertEquals(rb.getClass(), EmptyRecordBuffer.class);
  }

  @Test
  public void testStrategyFull() throws IOException {
    String file = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR, NetFlowV5.FIELD_SRCPORT};
    RecordBuffer rb = nr.prepareRecordBuffer(cols);

    assertEquals(rb.getClass(), ScanRecordBuffer.class);
  }

  @Test
  public void testFilterScan1() throws IOException {
    // test fairly simple predicate
    String file = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR, NetFlowV5.FIELD_SRCPORT};
    FilterPredicate filter = FilterApi.and(
      FilterApi.ge(NetFlowV5.FIELD_SRCADDR, 11L),
      FilterApi.le(NetFlowV5.FIELD_SRCADDR, 25L));
    RecordBuffer rb = nr.prepareRecordBuffer(cols, filter);
    Iterator<Object[]> iter = rb.iterator();
    int numRecordsScanned = 0;

    while (iter.hasNext()) {
      numRecordsScanned++;
      iter.next();
    }

    assertEquals(rb.getClass(), FilterRecordBuffer.class);
    assertEquals(numRecordsScanned, 25 - 11 + 1);
  }

  @Test
  public void testFilterScan2() throws IOException {
    // filter results in full scan
    String file = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR, NetFlowV5.FIELD_SRCPORT};
    FilterPredicate filter = FilterApi.or(
      FilterApi.ge(NetFlowV5.FIELD_SRCADDR, 11L),
      FilterApi.gt(NetFlowV5.FIELD_SRCPORT, -1));
    RecordBuffer rb = nr.prepareRecordBuffer(cols, filter);

    assertEquals(rb.getClass(), ScanRecordBuffer.class);
  }

  @Test
  public void testFilterScan3() throws IOException {
    // filter results in skip scan
    String file = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR, NetFlowV5.FIELD_SRCPORT};
    FilterPredicate filter = FilterApi.and(
      FilterApi.ge(NetFlowV5.FIELD_SRCADDR, 11L),
      FilterApi.gt(NetFlowV5.FIELD_SRCPORT, null));
    RecordBuffer rb = nr.prepareRecordBuffer(cols, filter);

    assertEquals(rb.getClass(), EmptyRecordBuffer.class);
  }

  @Test
  public void testFilterScan4() throws IOException {
    // test complex filter
    String file = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR, NetFlowV5.FIELD_SRCPORT};
    FilterPredicate filter = FilterApi.and(
      FilterApi.and(
        FilterApi.ge(NetFlowV5.FIELD_SRCADDR, 11L),
        FilterApi.le(NetFlowV5.FIELD_SRCADDR, 25L)
      ),
      FilterApi.or(
        FilterApi.or(
          FilterApi.eq(NetFlowV5.FIELD_DSTPORT, null),
          FilterApi.not(
            FilterApi.gt(NetFlowV5.FIELD_UNIX_SECS, 10L)
          )
        ),
        FilterApi.gt(NetFlowV5.FIELD_DSTPORT, -1)
      )
    );
    RecordBuffer rb = nr.prepareRecordBuffer(cols, filter);
    Iterator<Object[]> iter = rb.iterator();
    int numRecordsScanned = 0;

    while (iter.hasNext()) {
      numRecordsScanned++;
      iter.next();
    }

    assertEquals(rb.getClass(), FilterRecordBuffer.class);
    assertEquals(numRecordsScanned, 25 - 11 + 1);
  }

  @Test
  public void testFilterScan5() throws IOException {
    // test filter with statistics on time
    String file = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR, NetFlowV5.FIELD_SRCPORT};
    FilterPredicate filter = FilterApi.and(
      FilterApi.ge(NetFlowV5.FIELD_UNIX_SECS, -1L),
      FilterApi.le(NetFlowV5.FIELD_UNIX_SECS, 1L)
    );
    RecordBuffer rb = nr.prepareRecordBuffer(cols, filter);

    assertEquals(rb.getClass(), ScanRecordBuffer.class);
  }

  @Test
  public void testFilterScan6() throws IOException {
    // test skip scan filter with statistics on time
    String file = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR, NetFlowV5.FIELD_SRCPORT};
    FilterPredicate filter = FilterApi.and(
      FilterApi.ge(NetFlowV5.FIELD_UNIX_SECS, 2L),
      FilterApi.le(NetFlowV5.FIELD_UNIX_SECS, 5L)
    );
    RecordBuffer rb = nr.prepareRecordBuffer(cols, filter);

    assertEquals(rb.getClass(), EmptyRecordBuffer.class);
  }

  @Test
  public void testFilterScan7() throws IOException {
    // test filter scan with statistics on time
    String file = getClass().getResource("/correct/ftv5.2016-01-13.compress.9.sample").getPath();
    FSDataInputStream stm = getTestStream(file);

    NetFlowReader nr = NetFlowReader.prepareReader(stm, 30000);
    Column[] cols = new Column[]{NetFlowV5.FIELD_SRCADDR, NetFlowV5.FIELD_SRCPORT,
      NetFlowV5.FIELD_DSTPORT};
    FilterPredicate filter = FilterApi.or(
      FilterApi.and(
        FilterApi.ge(NetFlowV5.FIELD_UNIX_SECS, -1L),
        FilterApi.lt(NetFlowV5.FIELD_SRCPORT, 5)
      ),
      FilterApi.eq(NetFlowV5.FIELD_DSTPORT, 200)
    );

    RecordBuffer rb = nr.prepareRecordBuffer(cols, filter);

    assertEquals(rb.getClass(), FilterRecordBuffer.class);

    Iterator<Object[]> iter = rb.iterator();
    Object[][] records = new Object[6][3];
    int i = 0;
    while (iter.hasNext()) {
      records[i++] = iter.next();
    }

    Object[][] expected = new Object[][] {
      {0L, 0, 65280},
      {1L, 1, 65281},
      {2L, 2, 65282},
      {3L, 3, 65283},
      {4L, 4, 65284},
      {456L, 456, 200}
    };

    assertArrayEquals(records, expected);
  }
}
