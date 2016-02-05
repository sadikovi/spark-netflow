package com.github.sadikovi.netflowlib.statistics;

public class StatisticsOption {

  public StatisticsOption(long field, short size, long min, long max) {
    this.field = field;
    this.size = validateSize(size);
    this.min = min;
    this.max = max;
  }

  /** Size validation, we support only 1, 2 or 4 byte fields */
  private short validateSize(short size) {
    return size;
  }

  public long getField() {
    return field;
  }

  public short getSize() {
    return size;
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  public long fullLength() {
    return 0L;
  }

  private long field = 0;
  private short size = 0;
  private long min = 0;
  private long max = 0;
}
