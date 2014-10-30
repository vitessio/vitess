package com.youtube.vitess.vtgate;

import com.google.common.primitives.UnsignedLong;

import org.apache.commons.lang.CharEncoding;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

public class BindVariable {
  private String name;
  private Type type;
  private long longVal;
  private UnsignedLong uLongVal;
  private double doubleVal;
  private byte[] byteArrayVal;

  private BindVariable(String name, Type type) {
    this.name = name;
    this.type = type;
  }

  public static BindVariable forNull(String name) {
    return new BindVariable(name, Type.NULL);
  }

  public static BindVariable forInt(String name, Integer value) {
    return forLong(name, value.longValue());
  }

  public static BindVariable forLong(String name, Long value) {
    BindVariable bv = new BindVariable(name, Type.LONG);
    bv.longVal = value;
    return bv;
  }

  public static BindVariable forFloat(String name, Float value) {
    return forDouble(name, value.doubleValue());
  }

  public static BindVariable forDouble(String name, Double value) {
    BindVariable bv = new BindVariable(name, Type.DOUBLE);
    bv.doubleVal = value;
    return bv;
  }

  public static BindVariable forULong(String name, UnsignedLong value) {
    BindVariable bv = new BindVariable(name, Type.UNSIGNED_LONG);
    bv.uLongVal = value;
    return bv;
  }

  public static BindVariable forDateTime(String name, DateTime value) {
    String dateTimeStr =
        value.toString(ISODateTimeFormat.dateHourMinuteSecondMillis()).replace('T', ' ');
    return forString(name, dateTimeStr);
  }

  public static BindVariable forDate(String name, DateTime value) {
    String date = value.toString(ISODateTimeFormat.date());
    return forString(name, date);
  }

  public static BindVariable forTime(String name, DateTime value) {
    String time = value.toString(DateTimeFormat.forPattern("HH:mm:ss"));
    return forString(name, time);
  }

  public static BindVariable forString(String name, String value) {
    try {
      return forBytes(name, value.getBytes(CharEncoding.ISO_8859_1));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static BindVariable forBytes(String name, byte[] value) {
    BindVariable bv = new BindVariable(name, Type.BYTE_ARRAY);
    bv.byteArrayVal = value;
    return bv;
  }

  public static BindVariable forShort(String name, Short value) {
    return forLong(name, value.longValue());
  }

  public static BindVariable forBigDecimal(String name, BigDecimal value) {
    return forDouble(name, value.doubleValue());
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public long getLongVal() {
    return longVal;
  }

  public UnsignedLong getULongVal() {
    return uLongVal;
  }

  public double getDoubleVal() {
    return doubleVal;
  }

  public byte[] getByteArrayVal() {
    return byteArrayVal;
  }

  public static enum Type {
    NULL, LONG, UNSIGNED_LONG, DOUBLE, BYTE_ARRAY;
  }
}
