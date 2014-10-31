package com.youtube.vitess.vtgate;

import com.google.common.primitives.UnsignedLong;

import org.apache.commons.lang.CharEncoding;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

/**
 * Represents all field types supported by Vitess and their corresponding types in Java. mysqlType
 * numbers should exactly match values defined in dist/mysql-5.1.52/include/mysql/mysql_com.h
 *
 */
public enum FieldType {
  VT_DECIMAL(0, BigDecimal.class),
  VT_TINY(1, Integer.class),
  VT_SHORT(2, Integer.class),
  VT_LONG(3, Long.class),
  VT_FLOAT(4, Float.class),
  VT_DOUBLE(5, Double.class),
  VT_NULL(6, null),
  VT_TIMESTAMP(7, DateTime.class),
  VT_LONGLONG(8, UnsignedLong.class),
  VT_INT24(9, Integer.class),
  VT_DATE(10, DateTime.class),
  VT_TIME(11, DateTime.class),
  VT_DATETIME(12, DateTime.class),
  VT_YEAR(13, Short.class),
  VT_NEWDATE(14, DateTime.class),
  VT_VARCHAR(15, byte[].class),
  VT_BIT(16, byte[].class),
  VT_NEWDECIMAL(246, BigDecimal.class),
  VT_ENUM(247, String.class),
  VT_SET(248, String.class),
  VT_TINY_BLOB(249, byte[].class),
  VT_MEDIUM_BLOB(250, byte[].class),
  VT_LONG_BLOB(251, byte[].class),
  VT_BLOB(252, byte[].class),
  VT_VAR_STRING(253, byte[].class),
  VT_STRING(254, byte[].class),
  VT_GEOMETRY(255, byte[].class);

  public int mysqlType;
  public Class javaType;

  FieldType(int mysqlType, Class javaType) {
    this.mysqlType = mysqlType;
    this.javaType = javaType;
  }

  public static FieldType get(int mysqlTypeId) {
    for (FieldType ft : FieldType.values()) {
      if (ft.mysqlType == mysqlTypeId) {
        return ft;
      }
    }
    return null;
  }

  public Object convert(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    String s = null;
    try {
      s = new String(bytes, CharEncoding.ISO_8859_1);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

    switch (this) {
      case VT_DECIMAL:
        return new BigDecimal(s);
      case VT_TINY:
        return Integer.valueOf(s);
      case VT_SHORT:
        return Integer.valueOf(s);
      case VT_LONG:
        return Long.valueOf(s);
      case VT_FLOAT:
        return Float.valueOf(s);
      case VT_DOUBLE:
        return Double.valueOf(s);
      case VT_NULL:
        return null;
      case VT_TIMESTAMP:
        s = s.replace(' ', 'T');
        return DateTime.parse(s);
      case VT_LONGLONG:
        // This can be an unsigned or a signed long
        try {
          return UnsignedLong.valueOf(s);
        } catch (NumberFormatException e) {
          return Long.valueOf(s);
        }
      case VT_INT24:
        return Integer.valueOf(s);
      case VT_DATE:
        return DateTime.parse(s, ISODateTimeFormat.date());
      case VT_TIME:
        DateTime d = DateTime.parse(s, DateTimeFormat.forPattern("HH:mm:ss"));
        return d;
      case VT_DATETIME:
        s = s.replace(' ', 'T');
        return DateTime.parse(s);
      case VT_YEAR:
        return Short.valueOf(s);
      case VT_NEWDATE:
        return DateTime.parse(s, ISODateTimeFormat.date());
      case VT_VARCHAR:
        return bytes;
      case VT_BIT:
        return bytes;
      case VT_NEWDECIMAL:
        return new BigDecimal(s);
      case VT_ENUM:
        return s;
      case VT_SET:
        return s;
      case VT_TINY_BLOB:
        return bytes;
      case VT_MEDIUM_BLOB:
        return bytes;
      case VT_LONG_BLOB:
        return bytes;
      case VT_BLOB:
        return bytes;
      case VT_VAR_STRING:
        return bytes;
      case VT_STRING:
        return bytes;
      case VT_GEOMETRY:
        return bytes;
      default:
        throw new RuntimeException("invalid field type " + this);
    }
  }
}
