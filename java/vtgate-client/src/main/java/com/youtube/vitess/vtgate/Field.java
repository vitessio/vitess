package com.youtube.vitess.vtgate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.Row.Cell;

import org.apache.commons.lang.CharEncoding;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

public class Field {
  /**
   * MySQL field flags bitset values e.g. to distinguish between signed and unsigned integer.
   * Comments are taken from the original source code.
   * These numbers should exactly match values defined in dist/mysql-5.1.52/include/mysql_com.h
   */
  public enum Flag {
    // VT_ZEROVALUE_FLAG is not part of the MySQL specification and only used in unit tests.
    VT_ZEROVALUE_FLAG(0),
    VT_NOT_NULL_FLAG (1),     /* Field can't be NULL */
    VT_PRI_KEY_FLAG(2),       /* Field is part of a primary key */
    VT_UNIQUE_KEY_FLAG(4),        /* Field is part of a unique key */
    VT_MULTIPLE_KEY_FLAG(8),      /* Field is part of a key */
    VT_BLOB_FLAG(16),     /* Field is a blob */
    VT_UNSIGNED_FLAG(32),     /* Field is unsigned */
    VT_ZEROFILL_FLAG(64),     /* Field is zerofill */
    VT_BINARY_FLAG(128),      /* Field is binary   */
      /* The following are only sent to new clients */
    VT_ENUM_FLAG(256),        /* field is an enum */
    VT_AUTO_INCREMENT_FLAG(512),      /* field is a autoincrement field */
    VT_TIMESTAMP_FLAG(1024),      /* Field is a timestamp */
    VT_SET_FLAG(2048),        /* field is a set */
    VT_NO_DEFAULT_VALUE_FLAG(4096),   /* Field doesn't have default value */
    VT_ON_UPDATE_NOW_FLAG(8192),         /* Field is set to NOW on UPDATE */
    VT_NUM_FLAG(32768);       /* Field is num (for clients) */
    
    public long mysqlFlag;
    
    Flag(long mysqlFlag) {
      this.mysqlFlag = mysqlFlag;
    }
  }

  /**
   * Represents all field types supported by Vitess and their corresponding types in Java. mysqlType
   * numbers should exactly match values defined in dist/mysql-5.1.52/include/mysql/mysql_com.h
   *
   */
  enum FieldType {
    VT_DECIMAL(0, BigDecimal.class),
    VT_TINY(1, Integer.class),
    VT_SHORT(2, Integer.class),
    VT_LONG(3, Long.class),
    VT_FLOAT(4, Float.class),
    VT_DOUBLE(5, Double.class),
    VT_NULL(6, null),
    VT_TIMESTAMP(7, DateTime.class),
    VT_LONGLONG(8, Long.class, UnsignedLong.class),
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

    public final int mysqlType;
    public final Class javaType;
    public final Class unsignedJavaType;

    FieldType(int mysqlType, Class javaType) {
      this.mysqlType = mysqlType;
      this.javaType = javaType;
      this.unsignedJavaType = javaType;
    }

    FieldType(int mysqlType, Class javaType, Class unsignedJavaType) {
      this.mysqlType = mysqlType;
      this.javaType = javaType;
      this.unsignedJavaType = unsignedJavaType;
    }
  }
  
  private String name;
  private FieldType type;
  private long mysqlFlags;
  
  public static Field newFieldFromMysql(String name, int mysqlTypeId, long mysqlFlags) {
    for (FieldType ft : FieldType.values()) {
      if (ft.mysqlType == mysqlTypeId) {
        return new Field(name, ft, mysqlFlags);
      }
    }
    
    throw new RuntimeException("Unknown MySQL type: " + mysqlTypeId);
  }

  @VisibleForTesting
  static Field newFieldForTest(FieldType fieldType, Flag flag) {
    return new Field("dummyField", fieldType, flag.mysqlFlag);
  }

  private Field(String name, FieldType type, long mysqlFlags) {
    this.name = name;
    this.type = type;
    this.mysqlFlags = mysqlFlags;
  }

  public Cell convertValueToCell(byte[] bytes) {
    if ((mysqlFlags & Flag.VT_UNSIGNED_FLAG.mysqlFlag) != 0) {
      return new Cell(name, convert(bytes), type.unsignedJavaType);
    } else {
      return new Cell(name, convert(bytes), type.javaType);
    }
  }

  Object convert(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    String s = null;
    try {
      s = new String(bytes, CharEncoding.ISO_8859_1);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

    switch (type) {
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
        if ((mysqlFlags & Flag.VT_UNSIGNED_FLAG.mysqlFlag) != 0) {
          return UnsignedLong.valueOf(s);
        } else {
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
