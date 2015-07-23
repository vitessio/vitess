package com.youtube.vitess.vtgate;

import com.google.common.primitives.UnsignedLong;
import org.joda.time.DateTime;

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
