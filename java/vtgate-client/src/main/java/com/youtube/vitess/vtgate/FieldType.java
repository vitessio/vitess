package com.youtube.vitess.vtgate;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import com.google.common.primitives.UnsignedLong;

/**
 * Represents all field types supported by Vitess and their corresponding types
 * in Java. mysqlType numbers should exactly match values defined in
 * dist/mysql-5.1.52/include/mysql/mysql_com.h
 * 
 */
public enum FieldType {
	VT_DECIMAL(0, Double.class),
	VT_TINY(1, Integer.class),
	VT_SHORT(2, Integer.class),
	VT_LONG(3, Long.class),
	VT_FLOAT(4, Float.class),
	VT_DOUBLE(5, Double.class),
	VT_NULL(6, String.class),
	VT_TIMESTAMP(7, Date.class),
	VT_LONGLONG(8, UnsignedLong.class),
	VT_INT24(9, Integer.class),
	VT_DATE(10, Date.class),
	VT_TIME(11, Date.class),
	VT_DATETIME(12, Date.class),
	VT_YEAR(13, String.class),
	VT_NEWDATE(14, String.class),
	VT_VARCHAR(15, String.class),
	VT_BIT(16, String.class),
	VT_NEWDECIMAL(246, Double.class),
	VT_ENUM(247, String.class),
	VT_SET(248, String.class),
	VT_TINY_BLOB(249, String.class),
	VT_MEDIUM_BLOB(250, String.class),
	VT_LONG_BLOB(251, String.class),
	VT_BLOB(252, String.class),
	VT_VAR_STRING(253, byte[].class),
	VT_STRING(254, String.class),
	VT_GEOMETRY(255, String.class);

	int mysqlType;
	Class javaType;

	FieldType(int mysqlType, Class javaType) {
		this.mysqlType = mysqlType;
		this.javaType = javaType;
	}

	static FieldType get(int mysqlTypeId) {
		for (FieldType ft : FieldType.values()) {
			if (ft.mysqlType == mysqlTypeId) {
				return ft;
			}
		}
		return null;
	}

	Object convertBytes(byte[] bytes) {
		if (bytes == null) {
			return null;
		}

		switch (this) {
		case VT_VAR_STRING:
			return bytes;
		default:
			return convert(new String(bytes));
		}
	}

	/**
	 * VtTablet returns all field values as Strings. convert constructs Java
	 * types corresponding to VT types
	 */
	Object convert(String s) {
		if (s == null) {
			return null;
		}

		switch (this) {
		case VT_DECIMAL:
		case VT_DOUBLE:
		case VT_NEWDECIMAL:
			return Double.valueOf(s);
		case VT_TINY:
		case VT_SHORT:
		case VT_INT24:
			return Integer.valueOf(s);
		case VT_LONG:
			return Long.valueOf(s);
		case VT_FLOAT:
			return Float.valueOf(s);
		case VT_LONGLONG:
			return UnsignedLong.valueOf(s);
		case VT_DATETIME:
		case VT_TIMESTAMP:
			return new Date(Timestamp.valueOf(s).getTime());
		case VT_DATE:
			return new Date(java.sql.Date.valueOf(s).getTime());
		case VT_TIME:
			return new Date(Time.valueOf(s).getTime());
		default:
			return s;
		}
	}
}
