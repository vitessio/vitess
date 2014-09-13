package com.youtube.vitess.vtgate;

import java.math.BigInteger;

/**
 * Represents all field types supported by Vitess and their corresponding types
 * in Java. mysqlType numbers should exactly match values defined in
 * dist/mysql-5.1.52/include/mysql/mysql_com.h
 * 
 * TODO: support date types correctly
 */
public enum FieldType {
	VT_DECIMAL(0, Double.class),
	VT_TINY(1, Integer.class),
	VT_SHORT(2, Integer.class),
	VT_LONG(3, Long.class),
	VT_FLOAT(4, Float.class),
	VT_DOUBLE(5, Double.class),
	VT_NULL(6, String.class),
	VT_TIMESTAMP(7, String.class),
	VT_LONGLONG(8, BigInteger.class),
	VT_INT24(9, Integer.class),
	VT_DATE(10, String.class),
	VT_TIME(11, String.class),
	VT_DATETIME(12, String.class),
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
	VT_VAR_STRING(253, String.class),
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

	/**
	 * VtTablet returns all field values as Strings. convert constructs Java
	 * types corresponding to VT types
	 */
	Object convert(String s) {
		if (this.javaType == Integer.class) {
			return Integer.valueOf(s);
		}
		if (this.javaType == Long.class) {
			return Long.valueOf(s);
		}
		if (this.javaType == Float.class) {
			return Float.valueOf(s);
		}
		if (this.javaType == Double.class) {
			return Double.valueOf(s);
		}
		if (this.javaType == BigInteger.class) {
			return new BigInteger(s);
		}

		return s;
	}
}
