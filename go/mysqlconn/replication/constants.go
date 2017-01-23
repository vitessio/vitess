package replication

// This file contains the constant definitions for this package.

// This is the data type for a field.
// Values taken from include/mysql/mysql_com.h
// FIXME(alainjobart) this is the same info as constants describes in
// the go/mysql package. We will remove that package, and merge this
// package with go/mysqlconn as asoon as we can.
const (
	// TypeDecimal is MYSQL_TYPE_DECIMAL. It is deprecated.
	TypeDecimal = 0

	// TypeTiny is MYSQL_TYPE_TINY
	TypeTiny = 1

	// TypeShort is MYSQL_TYPE_SHORT
	TypeShort = 2

	// TypeLong is MYSQL_TYPE_LONG
	TypeLong = 3

	// TypeFloat is MYSQL_TYPE_FLOAT
	TypeFloat = 4

	// TypeDouble is MYSQL_TYPE_DOUBLE
	TypeDouble = 5

	// TypeNull is MYSQL_TYPE_NULL
	TypeNull = 6

	// TypeTimestamp is MYSQL_TYPE_TIMESTAMP
	TypeTimestamp = 7

	// TypeLongLong is MYSQL_TYPE_LONGLONG
	TypeLongLong = 8

	// TypeInt24 is MYSQL_TYPE_INT24
	TypeInt24 = 9

	// TypeDate is MYSQL_TYPE_DATE
	TypeDate = 10

	// TypeTime is MYSQL_TYPE_TIME
	TypeTime = 11

	// TypeDateTime is MYSQL_TYPE_DATETIME
	TypeDateTime = 12

	// TypeYear is MYSQL_TYPE_YEAR
	TypeYear = 13

	// TypeNewDate is MYSQL_TYPE_NEWDATE
	TypeNewDate = 14

	// TypeVarchar is MYSQL_TYPE_VARCHAR
	TypeVarchar = 15

	// TypeBit is MYSQL_TYPE_BIT
	TypeBit = 16

	// TypeNewDecimal is MYSQL_TYPE_NEWDECIMAL
	TypeNewDecimal = 246

	// TypeEnum is MYSQL_TYPE_ENUM
	TypeEnum = 247

	// TypeSet is MYSQL_TYPE_SET
	TypeSet = 248

	// TypeTinyBlob is MYSQL_TYPE_TINY_BLOB
	TypeTinyBlob = 249

	// TypeMediumBlob is MYSQL_TYPE_MEDIUM_BLOB
	TypeMediumBlob = 250

	// TypeLongBlob is MYSQL_TYPE_LONG_BLOB
	TypeLongBlob = 251

	// TypeBlob is MYSQL_TYPE_BLOB
	TypeBlob = 252

	// TypeVarString is MYSQL_TYPE_VAR_STRING
	TypeVarString = 253

	// TypeString is MYSQL_TYPE_STRING
	TypeString = 254

	// TypeGeometry is MYSQL_TYPE_GEOMETRY
	TypeGeometry = 255
)
