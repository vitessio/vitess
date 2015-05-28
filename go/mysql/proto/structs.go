// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"
)

// These numbers should exactly match values defined in dist/mysql-5.1.52/include/mysql/mysql_com.h
const (
	VT_DECIMAL     = 0
	VT_TINY        = 1
	VT_SHORT       = 2
	VT_LONG        = 3
	VT_FLOAT       = 4
	VT_DOUBLE      = 5
	VT_NULL        = 6
	VT_TIMESTAMP   = 7
	VT_LONGLONG    = 8
	VT_INT24       = 9
	VT_DATE        = 10
	VT_TIME        = 11
	VT_DATETIME    = 12
	VT_YEAR        = 13
	VT_NEWDATE     = 14
	VT_VARCHAR     = 15
	VT_BIT         = 16
	VT_NEWDECIMAL  = 246
	VT_ENUM        = 247
	VT_SET         = 248
	VT_TINY_BLOB   = 249
	VT_MEDIUM_BLOB = 250
	VT_LONG_BLOB   = 251
	VT_BLOB        = 252
	VT_VAR_STRING  = 253
	VT_STRING      = 254
	VT_GEOMETRY    = 255
)

// MySQL field flags bitset values e.g. to distinguish between signed and unsigned integer.
// Comments are taken from the original source code.
// These numbers should exactly match values defined in dist/mysql-5.1.52/include/mysql_com.h
const (
	// VT_ZEROVALUE_FLAG is not part of the MySQL specification and only used in unit tests.
	VT_ZEROVALUE_FLAG    = 0
	VT_NOT_NULL_FLAG     = 1   /* Field can't be NULL */
	VT_PRI_KEY_FLAG      = 2   /* Field is part of a primary key */
	VT_UNIQUE_KEY_FLAG   = 4   /* Field is part of a unique key */
	VT_MULTIPLE_KEY_FLAG = 8   /* Field is part of a key */
	VT_BLOB_FLAG         = 16  /* Field is a blob */
	VT_UNSIGNED_FLAG     = 32  /* Field is unsigned */
	VT_ZEROFILL_FLAG     = 64  /* Field is zerofill */
	VT_BINARY_FLAG       = 128 /* Field is binary   */
	/* The following are only sent to new clients */
	VT_ENUM_FLAG             = 256   /* field is an enum */
	VT_AUTO_INCREMENT_FLAG   = 512   /* field is a autoincrement field */
	VT_TIMESTAMP_FLAG        = 1024  /* Field is a timestamp */
	VT_SET_FLAG              = 2048  /* field is a set */
	VT_NO_DEFAULT_VALUE_FLAG = 4096  /* Field doesn't have default value */
	VT_ON_UPDATE_NOW_FLAG    = 8192  /* Field is set to NOW on UPDATE */
	VT_NUM_FLAG              = 32768 /* Field is num (for clients) */
)

// Field describes a column returned by MySQL.
type Field struct {
	Name  string
	Type  int64
	Flags int64
}

//go:generate bsongen -file $GOFILE -type Field -o field_bson.go

// QueryResult is the structure returned by the mysql library.
// When transmitted over the wire, the Rows all come back as strings
// and lose their original sqltypes. use Fields.Type to convert
// them back if needed, using the following functions.
type QueryResult struct {
	Fields       []Field
	RowsAffected uint64
	InsertId     uint64
	Rows         [][]sqltypes.Value
	Err          RPCError
}

//go:generate bsongen -file $GOFILE -type QueryResult -o query_result_bson.go

// RPCError is the structure that is returned by each RPC call, which contains
// the error information for that call.
type RPCError struct {
	Code    int
	Message string
}

//go:generate bsongen -file $GOFILE -type RPCError -o rpcerror_bson.go

// Charset contains the per-statement character set settings that accompany
// binlog QUERY_EVENT entries.
type Charset struct {
	Client int // @@session.character_set_client
	Conn   int // @@session.collation_connection
	Server int // @@session.collation_server
}

//go:generate bsongen -file $GOFILE -type Charset -o charset_bson.go

// Convert takes a type and a value, and returns the type:
// - nil for NULL value
// - uint64 for unsigned BIGINT values
// - int64 for all other integer values (signed and unsigned)
// - float64 for floating point values that fit in a float
// - []byte for everything else
func Convert(field Field, val sqltypes.Value) (interface{}, error) {
	if val.IsNull() {
		return nil, nil
	}

	switch field.Type {
	case VT_LONGLONG:
		if field.Flags&VT_UNSIGNED_FLAG == VT_UNSIGNED_FLAG {
			return strconv.ParseUint(val.String(), 0, 64)
		}
		return strconv.ParseInt(val.String(), 0, 64)
	case VT_TINY, VT_SHORT, VT_LONG, VT_INT24:
		// Regardless of whether UNSIGNED_FLAG is set in field.Flags, we map all
		// signed and unsigned values to a signed Go type because
		// - Go doesn't officially support uint64 in their SQL interface
		// - there is no loss of the value
		// The only exception we make are for unsigned BIGINTs, see VT_LONGLONG above.
		return strconv.ParseInt(val.String(), 0, 64)
	case VT_FLOAT, VT_DOUBLE:
		return strconv.ParseFloat(val.String(), 64)
	}
	return val.Raw(), nil
}
