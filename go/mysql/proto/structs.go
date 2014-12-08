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

// Field described a column returned by mysql
type Field struct {
	Name string
	Type int64
}

// QueryResult is the structure returned by the mysql library.
// When transmitted over the wire, the Rows all come back as strings
// and lose their original sqltypes. use Fields.Type to convert
// them back if needed, using the following functions.
type QueryResult struct {
	Fields       []Field
	RowsAffected uint64
	InsertId     uint64
	Rows         [][]sqltypes.Value
}

// Charset contains the per-statement character set settings that accompany
// binlog QUERY_EVENT entries.
type Charset struct {
	Client int // @@session.character_set_client
	Conn   int // @@session.collation_connection
	Server int // @@session.collation_server
}

// Convert takes a type and a value, and returns the type:
// - nil for NULL value
// - int64 if possible, otherwise, uint64
// - float64 for floating point values that fit in a float
// - []byte for everything else
func Convert(mysqlType int64, val sqltypes.Value) (interface{}, error) {
	if val.IsNull() {
		return nil, nil
	}

	switch mysqlType {
	case VT_TINY, VT_SHORT, VT_LONG, VT_LONGLONG, VT_INT24:
		val := val.String()
		signed, err := strconv.ParseInt(val, 0, 64)
		if err == nil {
			return signed, nil
		}
		unsigned, err := strconv.ParseUint(val, 0, 64)
		if err == nil {
			return unsigned, nil
		}
		return nil, err
	case VT_FLOAT, VT_DOUBLE:
		return strconv.ParseFloat(val.String(), 64)
	}
	return val.Raw(), nil
}
