// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/proto/query"
)

// These bit flags can be used to query on the
// common properties of types.
const (
	IsNumber   = int(query.Flag_ISNUMBER)
	IsUnsigned = int(query.Flag_ISUNSIGNED)
	IsFloat    = int(query.Flag_ISFLOAT)
	IsQuoted   = int(query.Flag_ISQUOTED)
	IsText     = int(query.Flag_ISTEXT)
	IsBinary   = int(query.Flag_ISBINARY)
)

// Vitess data types. These are idiomatically
// named synonyms for the query.Type values.
const (
	Null      = query.Type_NULL
	TinyInt   = query.Type_TINYINT
	TinyUint  = query.Type_TINYUINT
	ShortInt  = query.Type_SHORTINT
	ShortUint = query.Type_SHORTUINT
	Int24     = query.Type_INT24
	Uint24    = query.Type_UINT24
	Long      = query.Type_LONG
	Ulong     = query.Type_ULONG
	Longlong  = query.Type_LONGLONG
	Ulonglong = query.Type_ULONGLONG
	Float     = query.Type_FLOAT
	Double    = query.Type_DOUBLE
	Timestamp = query.Type_TIMESTAMP
	Date      = query.Type_DATE
	Time      = query.Type_TIME
	Datetime  = query.Type_DATETIME
	Year      = query.Type_YEAR
	Decimal   = query.Type_DECIMAL
	Text      = query.Type_TEXT
	Blob      = query.Type_BLOB
	VarChar   = query.Type_VARCHAR
	VarBinary = query.Type_VARBINARY
	Char      = query.Type_CHAR
	Binary    = query.Type_BINARY
	Bit       = query.Type_BIT
	Enum      = query.Type_ENUM
	Set       = query.Type_SET
)

// bit-shift the mysql flags by one byte so we
// can merge them with the mysql types.
const (
	mysqlUnsigned = 32 << 8
	mysqlBinary   = 128 << 8
	mysqlEnum     = 256 << 8
	mysqlSet      = 2048 << 8

	relevantFlags = mysqlUnsigned |
		mysqlBinary |
		mysqlEnum |
		mysqlSet
)

// If you add to this map, make sure you add a test case
// in tabletserver/endtoend.
var mysqlToType = map[int]query.Type{
	1:                  TinyInt,
	1 | mysqlUnsigned:  TinyUint,
	2:                  ShortInt,
	2 | mysqlUnsigned:  ShortUint,
	3:                  Long,
	3 | mysqlUnsigned:  Ulong,
	4:                  Float,
	5:                  Double,
	6:                  Null,
	6 | mysqlBinary:    Null,
	7:                  Timestamp,
	8:                  Longlong,
	8 | mysqlUnsigned:  Ulonglong,
	9:                  Int24,
	9 | mysqlUnsigned:  Uint24,
	10 | mysqlBinary:   Date,
	11 | mysqlBinary:   Time,
	12 | mysqlBinary:   Datetime,
	13 | mysqlUnsigned: Year,
	16 | mysqlUnsigned: Bit,
	246:                Decimal,
	252:                Text,
	252 | mysqlBinary:  Blob,
	253:                VarChar,
	253 | mysqlBinary:  VarBinary,
	254:                Char,
	254 | mysqlBinary:  Binary,
	254 | mysqlEnum:    Enum,
	254 | mysqlSet:     Set,
}

// TypeFromMySQL computes the vitess type from mysql type and flags.
func TypeFromMySQL(mysqlType, flags int) (query.Type, error) {
	converted := (flags << 8) & relevantFlags

	result, ok := mysqlToType[mysqlType|converted]
	if !ok {
		return Null, fmt.Errorf("Could not map: %d:%x to a vitess type", mysqlType, converted)
	}
	return result, nil
}
