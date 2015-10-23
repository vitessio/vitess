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
	Int8      = query.Type_INT8
	Uint8     = query.Type_UINT8
	Int16     = query.Type_INT16
	Uint16    = query.Type_UINT16
	Int24     = query.Type_INT24
	Uint24    = query.Type_UINT24
	Int32     = query.Type_INT32
	Uint32    = query.Type_UINT32
	Int64     = query.Type_INT64
	Uint64    = query.Type_UINT64
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
var mysqlToType = map[int64]query.Type{
	1:   Int8,
	2:   Int16,
	3:   Int32,
	4:   Float,
	5:   Double,
	6:   Null,
	7:   Timestamp,
	8:   Int64,
	9:   Int24,
	10:  Date,
	11:  Time,
	12:  Datetime,
	13:  Year,
	16:  Bit,
	246: Decimal,
	252: Text,
	253: VarChar,
	254: Char,
}

var modifier = map[int64]query.Type{
	int64(Int8) | mysqlUnsigned:  Uint8,
	int64(Int16) | mysqlUnsigned: Uint16,
	int64(Int32) | mysqlUnsigned: Uint32,
	int64(Int64) | mysqlUnsigned: Uint64,
	int64(Int24) | mysqlUnsigned: Uint24,
	int64(Text) | mysqlBinary:    Blob,
	int64(VarChar) | mysqlBinary: VarBinary,
	int64(Char) | mysqlBinary:    Binary,
	int64(Char) | mysqlEnum:      Enum,
	int64(Char) | mysqlSet:       Set,
}

// typeToMySQL is the reverse of mysqlToType.
var typeToMySQL = map[query.Type]struct {
	typ   int64
	flags int64
}{
	Int8:      {typ: 1},
	Uint8:     {typ: 1, flags: mysqlUnsigned},
	Int16:     {typ: 2},
	Uint16:    {typ: 2, flags: mysqlUnsigned},
	Int32:     {typ: 3},
	Uint32:    {typ: 3, flags: mysqlUnsigned},
	Float:     {typ: 4},
	Double:    {typ: 5},
	Null:      {typ: 6, flags: mysqlBinary},
	Timestamp: {typ: 7},
	Int64:     {typ: 8},
	Uint64:    {typ: 8, flags: mysqlUnsigned},
	Int24:     {typ: 9},
	Uint24:    {typ: 9, flags: mysqlUnsigned},
	Date:      {typ: 10, flags: mysqlBinary},
	Time:      {typ: 11, flags: mysqlBinary},
	Datetime:  {typ: 12, flags: mysqlBinary},
	Year:      {typ: 13, flags: mysqlUnsigned},
	Bit:       {typ: 16, flags: mysqlUnsigned},
	Decimal:   {typ: 246},
	Text:      {typ: 252},
	Blob:      {typ: 252, flags: mysqlBinary},
	VarChar:   {typ: 253},
	VarBinary: {typ: 253, flags: mysqlBinary},
	Char:      {typ: 254},
	Binary:    {typ: 254, flags: mysqlBinary},
	Enum:      {typ: 254, flags: mysqlEnum},
	Set:       {typ: 254, flags: mysqlSet},
}

// MySQLToType computes the vitess type from mysql type and flags.
func MySQLToType(mysqlType, flags int64) (query.Type, error) {
	result, ok := mysqlToType[mysqlType]
	if !ok {
		return Null, fmt.Errorf("Could not map: %d to a vitess type", mysqlType)
	}

	converted := (flags << 8) & relevantFlags
	modified, ok := modifier[int64(result)|converted]
	if ok {
		return modified, nil
	}
	return result, nil
}

// TypeToMySQL returns the equivalent mysql type and flag for a vitess type.
func TypeToMySQL(typ query.Type) (mysqlType, flags int64) {
	val := typeToMySQL[typ]
	return val.typ, val.flags >> 8
}
