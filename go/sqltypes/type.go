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
var mysqlToType = map[int64]query.Type{
	1:   TinyInt,
	2:   ShortInt,
	3:   Long,
	4:   Float,
	5:   Double,
	6:   Null,
	7:   Timestamp,
	8:   Longlong,
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
	int64(TinyInt) | mysqlUnsigned:  TinyUint,
	int64(ShortInt) | mysqlUnsigned: ShortUint,
	int64(Long) | mysqlUnsigned:     Ulong,
	int64(Longlong) | mysqlUnsigned: Ulonglong,
	int64(Int24) | mysqlUnsigned:    Uint24,
	int64(Text) | mysqlBinary:       Blob,
	int64(VarChar) | mysqlBinary:    VarBinary,
	int64(Char) | mysqlBinary:       Binary,
	int64(Char) | mysqlEnum:         Enum,
	int64(Char) | mysqlSet:          Set,
}

// typeToMySQL is the reverse of mysqlToType.
var typeToMySQL = map[query.Type]struct {
	typ   int64
	flags int64
}{
	TinyInt:   {typ: 1},
	TinyUint:  {typ: 1, flags: mysqlUnsigned},
	ShortInt:  {typ: 2},
	ShortUint: {typ: 2, flags: mysqlUnsigned},
	Long:      {typ: 3},
	Ulong:     {typ: 3, flags: mysqlUnsigned},
	Float:     {typ: 4},
	Double:    {typ: 5},
	Null:      {typ: 6, flags: mysqlBinary},
	Timestamp: {typ: 7},
	Longlong:  {typ: 8},
	Ulonglong: {typ: 8, flags: mysqlUnsigned},
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
