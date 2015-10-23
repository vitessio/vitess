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
	IsNumber   = int(query.Flag_IsNumber)
	IsUnsigned = int(query.Flag_IsUnsigned)
	IsFloat    = int(query.Flag_IsFloat)
	IsQuoted   = int(query.Flag_IsQuoted)
	IsText     = int(query.Flag_IsText)
	IsBinary   = int(query.Flag_IsBinary)
)

// Vitess data types. These are idiomatically
// named synonyms for the query.Type values.
const (
	Null      = query.Type_Null
	TinyInt   = query.Type_TinyInt
	TinyUint  = query.Type_TinyUint
	ShortInt  = query.Type_ShortInt
	ShortUint = query.Type_ShortUint
	Int24     = query.Type_Int24
	Uint24    = query.Type_Uint24
	Long      = query.Type_Long
	Ulong     = query.Type_Ulong
	Longlong  = query.Type_Longlong
	Ulonglong = query.Type_Ulonglong
	Float     = query.Type_Float
	Double    = query.Type_Double
	Timestamp = query.Type_Timestamp
	Date      = query.Type_Date
	Time      = query.Type_Time
	Datetime  = query.Type_Datetime
	Year      = query.Type_Year
	Decimal   = query.Type_Decimal
	Text      = query.Type_Text
	Blob      = query.Type_Blob
	VarChar   = query.Type_VarChar
	VarBinary = query.Type_VarBinary
	Char      = query.Type_Char
	Binary    = query.Type_Binary
	Bit       = query.Type_Bit
	Enum      = query.Type_Enum
	Set       = query.Type_Set
	Geometry  = query.Type_Geometry
)

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

// TypeFromMySQL computes the vitess from the mysql type and flags.
func TypeFromMySQL(mysqlType, flags int) (query.Type, error) {
	converted := (flags << 8) & relevantFlags

	result, ok := mysqlToType[mysqlType|converted]
	if !ok {
		return Null, fmt.Errorf("Could not map: %d:%x to a vitess type", mysqlType, converted)
	}
	return result, nil
}
