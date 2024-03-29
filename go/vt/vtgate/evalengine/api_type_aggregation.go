/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

type typeAggregation struct {
	double   uint16
	decimal  uint16
	signed   uint16
	unsigned uint16

	signedMax   sqltypes.Type
	unsignedMax sqltypes.Type

	bit       uint16
	year      uint16
	char      uint16
	binary    uint16
	charother uint16
	json      uint16

	date      uint16
	time      uint16
	timestamp uint16
	datetime  uint16

	geometry uint16
	blob     uint16
	total    uint16

	nullable bool
}

type TypeAggregator struct {
	types       typeAggregation
	collations  collationAggregation
	size, scale int32
	invalid     int32
}

func (ta *TypeAggregator) Add(typ Type, env *collations.Environment) error {
	if !typ.Valid() {
		ta.invalid++
		return nil
	}

	ta.types.addNullable(typ.typ, typ.nullable)
	if err := ta.collations.add(typedCoercionCollation(typ.typ, typ.collation), env); err != nil {
		return err
	}
	ta.size = max(typ.size, ta.size)
	ta.scale = max(typ.scale, ta.scale)
	return nil
}

func (ta *TypeAggregator) AddField(f *query.Field, env *collations.Environment) error {
	return ta.Add(NewTypeFromField(f), env)
}

func (ta *TypeAggregator) Type() Type {
	if ta.invalid > 0 || ta.types.empty() {
		return Type{}
	}
	return NewTypeEx(ta.types.result(), ta.collations.result().Collation, ta.types.nullable, ta.size, ta.scale)
}

func (ta *TypeAggregator) Field(name string) *query.Field {
	typ := ta.Type()
	return typ.ToField(name)
}

func (ta *typeAggregation) empty() bool {
	return ta.total == 0
}

func (ta *typeAggregation) addEval(e eval) {
	var t sqltypes.Type
	var f typeFlag
	switch e := e.(type) {
	case nil:
		t = sqltypes.Null
		ta.nullable = true
	case *evalBytes:
		t = sqltypes.Type(e.tt)
		f = e.flag
	default:
		t = e.SQLType()
	}
	ta.add(t, f)
}

func (ta *typeAggregation) addNullable(typ sqltypes.Type, nullable bool) {
	var flag typeFlag
	if typ == sqltypes.HexVal || typ == sqltypes.HexNum {
		typ = sqltypes.Binary
		flag |= flagHex
	}
	if nullable {
		flag |= flagNullable
	}
	ta.add(typ, flag)
}

func (ta *typeAggregation) add(tt sqltypes.Type, f typeFlag) {
	if f&flagNullable != 0 {
		ta.nullable = true
	}
	switch tt {
	case sqltypes.Float32, sqltypes.Float64:
		ta.double++
	case sqltypes.Decimal:
		ta.decimal++
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
		ta.signed++
		if tt > ta.signedMax {
			ta.signedMax = tt
		}
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		ta.unsigned++
		if tt > ta.unsignedMax {
			ta.unsignedMax = tt
		}
	case sqltypes.Bit:
		ta.bit++
	case sqltypes.Year:
		ta.year++
	case sqltypes.Char, sqltypes.VarChar, sqltypes.Set, sqltypes.Enum:
		if f&flagExplicitCollation != 0 {
			ta.charother++
		}
		ta.char++
	case sqltypes.Binary, sqltypes.VarBinary:
		if f&flagHex != 0 {
			ta.charother++
		}
		ta.binary++
	case sqltypes.TypeJSON:
		ta.json++
	case sqltypes.Date:
		ta.date++
	case sqltypes.Datetime:
		ta.datetime++
	case sqltypes.Time:
		ta.time++
	case sqltypes.Timestamp:
		ta.timestamp++
	case sqltypes.Geometry:
		ta.geometry++
	case sqltypes.Blob, sqltypes.Text:
		ta.blob++
	default:
		return
	}
	ta.total++
}

func nextSignedTypeForUnsigned(t sqltypes.Type) sqltypes.Type {
	switch t {
	case sqltypes.Uint8:
		return sqltypes.Int16
	case sqltypes.Uint16:
		return sqltypes.Int24
	case sqltypes.Uint24:
		return sqltypes.Int32
	case sqltypes.Uint32:
		return sqltypes.Int64
	case sqltypes.Uint64:
		return sqltypes.Decimal
	default:
		panic("bad unsigned integer type")
	}
}

func (ta *typeAggregation) result() sqltypes.Type {
	/*
		If all types are numeric, the aggregated type is also numeric:
			If at least one argument is double precision, the result is double precision.
			Otherwise, if at least one argument is DECIMAL, the result is DECIMAL.
			Otherwise, the result is an integer type (with one exception):
				If all integer types are all signed or all unsigned, the result is the same sign and the precision is the highest of all specified integer types (that is, TINYINT, SMALLINT, MEDIUMINT, INT, or BIGINT).
				If there is a combination of signed and unsigned integer types, the result is signed and the precision may be higher. For example, if the types are signed INT and unsigned INT, the result is signed BIGINT.
				The exception is unsigned BIGINT combined with any signed integer type. The result is DECIMAL with sufficient precision and scale 0.
		If all types are BIT, the result is BIT. Otherwise, BIT arguments are treated similar to BIGINT.
		If all types are YEAR, the result is YEAR. Otherwise, YEAR arguments are treated similar to INT.
		If all types are character string (CHAR or VARCHAR), the result is VARCHAR with maximum length determined by the longest character length of the operands.
		If all types are character or binary string, the result is VARBINARY.
		SET and ENUM are treated similar to VARCHAR; the result is VARCHAR.
		If all types are JSON, the result is JSON.
		If all types are temporal, the result is temporal:
			If all temporal types are DATE, TIME, or TIMESTAMP, the result is DATE, TIME, or TIMESTAMP, respectively.
			Otherwise, for a mix of temporal types, the result is DATETIME.
		If all types are GEOMETRY, the result is GEOMETRY.
		If any type is BLOB, the result is BLOB. This also applies to TEXT.
		For all other type combinations, the result is VARCHAR.
		Literal NULL operands are ignored for type aggregation.
	*/

	if ta.bit == ta.total {
		return sqltypes.Bit
	} else if ta.bit > 0 {
		ta.signed += ta.bit
		ta.signedMax = sqltypes.Int64
	}

	if ta.year == ta.total {
		return sqltypes.Year
	} else if ta.year > 0 {
		ta.signed += ta.year
		if sqltypes.Int32 > ta.signedMax {
			ta.signedMax = sqltypes.Int32
		}
	}

	if ta.double+ta.decimal+ta.signed+ta.unsigned == ta.total {
		if ta.double > 0 {
			return sqltypes.Float64
		}
		if ta.decimal > 0 {
			return sqltypes.Decimal
		}
		if ta.signed == ta.total {
			return ta.signedMax
		}
		if ta.unsigned == ta.total {
			return ta.unsignedMax
		}
		if ta.signed == 0 {
			panic("bad type aggregation for signed/unsigned types")
		}
		agtype := nextSignedTypeForUnsigned(ta.unsignedMax)
		if sqltypes.IsSigned(agtype) {
			return max(agtype, ta.signedMax)
		}
		return agtype
	}

	if ta.char == ta.total {
		return sqltypes.VarChar
	}
	if ta.char+ta.binary == ta.total {
		// HACK: this is not in the official documentation, but groups of strings where
		// one of the strings is not directly a VARCHAR or VARBINARY (e.g. a hex literal,
		// or a VARCHAR that has been explicitly collated) will result in VARCHAR when
		// aggregated
		if ta.charother > 0 {
			return sqltypes.VarChar
		}
		return sqltypes.VarBinary
	}
	if ta.json == ta.total {
		return sqltypes.TypeJSON
	}
	if ta.date+ta.time+ta.timestamp+ta.datetime == ta.total {
		if ta.date == ta.total {
			return sqltypes.Date
		}
		if ta.time == ta.total {
			return sqltypes.Time
		}
		if ta.timestamp == ta.total {
			return sqltypes.Timestamp
		}
		return sqltypes.Datetime
	}
	if ta.geometry == ta.total {
		return sqltypes.Geometry
	}
	if ta.blob > 0 {
		return sqltypes.Blob
	}
	return sqltypes.VarChar
}
