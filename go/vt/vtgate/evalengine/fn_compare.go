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
	"bytes"
	"math"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

type (
	builtinCoalesce struct {
		CallExpr
	}

	multiComparisonFunc func(args []eval, cmp int) (eval, error)

	builtinMultiComparison struct {
		CallExpr
		cmp int
	}
)

var _ Expr = (*builtinBitCount)(nil)
var _ Expr = (*builtinMultiComparison)(nil)

func (b *builtinCoalesce) eval(env *ExpressionEnv) (eval, error) {
	args, err := b.args(env)
	if err != nil {
		return nil, err
	}
	for _, arg := range args {
		if arg != nil {
			return arg, nil
		}
	}
	return nil, nil
}

func (b *builtinCoalesce) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	var ta typeAggregation
	for _, arg := range b.Arguments {
		tt, f := arg.typeof(env)
		ta.add(tt, f)
	}
	return ta.result(), flagNullable
}

func getMultiComparisonFunc(args []eval) multiComparisonFunc {
	var (
		integers int
		floats   int
		decimals int
		text     int
		binary   int
	)

	/*
		If any argument is NULL, the result is NULL. No comparison is needed.
		If all arguments are integer-valued, they are compared as integers.
		If at least one argument is double precision, they are compared as double-precision values. Otherwise, if at least one argument is a DECIMAL value, they are compared as DECIMAL values.
		If the arguments comprise a mix of numbers and strings, they are compared as strings.
		If any argument is a nonbinary (character) string, the arguments are compared as nonbinary strings.
		In all other cases, the arguments are compared as binary strings.
	*/

	for _, arg := range args {
		if arg == nil {
			return func(args []eval, cmp int) (eval, error) {
				return nil, nil
			}
		}

		switch arg := arg.(type) {
		case *evalInt64:
			integers++
		case *evalUint64:
			if arg.u > math.MaxInt64 {
				decimals++
			} else {
				integers++
			}
		case *evalFloat:
			floats++
		case *evalDecimal:
			decimals++
		case *evalBytes:
			switch arg.SQLType() {
			case sqltypes.Text, sqltypes.VarChar:
				text++
			case sqltypes.Blob, sqltypes.Binary, sqltypes.VarBinary:
				binary++
			}
		}
	}

	if integers == len(args) {
		return compareAllInteger
	}
	if binary > 0 || text > 0 {
		if binary > 0 {
			return compareAllBinary
		}
		if text > 0 {
			return compareAllText
		}
	} else {
		if floats > 0 {
			return compareAllFloat
		}
		if decimals > 0 {
			return compareAllDecimal
		}
	}
	panic("unexpected argument type")
}

func compareAllInteger(args []eval, cmp int) (eval, error) {
	var candidateI = args[0].(*evalInt64).i
	for _, arg := range args[1:] {
		thisI := arg.(*evalInt64).i
		if (cmp < 0) == (thisI < candidateI) {
			candidateI = thisI
		}
	}
	return &evalInt64{candidateI}, nil
}

func compareAllFloat(args []eval, cmp int) (eval, error) {
	candidateF, ok := evalToNumeric(args[0]).toFloat()
	if !ok {
		return nil, errDecimalOutOfRange
	}

	for _, arg := range args[1:] {
		thisF, ok := evalToNumeric(arg).toFloat()
		if !ok {
			return nil, errDecimalOutOfRange
		}
		if (cmp < 0) == (thisF.f < candidateF.f) {
			candidateF = thisF
		}
	}
	return candidateF, nil
}

func evalDecimalPrecision(e eval) int32 {
	if d, ok := e.(*evalDecimal); ok {
		return d.length
	}
	return 0
}

func compareAllDecimal(args []eval, cmp int) (eval, error) {
	decExtreme := evalToNumeric(args[0]).toDecimal(0, 0).dec
	precExtreme := evalDecimalPrecision(args[0])

	for _, arg := range args[1:] {
		d := evalToNumeric(arg).toDecimal(0, 0).dec
		if (cmp < 0) == (d.Cmp(decExtreme) < 0) {
			decExtreme = d
		}
		p := evalDecimalPrecision(arg)
		if p > precExtreme {
			precExtreme = p
		}
	}
	return newEvalDecimalWithPrec(decExtreme, precExtreme), nil
}

func compareAllText(args []eval, cmp int) (eval, error) {
	env := collations.Local()
	candidateB := args[0].ToRawBytes()
	collationB := evalCollation(args[0])

	var ca collationAggregation
	if err := ca.add(env, collationB); err != nil {
		return nil, err
	}

	for _, arg := range args[1:] {
		thisB := arg.ToRawBytes()
		thisColl := evalCollation(arg)
		if err := ca.add(env, thisColl); err != nil {
			return nil, err
		}

		thisTC, coerceLeft, coerceRight, err := env.MergeCollations(thisColl, collationB, collations.CoercionOptions{ConvertToSuperset: true, ConvertWithCoercion: true})
		if err != nil {
			return nil, err
		}

		collation := env.LookupByID(thisTC.Collation)

		var leftB = thisB
		var rightB = candidateB
		if coerceLeft != nil {
			leftB, _ = coerceLeft(nil, leftB)
		}
		if coerceRight != nil {
			rightB, _ = coerceRight(nil, rightB)
		}
		if (cmp < 0) == (collation.Collate(leftB, rightB, false) < 0) {
			candidateB = thisB
		}
	}

	return newEvalText(candidateB, ca.result()), nil
}

func compareAllBinary(args []eval, cmp int) (eval, error) {
	candidateB := args[0].ToRawBytes()

	for _, arg := range args[1:] {
		thisB := arg.ToRawBytes()
		if (cmp < 0) == (bytes.Compare(thisB, candidateB) < 0) {
			candidateB = thisB
		}
	}

	return newEvalBinary(candidateB), nil
}

func (call *builtinMultiComparison) eval(env *ExpressionEnv) (eval, error) {
	args, err := call.args(env)
	if err != nil {
		return nil, err
	}
	return getMultiComparisonFunc(args)(args, call.cmp)
}

func (call *builtinMultiComparison) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	var (
		integers int
		floats   int
		decimals int
		text     int
		binary   int
		flags    typeFlag
	)

	for _, expr := range call.Arguments {
		tt, f := expr.typeof(env)
		flags |= f

		switch tt {
		case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
			integers++
		case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
			if f&flagIntegerOvf != 0 {
				decimals++
			} else {
				integers++
			}
		case sqltypes.Float32, sqltypes.Float64:
			floats++
		case sqltypes.Decimal:
			decimals++
		case sqltypes.Text, sqltypes.VarChar:
			text++
		case sqltypes.Blob, sqltypes.Binary, sqltypes.VarBinary:
			binary++
		}
	}

	if flags&flagNull != 0 {
		return sqltypes.Null, flags
	}
	if integers == len(call.Arguments) {
		return sqltypes.Int64, flags
	}
	if binary > 0 || text > 0 {
		if binary > 0 {
			return sqltypes.VarBinary, flags
		}
		if text > 0 {
			return sqltypes.VarChar, flags
		}
	} else {
		if floats > 0 {
			return sqltypes.Float64, flags
		}
		if decimals > 0 {
			return sqltypes.Decimal, flags
		}
	}
	panic("unexpected argument type")
}

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
}

func (ta *typeAggregation) add(tt sqltypes.Type, f typeFlag) {
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
	case sqltypes.Blob:
		ta.blob++
	default:
		return
	}
	ta.total++
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
		If any type is BLOB, the result is BLOB.
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
		if ta.unsignedMax == sqltypes.Uint64 && ta.signed > 0 {
			return sqltypes.Decimal
		}
		// TODO
		return sqltypes.Uint64
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
