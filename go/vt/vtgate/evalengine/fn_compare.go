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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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

func (b *builtinCoalesce) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	var ta typeAggregation
	for _, arg := range b.Arguments {
		tt, f := arg.typeof(env, fields)
		ta.add(tt, f)
	}
	return ta.result(), flagNullable
}

func (b *builtinCoalesce) compile(c *compiler) (ctype, error) {
	return ctype{}, c.unsupported(b)
}

func getMultiComparisonFunc(args []eval) multiComparisonFunc {
	var (
		integersI int
		integersU int
		floats    int
		decimals  int
		text      int
		binary    int
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
			integersI++
		case *evalUint64:
			integersU++
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

	if integersI+integersU == len(args) {
		if integersI == len(args) {
			return compareAllInteger_i
		}
		if integersU == len(args) {
			return compareAllInteger_u
		}
		return compareAllDecimal
	}
	if binary > 0 || text > 0 {
		if text > 0 {
			return compareAllText
		}
		if binary > 0 {
			return compareAllBinary
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

func compareAllInteger_u(args []eval, cmp int) (eval, error) {
	x := args[0].(*evalUint64)
	for _, arg := range args[1:] {
		y := arg.(*evalUint64)
		if (cmp < 0) == (y.u < x.u) {
			x = y
		}
	}
	return x, nil
}

func compareAllInteger_i(args []eval, cmp int) (eval, error) {
	x := args[0].(*evalInt64)
	for _, arg := range args[1:] {
		y := arg.(*evalInt64)
		if (cmp < 0) == (y.i < x.i) {
			x = y
		}
	}
	return x, nil
}

func compareAllFloat(args []eval, cmp int) (eval, error) {
	candidateF, ok := evalToFloat(args[0])
	if !ok {
		return nil, errDecimalOutOfRange
	}

	for _, arg := range args[1:] {
		thisF, ok := evalToFloat(arg)
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
	decExtreme := evalToDecimal(args[0], 0, 0).dec
	precExtreme := evalDecimalPrecision(args[0])

	for _, arg := range args[1:] {
		d := evalToDecimal(arg, 0, 0).dec
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

	var charsets = make([]charset.Charset, 0, len(args))
	var ca collationAggregation
	for _, arg := range args {
		col := evalCollation(arg)
		if err := ca.add(env, col); err != nil {
			return nil, err
		}
		charsets = append(charsets, col.Collation.Get().Charset())
	}

	tc := ca.result()
	col := tc.Collation.Get()
	cs := col.Charset()

	b1, err := charset.Convert(nil, cs, args[0].ToRawBytes(), charsets[0])
	if err != nil {
		return nil, err
	}

	for i, arg := range args[1:] {
		b2, err := charset.Convert(nil, cs, arg.ToRawBytes(), charsets[i+1])
		if err != nil {
			return nil, err
		}
		if (cmp < 0) == (col.Collate(b2, b1, false) < 0) {
			b1 = b2
		}
	}

	return newEvalText(b1, tc), nil
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

func (call *builtinMultiComparison) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	var (
		integersI int
		integersU int
		floats    int
		decimals  int
		text      int
		binary    int
		flags     typeFlag
	)

	for _, expr := range call.Arguments {
		tt, f := expr.typeof(env, fields)
		flags |= f

		switch tt {
		case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
			integersI++
		case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
			integersU++
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
	if integersI+integersU == len(call.Arguments) {
		if integersI == len(call.Arguments) {
			return sqltypes.Int64, flags
		}
		if integersU == len(call.Arguments) {
			return sqltypes.Uint64, flags
		}
		return sqltypes.Decimal, flags
	}
	if binary > 0 || text > 0 {
		if text > 0 {
			return sqltypes.VarChar, flags
		}
		if binary > 0 {
			return sqltypes.VarBinary, flags
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

func (call *builtinMultiComparison) compile_c(c *compiler, args []ctype) (ctype, error) {
	env := collations.Local()

	var ca collationAggregation
	for _, arg := range args {
		if err := ca.add(env, arg.Col); err != nil {
			return ctype{}, err
		}
	}

	tc := ca.result()
	c.asm.Fn_MULTICMP_c(len(args), call.cmp < 0, tc)
	return ctype{Type: sqltypes.VarChar, Col: tc}, nil
}

func (call *builtinMultiComparison) compile_d(c *compiler, args []ctype) (ctype, error) {
	for i, tt := range args {
		c.compileToDecimal(tt, len(args)-i)
	}
	c.asm.Fn_MULTICMP_d(len(args), call.cmp < 0)
	return ctype{Type: sqltypes.Decimal, Col: collationNumeric}, nil
}

func (call *builtinMultiComparison) compile(c *compiler) (ctype, error) {
	var (
		signed   int
		unsigned int
		floats   int
		decimals int
		text     int
		binary   int
		args     []ctype
	)

	/*
		If any argument is NULL, the result is NULL. No comparison is needed.
		If all arguments are integer-valued, they are compared as integers.
		If at least one argument is double precision, they are compared as double-precision values. Otherwise, if at least one argument is a DECIMAL value, they are compared as DECIMAL values.
		If the arguments comprise a mix of numbers and strings, they are compared as strings.
		If any argument is a nonbinary (character) string, the arguments are compared as nonbinary strings.
		In all other cases, the arguments are compared as binary strings.
	*/

	for _, expr := range call.Arguments {
		tt, err := expr.compile(c)
		if err != nil {
			return ctype{}, err
		}

		args = append(args, tt)

		switch tt.Type {
		case sqltypes.Int64:
			signed++
		case sqltypes.Uint64:
			unsigned++
		case sqltypes.Float64:
			floats++
		case sqltypes.Decimal:
			decimals++
		case sqltypes.Text, sqltypes.VarChar:
			text++
		case sqltypes.Blob, sqltypes.Binary, sqltypes.VarBinary:
			binary++
		default:
			return ctype{}, c.unsupported(call)
		}
	}

	if signed+unsigned == len(args) {
		if signed == len(args) {
			c.asm.Fn_MULTICMP_i(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
		}
		if unsigned == len(args) {
			c.asm.Fn_MULTICMP_u(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Uint64, Col: collationNumeric}, nil
		}
		return call.compile_d(c, args)
	}
	if binary > 0 || text > 0 {
		if text > 0 {
			return call.compile_c(c, args)
		}
		c.asm.Fn_MULTICMP_b(len(args), call.cmp < 0)
		return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil
	} else {
		if floats > 0 {
			for i, tt := range args {
				c.compileToFloat(tt, len(args)-i)
			}
			c.asm.Fn_MULTICMP_f(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Float64, Col: collationNumeric}, nil
		}
		if decimals > 0 {
			return call.compile_d(c, args)
		}
	}
	return ctype{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected argument for GREATEST/LEAST")
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
