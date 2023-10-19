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
	"vitess.io/vitess/go/mysql/collations/colldata"
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
		charsets = append(charsets, colldata.Lookup(col.Collation).Charset())
	}

	tc := ca.result()
	col := colldata.Lookup(tc.Collation)
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
