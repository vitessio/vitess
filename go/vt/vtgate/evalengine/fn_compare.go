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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	builtinCoalesce struct {
		CallExpr
	}

	multiComparisonFunc func(collationEnv *collations.Environment, args []eval, cmp int) (eval, error)

	builtinMultiComparison struct {
		CallExpr
		cmp int
	}
)

var _ IR = (*builtinBitCount)(nil)
var _ IR = (*builtinMultiComparison)(nil)

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

func (b *builtinCoalesce) compile(c *compiler) (ctype, error) {
	var (
		ta typeAggregation
		ca collationAggregation
	)

	f := flagNullable
	for _, arg := range b.Arguments {
		tt, err := arg.compile(c)
		if err != nil {
			return ctype{}, err
		}
		if !tt.nullable() {
			f = 0
		}
		ta.add(tt.Type, tt.Flag)
		if err := ca.add(tt.Col, c.env.CollationEnv()); err != nil {
			return ctype{}, err
		}
	}

	args := len(b.Arguments)
	c.asm.adjustStack(-(args - 1))
	c.asm.emit(func(env *ExpressionEnv) int {
		for sp := env.vm.sp - args; sp < env.vm.sp; sp++ {
			if env.vm.stack[sp] != nil {
				env.vm.stack[env.vm.sp-args] = env.vm.stack[sp]
				break
			}
		}
		env.vm.sp -= args - 1
		return 1
	}, "COALESCE (SP-%d) ... (SP-1)", args)

	return ctype{Type: ta.result(), Flag: f, Col: ca.result()}, nil
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
			return func(collationEnv *collations.Environment, args []eval, cmp int) (eval, error) {
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

func compareAllInteger_u(_ *collations.Environment, args []eval, cmp int) (eval, error) {
	x := args[0].(*evalUint64)
	for _, arg := range args[1:] {
		y := arg.(*evalUint64)
		if (cmp < 0) == (y.u < x.u) {
			x = y
		}
	}
	return x, nil
}

func compareAllInteger_i(_ *collations.Environment, args []eval, cmp int) (eval, error) {
	x := args[0].(*evalInt64)
	for _, arg := range args[1:] {
		y := arg.(*evalInt64)
		if (cmp < 0) == (y.i < x.i) {
			x = y
		}
	}
	return x, nil
}

func compareAllFloat(_ *collations.Environment, args []eval, cmp int) (eval, error) {
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

func compareAllDecimal(_ *collations.Environment, args []eval, cmp int) (eval, error) {
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

func compareAllText(collationEnv *collations.Environment, args []eval, cmp int) (eval, error) {
	var charsets = make([]charset.Charset, 0, len(args))
	var ca collationAggregation
	for _, arg := range args {
		col := evalCollation(arg)
		if err := ca.add(col, collationEnv); err != nil {
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

func compareAllBinary(_ *collations.Environment, args []eval, cmp int) (eval, error) {
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
	return getMultiComparisonFunc(args)(env.collationEnv, args, call.cmp)
}

func (call *builtinMultiComparison) compile_c(c *compiler, args []ctype) (ctype, error) {
	var ca collationAggregation
	var f typeFlag
	for _, arg := range args {
		f |= nullableFlags(arg.Flag)
		if err := ca.add(arg.Col, c.env.CollationEnv()); err != nil {
			return ctype{}, err
		}
	}

	tc := ca.result()
	c.asm.Fn_MULTICMP_c(len(args), call.cmp < 0, tc)
	return ctype{Type: sqltypes.VarChar, Flag: f, Col: tc}, nil
}

func (call *builtinMultiComparison) compile_d(c *compiler, args []ctype) (ctype, error) {
	var f typeFlag
	for i, tt := range args {
		f |= nullableFlags(tt.Flag)
		c.compileToDecimal(tt, len(args)-i)
	}
	c.asm.Fn_MULTICMP_d(len(args), call.cmp < 0)
	return ctype{Type: sqltypes.Decimal, Flag: f, Col: collationNumeric}, nil
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
		nullable bool
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

		nullable = nullable || tt.nullable()
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
		case sqltypes.Null:
			nullable = true
		default:
			panic("unexpected argument type")
		}
	}

	var f typeFlag
	if nullable {
		f |= flagNullable
	}
	if signed+unsigned == len(args) {
		if signed == len(args) {
			c.asm.Fn_MULTICMP_i(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Int64, Flag: f, Col: collationNumeric}, nil
		}
		if unsigned == len(args) {
			c.asm.Fn_MULTICMP_u(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Uint64, Flag: f, Col: collationNumeric}, nil
		}
		return call.compile_d(c, args)
	}
	if binary > 0 || text > 0 {
		if text > 0 {
			return call.compile_c(c, args)
		}
		c.asm.Fn_MULTICMP_b(len(args), call.cmp < 0)
		return ctype{Type: sqltypes.VarBinary, Flag: f, Col: collationBinary}, nil
	} else {
		if floats > 0 {
			for i, tt := range args {
				c.compileToFloat(tt, len(args)-i)
			}
			c.asm.Fn_MULTICMP_f(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Float64, Flag: f, Col: collationNumeric}, nil
		}
		if decimals > 0 {
			return call.compile_d(c, args)
		}
	}
	return ctype{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected argument for GREATEST/LEAST")
}
