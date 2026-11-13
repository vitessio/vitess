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
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	datetime2 "vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	builtinCoalesce struct {
		CallExpr
		// collate is the connection collation. See caseResult.
		collate collations.ID
	}

	multiComparisonFunc func(env *ExpressionEnv, args []eval, cmp, prec int) (eval, error)

	builtinMultiComparison struct {
		CallExpr
		cmp  int
		prec int
	}
)

var (
	_ IR = (*builtinBitCount)(nil)
	_ IR = (*builtinMultiComparison)(nil)
)

func (b *builtinCoalesce) eval(env *ExpressionEnv) (eval, error) {
	args, err := b.args(env)
	if err != nil {
		return nil, err
	}
	var (
		ta typeAggregation
		ca collationAggregation
	)
	for i, arg := range args {
		if err := addCaseBranch(&ta, &ca, b.Arguments[i], arg, env); err != nil {
			return nil, err
		}
	}
	tt, col := coalesceType(&ta, &ca, b.collate, env.currentVersion())
	for _, arg := range args {
		if arg != nil {
			return coalesceResult(arg, tt, ta.size, ta.scale, col, env.now, env.sqlmode.AllowZeroDate())
		}
	}
	return nil, nil
}

// coalesceType returns the type and the collation of the result of
// COALESCE, whose arguments have the types in ta and the collations in ca.
// It is the one of a CASE, see caseResult, except for an ENUM or SET that
// is the only argument that is not the NULL literal: COALESCE(en, NULL)
// is an ENUM, where COALESCE(en, en) is a VARCHAR.
func coalesceType(ta *typeAggregation, ca *collationAggregation, conn collations.ID, version string) (sqltypes.Type, collations.TypedCollation) {
	switch {
	case ta.total == 1 && ta.enum == 1:
		return sqltypes.Enum, ca.result()
	case ta.total == 1 && ta.set == 1:
		return sqltypes.Set, ca.result()
	}
	return caseResult(ta, ca, conn, version)
}

// coalesceResult converts sel, the argument that COALESCE selected, to the
// type tt and the collation col that all the arguments combine to, as CASE
// does. size and scale are the ones of evalCoerce. With an INT column i that
// holds 5, COALESCE(i, 1.5) is the decimal 5.0 and COALESCE(i, s) the
// string '5'.
//
// The result is never a hex or bit literal: COALESCE(0x31) + 0 is 1. It
// keeps the character set of the selected argument for a conversion to
// JSON, see flagJSONBinary.
func coalesceResult(sel eval, tt sqltypes.Type, size, scale int32, col collations.TypedCollation, now time.Time, allowZero bool) (eval, error) {
	var selBinary bool
	if b, ok := sel.(*evalBytes); ok {
		selBinary = b.jsonBinary()
	}
	sel = evalPlain(sel)

	var (
		res eval
		err error
	)
	if b, ok := sel.(*evalBytes); ok && isCoalesceString(b.SQLType()) && isCoalesceString(tt) {
		res, err = coalesceString(b, tt, col)
	} else {
		res, err = evalCoerce(sel, tt, size, scale, col.Collation, now, allowZero)
		res = evalWithCollation(evalTemporalFrom(sel, res, false), col)
	}
	r, ok := res.(*evalBytes)
	if !ok || err != nil {
		return res, err
	}
	if r.jsonBinary() == selBinary {
		return r, nil
	}
	if r == sel {
		c := *r
		r = &c
	}
	if selBinary {
		r.flag |= flagJSONBinary
	} else {
		r.flag |= flagJSONText
	}
	return r, nil
}

// coalesceString converts the string sel to the string type tt and the
// collation col. See coalesceResult.
//
// With a text column s and a binary column vb, COALESCE(s, vb) is a binary
// string, and COALESCE(s, 0x41) has the collation of s.
func coalesceString(sel *evalBytes, tt sqltypes.Type, col collations.TypedCollation) (eval, error) {
	if col.Collation == collations.Unknown {
		return sel, nil
	}
	binary := col.Collation == collations.CollationBinaryID
	r := sel
	switch {
	case sel.isBinary() && binary:
	case sel.isBinary():
		// the bytes of a binary string are read in the character set of col
		r = newEvalText(sel.bytes, col)
	case binary && (tt == sqltypes.VarBinary || tt == sqltypes.Binary || tt == sqltypes.Blob):
		r = newEvalBinary(sel.bytes)
	case sel.col.Collation == collations.CollationBinaryID && !binary:
		// the collation of the string is binary or not known: its bytes
		// are read in the character set of col
		c := *sel
		c.col = col
		r = &c
	case sel.col.Collation != col.Collation:
		t, err := evalToVarchar(sel, col.Collation, true)
		if err != nil {
			return nil, err
		}
		// a TEXT stays a TEXT
		t.tt = sel.tt
		r = t
	}
	if tt == sqltypes.Text || tt == sqltypes.Blob || sqltypes.IsBinary(r.SQLType()) != sqltypes.IsBinary(tt) {
		// the result is a binary string or a TEXT when COALESCE is, also
		// when it is the selected string: COALESCE(s, CAST(js AS CHAR)) is
		// a TEXT
		if r == sel {
			c := *r
			r = &c
		}
		r.tt = int16(tt)
	}
	return evalWithCollation(r, col), nil
}

// isCoalesceString reports whether tt is one of the string types that
// coalesceString converts from and to.
func isCoalesceString(tt sqltypes.Type) bool {
	switch tt {
	case sqltypes.VarChar, sqltypes.Char, sqltypes.VarBinary, sqltypes.Binary, sqltypes.Text, sqltypes.Blob:
		return true
	}
	return false
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
		ta.add(c.caseBranchType(arg, tt.Type), tt.Flag, tt.Size, tt.Scale)
		if err := ca.addTyped(tt.Type, tt.Col, c.env.CollationEnv()); err != nil {
			return ctype{}, err
		}
	}

	args := len(b.Arguments)
	tt, col := coalesceType(&ta, &ca, b.collate, c.env.MySQLVersion())
	size, scale, allowZero := ta.size, ta.scale, c.sqlmode.AllowZeroDate()
	c.asm.adjustStack(-(args - 1))
	c.asm.emit(func(env *ExpressionEnv) int {
		var sel eval
		for sp := env.vm.sp - args; sp < env.vm.sp; sp++ {
			if env.vm.stack[sp] != nil {
				sel = env.vm.stack[sp]
				break
			}
		}
		if sel != nil {
			env.vm.stack[env.vm.sp-args], env.vm.err = coalesceResult(sel, tt, size, scale, col, env.now, allowZero)
		} else {
			env.vm.stack[env.vm.sp-args] = nil
		}
		env.vm.sp -= args - 1
		return 1
	}, "COALESCE (SP-%d) ... (SP-1)", args)

	return ctype{Type: tt, Flag: f, Col: col, Size: size, Scale: scale}, nil
}

func (call *builtinMultiComparison) getMultiComparisonFunc(args []eval) multiComparisonFunc {
	var (
		integersI int
		integersU int
		floats    int
		decimals  int
		text      int
		binary    int
		temporal  int
		datetime  int
		timestamp int
		date      int
		time      int
		json      int
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
			return func(_ *ExpressionEnv, _ []eval, _, _ int) (eval, error) {
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
			call.prec = datetime2.DefaultPrecision
		case *evalDecimal:
			decimals++
			call.prec = max(call.prec, int(arg.length))
		case *evalBytes:
			switch arg.SQLType() {
			case sqltypes.Text, sqltypes.VarChar:
				text++
				call.prec = max(call.prec, datetime2.DefaultPrecision)
			case sqltypes.Blob, sqltypes.Binary, sqltypes.VarBinary:
				binary++
				if !arg.isHexOrBitLiteral() {
					call.prec = max(call.prec, datetime2.DefaultPrecision)
				}
			case sqltypes.Geometry:
				return func(_ *ExpressionEnv, _ []eval, _, _ int) (eval, error) {
					return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported argument of geometry type for GREATEST/LEAST")
				}
			}
		case *evalSet:
			text++
		case *evalEnum:
			text++
		case *evalTemporal:
			temporal++
			call.prec = max(call.prec, int(arg.prec))
			switch arg.SQLType() {
			case sqltypes.Datetime:
				datetime++
			case sqltypes.Timestamp:
				timestamp++
			case sqltypes.Date:
				date++
			case sqltypes.Time:
				time++
			}
		case *evalJSON:
			json++
		}
	}

	if temporal == len(args) {
		switch {
		case datetime > 0:
			return compareAllTemporal(func(env *ExpressionEnv, arg eval, prec int) *evalTemporal {
				return evalToDateTime(arg, prec, env.now, env.sqlmode.AllowZeroDate())
			})
		case timestamp > 0:
			return compareAllTemporal(func(env *ExpressionEnv, arg eval, prec int) *evalTemporal {
				return evalToTimestamp(arg, prec, env.now, env.sqlmode.AllowZeroDate())
			})
		case date > 0 && time > 0:
			// When all types are temporal, we convert the case
			// of having a date and time all to datetime.
			// This is contrary to the case where we have a non-temporal
			// type in the list, since MySQL doesn't do that.
			return compareAllTemporal(func(env *ExpressionEnv, arg eval, prec int) *evalTemporal {
				return evalToDateTime(arg, prec, env.now, env.sqlmode.AllowZeroDate())
			})
		case date > 0:
			return compareAllTemporal(func(env *ExpressionEnv, arg eval, _ int) *evalTemporal {
				return evalToDate(arg, env.now, env.sqlmode.AllowZeroDate())
			})
		case time > 0:
			return compareAllTemporal(func(_ *ExpressionEnv, arg eval, prec int) *evalTemporal {
				return evalToTime(arg, prec)
			})
		}
	}

	switch {
	case datetime > 0:
		return compareAllTemporalAsString(func(env *ExpressionEnv, arg eval, prec int) *evalTemporal {
			return evalToDateTime(arg, prec, env.now, env.sqlmode.AllowZeroDate())
		})
	case timestamp > 0:
		return compareAllTemporalAsString(func(env *ExpressionEnv, arg eval, prec int) *evalTemporal {
			return evalToTimestamp(arg, prec, env.now, env.sqlmode.AllowZeroDate())
		})
	case date > 0:
		return compareAllTemporalAsString(func(env *ExpressionEnv, arg eval, _ int) *evalTemporal {
			return evalToDate(arg, env.now, env.sqlmode.AllowZeroDate())
		})
	case time > 0:
		// So for time, there's actually no conversion and
		// internal comparisons as time. So we don't pass it
		// a conversion function.
		return compareAllTemporalAsString(nil)
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
		if json > 0 {
			return compareAllText
		}
	}
	panic("unexpected argument type")
}

func compareAllTemporal(f func(env *ExpressionEnv, arg eval, prec int) *evalTemporal) multiComparisonFunc {
	return func(env *ExpressionEnv, args []eval, cmp, prec int) (eval, error) {
		var x *evalTemporal
		for _, arg := range args {
			conv := f(env, arg, prec)
			if x == nil {
				x = conv
				continue
			}
			if (cmp < 0) == (conv.compare(x) < 0) {
				x = conv
			}
		}
		return x, nil
	}
}

func compareAllTemporalAsString(f func(env *ExpressionEnv, arg eval, prec int) *evalTemporal) multiComparisonFunc {
	return func(env *ExpressionEnv, args []eval, cmp, prec int) (eval, error) {
		validArgs := make([]*evalTemporal, 0, len(args))
		var ca collationAggregation
		for _, arg := range args {
			if err := ca.add(evalCollation(arg), env.collationEnv); err != nil {
				return nil, err
			}
			if f != nil {
				conv := f(env, arg, prec)
				validArgs = append(validArgs, conv)
			}
		}
		tc := ca.result()
		if tc.Coercibility == collations.CoerceNumeric {
			tc = typedCoercionCollation(sqltypes.VarChar, env.collationEnv.DefaultConnectionCharset())
		}
		if f != nil {
			idx := compareTemporalInternal(validArgs, cmp)
			if idx >= 0 {
				arg := args[idx]
				if _, ok := arg.(*evalTemporal); ok {
					arg = validArgs[idx]
				}
				return evalToVarchar(arg, tc.Collation, false)
			}
		}
		txt, err := compareAllText(env, args, cmp, prec)
		if err != nil {
			return nil, err
		}
		return evalToVarchar(txt, tc.Collation, false)
	}
}

func compareTemporalInternal(args []*evalTemporal, cmp int) int {
	if cmp < 0 {
		// If we have any failed conversions and want to have the smallest value,
		// we can't find that so we return -1 to indicate that.
		// This will result in a fallback to do a string comparison.
		for _, arg := range args {
			if arg == nil {
				return -1
			}
		}
	}

	x := 0
	for i, arg := range args[1:] {
		if arg == nil {
			continue
		}
		if (cmp < 0) == (compareTemporal(args, i+1, x) < 0) {
			x = i + 1
		}
	}
	return x
}

func compareTemporal(args []*evalTemporal, idx1, idx2 int) int {
	if idx1 < 0 {
		return 1
	}
	if idx2 < 0 {
		return -1
	}
	return args[idx1].compare(args[idx2])
}

func compareAllInteger_u(_ *ExpressionEnv, args []eval, cmp, _ int) (eval, error) {
	x := args[0].(*evalUint64)
	for _, arg := range args[1:] {
		y := arg.(*evalUint64)
		if (cmp < 0) == (y.u < x.u) {
			x = y
		}
	}
	return x, nil
}

func compareAllInteger_i(_ *ExpressionEnv, args []eval, cmp, _ int) (eval, error) {
	x := args[0].(*evalInt64)
	for _, arg := range args[1:] {
		y := arg.(*evalInt64)
		if (cmp < 0) == (y.i < x.i) {
			x = y
		}
	}
	return x, nil
}

func compareAllFloat(_ *ExpressionEnv, args []eval, cmp, _ int) (eval, error) {
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

func compareAllDecimal(_ *ExpressionEnv, args []eval, cmp, _ int) (eval, error) {
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

func compareAllText(env *ExpressionEnv, args []eval, cmp, _ int) (eval, error) {
	charsets := make([]charset.Charset, 0, len(args))
	var ca collationAggregation
	for _, arg := range args {
		col := evalCollation(arg)
		if err := ca.add(col, env.collationEnv); err != nil {
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

func compareAllBinary(_ *ExpressionEnv, args []eval, cmp, _ int) (eval, error) {
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
	res, err := call.getMultiComparisonFunc(args)(env, args, call.cmp, call.prec)
	return evalPlain(res), err
}

func (call *builtinMultiComparison) compile_c(c *compiler, args []ctype, jumps []*jump) (ctype, error) {
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
	c.asm.jumpDestination(jumps...)
	return ctype{Type: sqltypes.VarChar, Flag: f, Col: tc}, nil
}

func (call *builtinMultiComparison) compile_d(c *compiler, args []ctype, jumps []*jump) (ctype, error) {
	var f typeFlag
	var size int32
	var scale int32
	for i, tt := range args {
		f |= nullableFlags(tt.Flag)
		size = max(size, tt.Size)
		scale = max(scale, tt.Scale)
		c.compileToDecimal(tt, len(args)-i)
	}
	c.asm.Fn_MULTICMP_d(len(args), call.cmp < 0)
	c.asm.jumpDestination(jumps...)
	return ctype{Type: sqltypes.Decimal, Flag: f, Col: collationNumeric, Size: size, Scale: scale}, nil
}

func (call *builtinMultiComparison) compile(c *compiler) (ctype, error) {
	var (
		signed    int
		unsigned  int
		floats    int
		decimals  int
		temporal  int
		date      int
		datetime  int
		timestamp int
		time      int
		text      int
		binary    int
		json      int
		args      []ctype
		nullable  bool
		prec      int
	)

	/*
		If any argument is NULL, the result is NULL. No comparison is needed.
		If all arguments are integer-valued, they are compared as integers.
		If at least one argument is double precision, they are compared as double-precision values. Otherwise, if at least one argument is a DECIMAL value, they are compared as DECIMAL values.
		If the arguments comprise a mix of numbers and strings, they are compared as strings.
		If any argument is a nonbinary (character) string, the arguments are compared as nonbinary strings.
		In all other cases, the arguments are compared as binary strings.
	*/

	jumps := make([]*jump, 0, len(call.Arguments))
	for i, expr := range call.Arguments {
		tt, err := expr.compile(c)
		if err != nil {
			return ctype{}, err
		}

		if tt.nullable() {
			jumps = append(jumps, c.compileNullCheckArg(tt, i))
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
			prec = max(prec, datetime2.DefaultPrecision)
		case sqltypes.Decimal:
			decimals++
			prec = max(prec, int(tt.Scale))
		case sqltypes.Text, sqltypes.VarChar:
			text++
			prec = max(prec, datetime2.DefaultPrecision)
		case sqltypes.Blob, sqltypes.Binary, sqltypes.VarBinary:
			binary++
			if !tt.isHexOrBitLiteral() {
				prec = max(prec, datetime2.DefaultPrecision)
			}
		case sqltypes.Date:
			temporal++
			date++
			prec = max(prec, int(tt.Size))
		case sqltypes.Datetime:
			temporal++
			datetime++
			prec = max(prec, int(tt.Size))
		case sqltypes.Timestamp:
			temporal++
			timestamp++
			prec = max(prec, int(tt.Size))
		case sqltypes.Time:
			temporal++
			time++
			prec = max(prec, int(tt.Size))
		case sqltypes.Set, sqltypes.Enum:
			text++
		case sqltypes.TypeJSON:
			json++
		case sqltypes.Geometry:
			return ctype{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported argument of geometry type for GREATEST/LEAST")
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
	if temporal == len(args) {
		var typ sqltypes.Type
		switch {
		case datetime > 0:
			typ = sqltypes.Datetime
		case timestamp > 0:
			typ = sqltypes.Timestamp
		case date > 0 && time > 0:
			// When all types are temporal, we convert the case
			// of having a date and time all to datetime.
			// This is contrary to the case where we have a non-temporal
			// type in the list, since MySQL doesn't do that.
			typ = sqltypes.Datetime
		case date > 0:
			typ = sqltypes.Date
		case time > 0:
			typ = sqltypes.Time
		}
		for i, tt := range args {
			if tt.Type != typ || int(tt.Size) != prec {
				c.compileToTemporal(tt, typ, len(args)-i, prec)
			}
		}
		c.asm.Fn_MULTICMP_temporal(len(args), call.cmp < 0)
		c.asm.jumpDestination(jumps...)
		return ctype{Type: typ, Flag: f, Col: collationBinary}, nil
	} else if temporal > 0 {
		var ca collationAggregation
		for _, arg := range args {
			if err := ca.add(arg.Col, c.env.CollationEnv()); err != nil {
				return ctype{}, err
			}
		}

		tc := ca.result()
		if tc.Coercibility == collations.CoerceNumeric {
			tc = typedCoercionCollation(sqltypes.VarChar, c.collation)
		}
		switch {
		case datetime > 0:
			c.asm.Fn_MULTICMP_temporal_fallback(compareAllTemporalAsString(func(env *ExpressionEnv, arg eval, prec int) *evalTemporal {
				return evalToDateTime(arg, prec, env.now, env.sqlmode.AllowZeroDate())
			}), len(args), call.cmp, prec)
		case timestamp > 0:
			c.asm.Fn_MULTICMP_temporal_fallback(compareAllTemporalAsString(func(env *ExpressionEnv, arg eval, prec int) *evalTemporal {
				return evalToTimestamp(arg, prec, env.now, env.sqlmode.AllowZeroDate())
			}), len(args), call.cmp, prec)
		case date > 0:
			c.asm.Fn_MULTICMP_temporal_fallback(compareAllTemporalAsString(func(env *ExpressionEnv, arg eval, prec int) *evalTemporal {
				return evalToDate(arg, env.now, env.sqlmode.AllowZeroDate())
			}), len(args), call.cmp, prec)
		case time > 0:
			c.asm.Fn_MULTICMP_temporal_fallback(compareAllTemporalAsString(nil), len(args), call.cmp, prec)
		}
		c.asm.jumpDestination(jumps...)
		return ctype{Type: sqltypes.VarChar, Flag: f, Col: tc}, nil
	}
	if signed+unsigned == len(args) {
		if signed == len(args) {
			c.asm.Fn_MULTICMP_i(len(args), call.cmp < 0)
			c.asm.jumpDestination(jumps...)
			return ctype{Type: sqltypes.Int64, Flag: f, Col: collationNumeric}, nil
		}
		if unsigned == len(args) {
			c.asm.Fn_MULTICMP_u(len(args), call.cmp < 0)
			c.asm.jumpDestination(jumps...)
			return ctype{Type: sqltypes.Uint64, Flag: f, Col: collationNumeric}, nil
		}
		return call.compile_d(c, args, jumps)
	}
	if binary > 0 || text > 0 {
		if text > 0 {
			return call.compile_c(c, args, jumps)
		}
		c.asm.Fn_MULTICMP_b(len(args), call.cmp < 0)
		c.asm.jumpDestination(jumps...)
		return ctype{Type: sqltypes.VarBinary, Flag: f, Col: collationBinary}, nil
	} else {
		if floats > 0 {
			for i, tt := range args {
				c.compileToFloat(tt, len(args)-i)
			}
			c.asm.Fn_MULTICMP_f(len(args), call.cmp < 0)
			c.asm.jumpDestination(jumps...)
			return ctype{Type: sqltypes.Float64, Flag: f, Col: collationNumeric}, nil
		}
		if decimals > 0 {
			return call.compile_d(c, args, jumps)
		}
		if json > 0 {
			c.asm.Fn_MULTICMP_c(len(args), call.cmp < 0, collationJSON)
			c.asm.jumpDestination(jumps...)
			return ctype{Type: sqltypes.Text, Flag: f, Col: collationJSON}, nil
		}

		// The next case only gets hit if we already know at least one of the inputs
		// is a static NULL typed value. That means we already have removed all items
		// from the stack at this point and the top is a NULL to return.
		if nullable {
			c.asm.adjustStack(-len(args) + 1)
			c.asm.jumpDestination(jumps...)
			return ctype{Type: sqltypes.Null, Flag: flagNull | flagNullable, Col: collationBinary}, nil
		}
	}
	return ctype{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected argument for GREATEST/LEAST")
}
