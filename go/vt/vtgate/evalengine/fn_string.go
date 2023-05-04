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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	builtinChangeCase struct {
		CallExpr
		upcase  bool
		collate collations.ID
	}

	builtinCharLength struct {
		CallExpr
	}

	builtinLength struct {
		CallExpr
	}

	builtinASCII struct {
		CallExpr
	}

	builtinBitLength struct {
		CallExpr
	}

	builtinCollation struct {
		CallExpr
	}

	builtinWeightString struct {
		String Expr
		Cast   string
		Len    int
		HasLen bool
	}

	builtinLeftRight struct {
		CallExpr
		collate collations.ID
		left    bool
	}

	builtinPad struct {
		CallExpr
		collate collations.ID
		left    bool
	}
)

var _ Expr = (*builtinChangeCase)(nil)
var _ Expr = (*builtinCharLength)(nil)
var _ Expr = (*builtinLength)(nil)
var _ Expr = (*builtinASCII)(nil)
var _ Expr = (*builtinBitLength)(nil)
var _ Expr = (*builtinCollation)(nil)
var _ Expr = (*builtinWeightString)(nil)
var _ Expr = (*builtinLeftRight)(nil)
var _ Expr = (*builtinPad)(nil)

func (call *builtinChangeCase) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}

	switch e := arg.(type) {
	case nil:
		return nil, nil

	case evalNumeric:
		return evalToVarchar(e, call.collate, false)

	case *evalBytes:
		coll := e.col.Collation.Get()
		csa, ok := coll.(collations.CaseAwareCollation)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
		}
		var newcase []byte
		if call.upcase {
			newcase = csa.ToUpper(nil, e.bytes)
		} else {
			newcase = csa.ToLower(nil, e.bytes)
		}
		return newEvalText(newcase, e.col), nil

	default:
		return e, nil
	}
}

func (call *builtinChangeCase) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarChar, f
}

func (call *builtinChangeCase) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, c.cfg.Collation, 0, false)
	}

	c.asm.Fn_LUCASE(call.upcase)
	c.asm.jumpDestination(skip)

	return str, nil
}

func (call *builtinCharLength) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	switch e := arg.(type) {
	case nil:
		return nil, nil
	case *evalBytes:
		if sqltypes.IsBinary(e.SQLType()) {
			return newEvalInt64(int64(len(e.bytes))), nil
		}
		coll := e.col.Collation.Get()
		count := charset.Length(coll.Charset(), e.bytes)
		return newEvalInt64(int64(count)), nil
	default:
		return newEvalInt64(int64(len(e.ToRawBytes()))), nil
	}
}

func (call *builtinCharLength) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, f
}

func (call *builtinCharLength) compile(c *compiler) (ctype, error) {
	return c.compileFn_length(call.Arguments[0], c.asm.Fn_CHAR_LENGTH)
}

func (call *builtinLength) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}
	return newEvalInt64(int64(len(arg.ToRawBytes()))), nil
}

func (call *builtinLength) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, f
}

func (call *builtinLength) compile(c *compiler) (ctype, error) {
	return c.compileFn_length(call.Arguments[0], c.asm.Fn_LENGTH)
}

func (call *builtinBitLength) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}
	return newEvalInt64(int64(len(arg.ToRawBytes())) * 8), nil
}

func (call *builtinBitLength) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, f
}

func (call *builtinBitLength) compile(c *compiler) (ctype, error) {
	return c.compileFn_length(call.Arguments[0], c.asm.Fn_BIT_LENGTH)
}

func (call *builtinASCII) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	b, ok := arg.(*evalBytes)
	if !ok {
		b = evalToBinary(arg)
	}
	if len(b.bytes) == 0 {
		return newEvalInt64(0), nil
	}
	return newEvalInt64(int64(b.bytes[0])), nil
}

func (call *builtinASCII) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, f
}

func (call *builtinASCII) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, c.cfg.Collation, 0, false)
	}

	c.asm.Fn_ASCII()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
}

// maxRepeatLength is the maximum number of times a string can be repeated.
// This is based on how MySQL behaves here. The maximum value in MySQL is
// actually based on `max_allowed_packet`. The value here is the maximum
// for `max_allowed_packet` with 1GB.
// See https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet
//
// Practically though, this is really whacky anyway.
// There's 3 possible states:
//   - `<= max_allowed_packet` and actual packet  generated is `<= max_allowed_packet`. It works
//   - `<= max_allowed_packet` but the actual packet generated is `> max_allowed_packet` so it fails with an
//     error: `ERROR 2020 (HY000): Got packet bigger than 'max_allowed_packet' bytes` and the client gets disconnected.
//   - `> max_allowed_packet`, no error and returns `NULL`.
const maxRepeatLength = 1073741824

type builtinRepeat struct {
	CallExpr
	collate collations.ID
}

func (call *builtinRepeat) eval(env *ExpressionEnv) (eval, error) {
	arg1, arg2, err := call.arg2(env)
	if err != nil {
		return nil, err
	}
	if arg1 == nil || arg2 == nil {
		return nil, nil
	}

	text, ok := arg1.(*evalBytes)
	if !ok {
		text, err = evalToVarchar(arg1, call.collate, true)
		if err != nil {
			return nil, err
		}
	}

	repeat := evalToInt64(arg2).i
	if repeat < 0 {
		repeat = 0
	}
	if !checkMaxLength(int64(len(text.bytes)), repeat) {
		return nil, nil
	}

	return newEvalText(bytes.Repeat(text.bytes, int(repeat)), text.col), nil
}

func checkMaxLength(len, repeat int64) bool {
	if repeat <= 0 {
		return true
	}
	if len*repeat/repeat != len {
		// we have an overflow, can't be a valid length.
		return false
	}
	return len*repeat <= maxRepeatLength
}

func (call *builtinRepeat) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := call.Arguments[0].typeof(env, fields)
	// typecheck the right-hand argument but ignore its flags
	call.Arguments[1].typeof(env, fields)
	return sqltypes.VarChar, f1
}

func (expr *builtinRepeat) compile(c *compiler) (ctype, error) {
	str, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	repeat, err := expr.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck2(str, repeat)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xc(2, sqltypes.VarChar, c.cfg.Collation, 0, false)
	}
	_ = c.compileToInt64(repeat, 1)

	c.asm.Fn_REPEAT()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: str.Col, Flag: flagNullable}, nil
}

func (c *builtinCollation) eval(env *ExpressionEnv) (eval, error) {
	arg, err := c.arg1(env)
	if err != nil {
		return nil, err
	}

	col := evalCollation(arg).Collation.Get()

	// the collation of a `COLLATION` expr is hardcoded to `utf8_general_ci`,
	// not to the default collation of our connection. this is probably a bug in MySQL, but we match it
	return newEvalText([]byte(col.Name()), collationUtf8mb3), nil
}

func (*builtinCollation) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.VarChar, 0
}

func (expr *builtinCollation) compile(c *compiler) (ctype, error) {
	_, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.asm.jumpFrom()

	c.asm.Fn_COLLATION(collationUtf8mb3)
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarChar, Col: collationUtf8mb3}, nil
}

func (c *builtinWeightString) callable() []Expr {
	return []Expr{c.String}
}

func (c *builtinWeightString) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := c.String.typeof(env, fields)
	return sqltypes.VarBinary, f
}

func (c *builtinWeightString) eval(env *ExpressionEnv) (eval, error) {
	var (
		tc      collations.TypedCollation
		text    []byte
		weights []byte
		length  = c.Len
	)

	str, err := c.String.eval(env)
	if err != nil {
		return nil, err
	}

	switch str := str.(type) {
	case *evalInt64, *evalUint64:
		// when calling WEIGHT_STRING with an integral value, MySQL returns the
		// internal sort key that would be used in an InnoDB table... we do not
		// support that
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %s", ErrEvaluatedExprNotSupported, FormatExpr(c))
	case *evalBytes:
		text = str.bytes
		tc = str.col
	default:
		return nil, nil
	}

	if c.Cast == "binary" {
		tc = collationBinary
		weights = make([]byte, 0, c.Len)
		length = collations.PadToMax
	}

	collation := tc.Collation.Get()
	weights = collation.WeightString(weights, text, length)
	return newEvalBinary(weights), nil
}

func (call *builtinWeightString) compile(c *compiler) (ctype, error) {
	str, err := call.String.compile(c)
	if err != nil {
		return ctype{}, err
	}

	switch str.Type {
	case sqltypes.Int64, sqltypes.Uint64:
		return ctype{}, c.unsupported(call)

	case sqltypes.VarChar, sqltypes.VarBinary:
		skip := c.compileNullCheck1(str)

		if call.Cast == "binary" {
			c.asm.Fn_WEIGHT_STRING_b(call.Len)
		} else {
			c.asm.Fn_WEIGHT_STRING_c(str.Col.Collation.Get(), call.Len)
		}
		c.asm.jumpDestination(skip)
		return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil

	default:
		c.asm.SetNull(1)
		return ctype{Type: sqltypes.VarBinary, Flag: flagNullable | flagNull, Col: collationBinary}, nil
	}
}

func (call builtinLeftRight) eval(env *ExpressionEnv) (eval, error) {
	str, l, err := call.arg2(env)
	if err != nil {
		return nil, err
	}
	if str == nil || l == nil {
		return nil, nil
	}

	text, ok := str.(*evalBytes)
	if !ok {
		text, err = evalToVarchar(str, call.collate, true)
		if err != nil {
			return nil, err
		}
	}

	length := evalToInt64(l).i
	if length <= 0 {
		return newEvalText(nil, text.col), nil
	}

	cs := text.col.Collation.Get().Charset()
	strLen := charset.Length(cs, text.bytes)

	if strLen <= int(length) {
		return newEvalText(text.bytes, text.col), nil
	}

	var res []byte
	if call.left {
		res = charset.Slice(cs, text.bytes, 0, int(length))
	} else {
		res = charset.Slice(cs, text.bytes, strLen-int(length), strLen)
	}
	return newEvalText(res, text.col), nil
}

func (call builtinLeftRight) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarChar, f1
}

func (call builtinLeftRight) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	l, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck2(str, l)

	col := defaultCoercionCollation(c.cfg.Collation)
	switch {
	case str.isTextual():
		col = str.Col
	default:
		c.asm.Convert_xc(2, sqltypes.VarChar, col.Collation, 0, false)
	}
	_ = c.compileToInt64(l, 1)

	if call.left {
		c.asm.Fn_LEFT(col)
	} else {
		c.asm.Fn_RIGHT(col)
	}
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: flagNullable}, nil
}

func (call builtinPad) eval(env *ExpressionEnv) (eval, error) {
	str, l, p, err := call.arg3(env)
	if err != nil {
		return nil, err
	}

	if str == nil || l == nil || p == nil {
		return nil, nil
	}

	text, ok := str.(*evalBytes)
	if !ok {
		text, err = evalToVarchar(str, call.collate, true)
		if err != nil {
			return nil, err
		}
	}

	length := evalToInt64(l).i
	if length < 0 {
		return nil, nil
	}

	pad, ok := p.(*evalBytes)
	if !ok {
		pad, err = evalToVarchar(p, call.collate, true)
		if err != nil {
			return nil, err
		}
	}

	if !checkMaxLength(int64(len(pad.bytes)), length) {
		return nil, nil
	}

	cs := text.col.Collation.Get().Charset()
	strLen := charset.Length(cs, text.bytes)

	if strLen >= int(length) {
		return newEvalText(charset.Slice(cs, text.bytes, 0, int(length)), text.col), nil
	}

	runeLen := charset.Length(cs, pad.bytes)
	if runeLen == 0 {
		return newEvalText(nil, text.col), nil
	}

	repeat := (int(length) - strLen) / runeLen
	remainder := (int(length) - strLen) % runeLen

	var res []byte
	if !call.left {
		res = text.bytes
	}

	res = append(res, bytes.Repeat(pad.bytes, repeat)...)
	if remainder > 0 {
		res = append(res, charset.Slice(cs, pad.bytes, 0, remainder)...)
	}

	if call.left {
		res = append(res, text.bytes...)
	}

	return newEvalText(res, text.col), nil
}

func (call builtinPad) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarChar, f1
}

func (call builtinPad) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	l, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	pad, err := call.Arguments[2].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck3(str, l, pad)

	col := defaultCoercionCollation(c.cfg.Collation)
	switch {
	case str.isTextual():
		col = str.Col
	default:
		c.asm.Convert_xc(3, sqltypes.VarChar, col.Collation, 0, false)
	}
	_ = c.compileToInt64(l, 2)

	switch {
	case pad.isTextual():
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, col.Collation, 0, false)
	}

	if call.left {
		c.asm.Fn_LPAD(col)
	} else {
		c.asm.Fn_RPAD(col)
	}
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: col}, nil
}
