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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	builtinChangeCase struct {
		CallExpr
		upcase bool
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
)

var _ Expr = (*builtinChangeCase)(nil)
var _ Expr = (*builtinCharLength)(nil)
var _ Expr = (*builtinLength)(nil)
var _ Expr = (*builtinASCII)(nil)
var _ Expr = (*builtinBitLength)(nil)
var _ Expr = (*builtinCollation)(nil)
var _ Expr = (*builtinWeightString)(nil)

func (call *builtinChangeCase) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}

	switch e := arg.(type) {
	case nil:
		return nil, nil

	case evalNumeric:
		return evalToVarchar(e, env.DefaultCollation, false)

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

func (call *builtinChangeCase) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.VarChar, f
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

func (call *builtinCharLength) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Int64, f
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

func (call *builtinLength) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Int64, f
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

func (call *builtinBitLength) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Int64, f
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

func (call *builtinASCII) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Int64, f
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
		text, err = evalToVarchar(arg1, env.DefaultCollation, true)
		if err != nil {
			return nil, err
		}
	}

	repeat := evalToNumeric(arg2).toInt64().i
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

func (call *builtinRepeat) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f1 := call.Arguments[0].typeof(env)
	// typecheck the right-hand argument but ignore its flags
	call.Arguments[1].typeof(env)
	return sqltypes.VarChar, f1
}

func (c *builtinCollation) eval(env *ExpressionEnv) (eval, error) {
	arg, err := c.arg1(env)
	if err != nil {
		return nil, err
	}

	col := evalCollation(arg).Collation.Get()

	// the collation of a `COLLATION` expr is hardcoded to `utf8_general_ci`,
	// not to the default collation of our connection. this is probably a bug in MySQL, but we match it
	return newEvalText([]byte(col.Name()), collations.TypedCollation{
		Collation:    collations.CollationUtf8ID,
		Coercibility: collations.CoerceImplicit,
		Repertoire:   collations.RepertoireASCII,
	}), nil
}

func (*builtinCollation) typeof(_ *ExpressionEnv) (sqltypes.Type, typeFlag) {
	return sqltypes.VarChar, 0
}

func (c *builtinWeightString) callable() []Expr {
	return []Expr{c.String}
}

func (c *builtinWeightString) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := c.String.typeof(env)
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
