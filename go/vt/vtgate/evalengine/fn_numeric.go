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
	"math"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type builtinCeil struct {
	CallExpr
}

var _ Expr = (*builtinCeil)(nil)

func (call *builtinCeil) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	switch num := arg.(type) {
	case *evalInt64, *evalUint64:
		return num, nil
	case *evalDecimal:
		dec := num.dec
		dec = dec.Ceil()
		intnum, isfit := dec.Int64()
		if isfit {
			return newEvalInt64(intnum), nil
		}
		return newEvalDecimalWithPrec(dec, 0), nil
	default:
		f, _ := evalToNumeric(num).toFloat()
		return newEvalFloat(math.Ceil(f.f)), nil
	}
}

func (call *builtinCeil) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	t, f := call.Arguments[0].typeof(env, fields)
	if sqltypes.IsSigned(t) {
		return sqltypes.Int64, f
	} else if sqltypes.IsUnsigned(t) {
		return sqltypes.Uint64, f
	} else if sqltypes.Decimal == t {
		return sqltypes.Int64, f | flagAmbiguousType
	} else {
		return sqltypes.Float64, f
	}
}

type builtinFloor struct {
	CallExpr
}

var _ Expr = (*builtinFloor)(nil)

func (call *builtinFloor) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	switch num := arg.(type) {
	case *evalInt64, *evalUint64:
		return num, nil
	case *evalDecimal:
		dec := num.dec
		dec = dec.Floor()
		intnum, isfit := dec.Int64()
		if isfit {
			return newEvalInt64(intnum), nil
		}
		return newEvalDecimalWithPrec(dec, 0), nil
	default:
		f, _ := evalToNumeric(num).toFloat()
		return newEvalFloat(math.Floor(f.f)), nil
	}
}

func (call *builtinFloor) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	t, f := call.Arguments[0].typeof(env, fields)
	if sqltypes.IsSigned(t) {
		return sqltypes.Int64, f
	} else if sqltypes.IsUnsigned(t) {
		return sqltypes.Uint64, f
	} else if sqltypes.Decimal == t {
		return sqltypes.Int64, f | flagAmbiguousType
	} else {
		return sqltypes.Float64, f
	}
}

type builtinAbs struct {
	CallExpr
}

var _ Expr = (*builtinAbs)(nil)

func (call *builtinAbs) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	switch num := arg.(type) {
	case *evalUint64:
		return num, nil
	case *evalInt64:
		if num.i < 0 {
			if num.i == math.MinInt64 {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "BIGINT value is out of range")
			}
			return newEvalInt64(-num.i), nil
		}
		return num, nil
	case *evalDecimal:
		return newEvalDecimalWithPrec(num.dec.Abs(), num.length), nil
	default:
		f, _ := evalToNumeric(num).toFloat()
		return newEvalFloat(math.Abs(f.f)), nil
	}
}

func (call *builtinAbs) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	t, f := call.Arguments[0].typeof(env, fields)
	if sqltypes.IsNumber(t) {
		return t, f
	} else {
		return sqltypes.Float64, f
	}
}

type builtinPi struct {
	CallExpr
}

var _ Expr = (*builtinPi)(nil)

func (call *builtinPi) eval(env *ExpressionEnv) (eval, error) {
	return newEvalFloat(math.Pi), nil
}

func (call *builtinPi) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Float64, 0
}

type builtinAcos struct {
	CallExpr
}

var _ Expr = (*builtinAcos)(nil)

func (call *builtinAcos) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	if f.f < -1 || f.f > 1 {
		return nil, nil
	}
	return newEvalFloat(math.Acos(f.f)), nil
}

func (call *builtinAcos) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinAsin struct {
	CallExpr
}

var _ Expr = (*builtinAsin)(nil)

func (call *builtinAsin) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	if f.f < -1 || f.f > 1 {
		return nil, nil
	}
	return newEvalFloat(math.Asin(f.f)), nil
}

func (call *builtinAsin) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f | flagNullable
}

type builtinAtan struct {
	CallExpr
}

var _ Expr = (*builtinAtan)(nil)

func (call *builtinAtan) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(math.Atan(f.f)), nil
}

func (call *builtinAtan) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinAtan2 struct {
	CallExpr
}

var _ Expr = (*builtinAtan2)(nil)

func (call *builtinAtan2) eval(env *ExpressionEnv) (eval, error) {
	arg1, arg2, err := call.arg2(env)
	if err != nil {
		return nil, err
	}
	if arg1 == nil || arg2 == nil {
		return nil, nil
	}

	f1, _ := evalToNumeric(arg1).toFloat()
	f2, _ := evalToNumeric(arg2).toFloat()
	return newEvalFloat(math.Atan2(f1.f, f2.f)), nil
}

func (call *builtinAtan2) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinCos struct {
	CallExpr
}

var _ Expr = (*builtinCos)(nil)

func (call *builtinCos) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(math.Cos(f.f)), nil
}

func (call *builtinCos) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinCot struct {
	CallExpr
}

var _ Expr = (*builtinCot)(nil)

func (call *builtinCot) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(1.0 / math.Tan(f.f)), nil
}

func (call *builtinCot) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinSin struct {
	CallExpr
}

var _ Expr = (*builtinSin)(nil)

func (call *builtinSin) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(math.Sin(f.f)), nil
}

func (call *builtinSin) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinTan struct {
	CallExpr
}

var _ Expr = (*builtinTan)(nil)

func (call *builtinTan) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(math.Tan(f.f)), nil
}

func (call *builtinTan) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinDegrees struct {
	CallExpr
}

var _ Expr = (*builtinDegrees)(nil)

func (call *builtinDegrees) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(f.f * (180 / math.Pi)), nil
}

func (call *builtinDegrees) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}

type builtinRadians struct {
	CallExpr
}

var _ Expr = (*builtinRadians)(nil)

func (call *builtinRadians) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	f, _ := evalToNumeric(arg).toFloat()
	return newEvalFloat(f.f * (math.Pi / 180)), nil
}

func (call *builtinRadians) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Float64, f
}
