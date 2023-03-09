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

func (call *builtinCeil) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	t, f := call.Arguments[0].typeof(env)
	if sqltypes.IsIntegral(t) {
		return sqltypes.Int64, f
	} else if sqltypes.Decimal == t {
		return sqltypes.Decimal, f
	} else {
		return sqltypes.Float64, f
	}
}
