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
	"math/bits"

	"vitess.io/vitess/go/sqltypes"
)

type builtinBitCount struct {
	CallExpr
}

var _ Expr = (*builtinBitCount)(nil)

func (call *builtinBitCount) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	var count int
	if b, ok := arg.(*evalBytes); ok && b.isBinary() && !b.isHexOrBitLiteral() {
		binary := b.bytes
		for _, b := range binary {
			count += bits.OnesCount8(b)
		}
	} else {
		u := evalToNumeric(arg).toUint64()
		count = bits.OnesCount64(u.u)
	}
	return newEvalInt64(int64(count)), nil
}

func (call *builtinBitCount) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.Int64, f
}
