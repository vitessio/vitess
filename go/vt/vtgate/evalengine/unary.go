/*
Copyright 2021 The Vitess Authors.

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

import "vitess.io/vitess/go/sqltypes"

type (
	UnaryExpr struct {
		Inner Expr
	}

	NegateExpr struct {
		UnaryExpr
	}
)

func (n *NegateExpr) eval(env *ExpressionEnv, result *EvalResult) {
	result.init(env, n.Inner)
	result.negateNumeric()
}

func (n *NegateExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	tt, f := n.Inner.typeof(env)
	switch tt {
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		if f&flagIntegerOvf != 0 {
			return sqltypes.Decimal, f & ^flagIntegerRange
		}
		if f&flagIntegerCap != 0 {
			return sqltypes.Int64, (f | flagIntegerUdf) & ^flagIntegerCap
		}
		return sqltypes.Int64, f
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
		if f&flagIntegerUdf != 0 {
			return sqltypes.Decimal, f & ^flagIntegerRange
		}
		return sqltypes.Int64, f
	case sqltypes.Decimal:
		return sqltypes.Decimal, f
	}
	return sqltypes.Float64, f
}
