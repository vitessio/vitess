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
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	TupleExpr []Expr
)

var _ Expr = (TupleExpr)(nil)

func (t TupleExpr) eval(env *ExpressionEnv) (eval, error) {
	tup := make([]eval, 0, len(t))
	for _, expr := range t {
		e, err := expr.eval(env)
		if err != nil {
			return nil, err
		}
		tup = append(tup, e)
	}
	return &evalTuple{t: tup}, nil
}

// typeof implements the Expr interface
func (t TupleExpr) typeof(*ExpressionEnv, []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Tuple, flagNullable
}

func (tuple TupleExpr) compile(c *compiler) (ctype, error) {
	for _, arg := range tuple {
		_, err := arg.compile(c)
		if err != nil {
			return ctype{}, err
		}
	}
	c.asm.PackTuple(len(tuple))
	return ctype{Type: sqltypes.Tuple, Col: collationBinary}, nil
}
