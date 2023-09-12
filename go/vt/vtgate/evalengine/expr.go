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
	// Expr is the interface that all evaluating expressions must implement
	Expr interface {
		eval(env *ExpressionEnv) (eval, error)
		typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag)
		format(buf *formatter, depth int)
		constant() bool
		simplify(env *ExpressionEnv) error
		compile(c *compiler) (ctype, error)
	}

	UnaryExpr struct {
		Inner Expr
	}

	BinaryExpr struct {
		Left, Right Expr
	}
)

func (expr *BinaryExpr) LeftExpr() Expr {
	return expr.Left
}

func (expr *BinaryExpr) RightExpr() Expr {
	return expr.Right
}

func (expr *BinaryExpr) arguments(env *ExpressionEnv) (eval, eval, error) {
	left, err := expr.Left.eval(env)
	if err != nil {
		return nil, nil, err
	}
	right, err := expr.Right.eval(env)
	if err != nil {
		return nil, nil, err
	}
	return left, right, nil
}
