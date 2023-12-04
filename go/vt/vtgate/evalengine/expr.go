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
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// Expr is a compiled expression that can be evaluated and serialized back to SQL.
	Expr interface {
		sqlparser.Expr
		IR() IR
		eval(env *ExpressionEnv) (eval, error)
		typeof(env *ExpressionEnv) (ctype, error)
	}

	// IR is the interface that all evaluation nodes must implement. It can only be used to
	// match the AST of the compiled expression for planning purposes. IR objects cannot be
	// evaluated directly.
	IR interface {
		eval(env *ExpressionEnv) (eval, error)
		format(buf *sqlparser.TrackedBuffer)
		constant() bool
		simplify(env *ExpressionEnv) error
		compile(c *compiler) (ctype, error)
	}

	UnaryExpr struct {
		Inner IR
	}

	BinaryExpr struct {
		Left, Right IR
	}
)

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
