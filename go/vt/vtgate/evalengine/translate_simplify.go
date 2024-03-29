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

import "vitess.io/vitess/go/mysql/collations/colldata"

func (expr *Literal) constant() bool {
	return true
}

func (expr *BindVariable) constant() bool {
	return false
}

func (expr *TupleBindVariable) constant() bool {
	return false
}

func (expr *Column) constant() bool {
	return false
}

func (expr *BinaryExpr) constant() bool {
	return expr.Left.constant() && expr.Right.constant()
}

func (tuple TupleExpr) constant() bool {
	for _, subexpr := range tuple {
		if !subexpr.constant() {
			return false
		}
	}
	return true
}

func (expr *UnaryExpr) constant() bool {
	return expr.Inner.constant()
}

func (expr *Literal) simplify(_ *ExpressionEnv) error {
	return nil
}

func (expr *BindVariable) simplify(_ *ExpressionEnv) error {
	return nil
}

func (expr *TupleBindVariable) simplify(_ *ExpressionEnv) error {
	return nil
}

func (expr *Column) simplify(_ *ExpressionEnv) error {
	return nil
}

func (expr *BinaryExpr) simplify(env *ExpressionEnv) error {
	var err error
	expr.Left, err = simplifyExpr(env, expr.Left)
	if err != nil {
		return err
	}
	expr.Right, err = simplifyExpr(env, expr.Right)
	if err != nil {
		return err
	}
	return nil
}

func (expr *LikeExpr) simplify(env *ExpressionEnv) error {
	if err := expr.BinaryExpr.simplify(env); err != nil {
		return err
	}

	if lit, ok := expr.Right.(*Literal); ok {
		if b, ok := lit.inner.(*evalBytes); ok && (b.isVarChar() || b.isBinary()) {
			expr.MatchCollation = b.col.Collation
			coll := colldata.Lookup(expr.MatchCollation)
			expr.Match = coll.Wildcard(b.bytes, 0, 0, 0)
		}
	}
	return nil
}

func (inexpr *InExpr) simplify(env *ExpressionEnv) error {
	if err := inexpr.BinaryExpr.simplify(env); err != nil {
		return err
	}

	if err := inexpr.Right.simplify(env); err != nil {
		return err
	}

	return nil
}

func (tuple TupleExpr) simplify(env *ExpressionEnv) error {
	var err error
	for i, subexpr := range tuple {
		tuple[i], err = simplifyExpr(env, subexpr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (expr *UnaryExpr) simplify(env *ExpressionEnv) error {
	var err error
	expr.Inner, err = simplifyExpr(env, expr.Inner)
	return err
}

func (c *CallExpr) constant() bool {
	return c.Arguments.constant()
}

func (c *CallExpr) simplify(env *ExpressionEnv) error {
	return c.Arguments.simplify(env)
}

func simplifyExpr(env *ExpressionEnv, e IR) (IR, error) {
	if e.constant() {
		simplified, err := e.eval(env)
		if err != nil {
			return nil, err
		}
		return &Literal{inner: simplified}, nil
	}
	if err := e.simplify(env); err != nil {
		return nil, err
	}
	return e, nil
}
