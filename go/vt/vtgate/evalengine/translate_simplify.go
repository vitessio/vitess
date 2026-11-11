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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
)

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
		lit := evalToIR(simplified)
		if l, ok := lit.(*Literal); ok && l.inner != nil {
			// a folded literal's type comes from its value, which the
			// interpreter computed without declared types. For pass-through
			// constructs, convert the value to the declared representation and
			// keep the declared SQL type (e.g. YEAR).
			if ct, ok := foldedType(env, e); ok && sqltypes.IsNumber(ct.Type) {
				if l.inner.SQLType() != ct.Type {
					v, err := evalCoerce(l.inner, ct.Type, ct.Size, ct.Scale, collations.CollationBinaryID, env.now, env.sqlmode.AllowZeroDate())
					if err != nil {
						return nil, err
					}
					l = &Literal{inner: v}
				}
				if ct.SQL != 0 {
					return &declaredLiteral{Literal: *l, sql: ct.SQL}, nil
				}
				return l, nil
			}
		}
		return lit, nil
	}
	if err := e.simplify(env); err != nil {
		return nil, err
	}
	return e, nil
}

// evalToIR turns an internal eval result into an IR:
//   - if it’s an evalTuple, it recurses into a TupleExpr
//   - otherwise it wraps the single value in a Literal
func evalToIR(simplified eval) IR {
	if tuple, isTuple := simplified.(*evalTuple); isTuple {
		te := make(TupleExpr, len(tuple.t))
		for i, t := range tuple.t {
			te[i] = evalToIR(t)
		}
		return te
	}
	if j, ok := simplified.(*evalJSON); ok {
		// The literal is shared by every evaluation and every compiled
		// specialisation: settle it while this goroutine still owns it.
		j.Resolve()
	}
	return &Literal{inner: simplified}
}

// foldedSQLType returns the declared type of a constant pass-through expression.
func foldedType(env *ExpressionEnv, e IR) (ctype, bool) {
	if !keepsSQLType(e) {
		return ctype{}, false
	}
	comp := compiler{env: env.vc.Environment(), collation: env.collationEnv.DefaultConnectionCharset(), sqlmode: env.sqlmode}
	ct, err := e.compile(&comp)
	if err != nil {
		return ctype{}, false
	}
	return declared(e, ct), true
}

// declaredLiteral is a folded constant that keeps the declared SQL type of the
// expression it replaced (e.g. YEAR for YEAR('2023-05-06')).
type declaredLiteral struct {
	Literal
	sql sqltypes.Type
}

func (l *declaredLiteral) IR() IR { return l }

func (l *declaredLiteral) typeof(env *ExpressionEnv) (ctype, error) {
	ct, err := l.Literal.typeof(env)
	return ct.withSQL(l.sql), err
}

func (l *declaredLiteral) compile(c *compiler) (ctype, error) {
	ct, err := l.Literal.compile(c)
	return ct.withSQL(l.sql), err
}
