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

import (
	"vitess.io/vitess/go/mysql/collations"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func (expr *Literal) constant() bool {
	return true
}

func (expr *BindVariable) constant() bool {
	return false
}

func (expr *Column) constant() bool {
	return false
}

func (expr *BinaryExpr) constant() bool {
	return expr.Left.constant() && expr.Right.constant()
}

func (expr TupleExpr) constant() bool {
	for _, subexpr := range expr {
		if !subexpr.constant() {
			return false
		}
	}
	return true
}

func (expr *UnaryExpr) constant() bool {
	return expr.Inner.constant()
}

func (expr *Literal) simplify(_ ConverterLookup) error {
	return nil
}

func (expr *BindVariable) simplify(_ ConverterLookup) error {
	return nil
}

func (expr *Column) simplify(_ ConverterLookup) error {
	return nil
}

func (expr *BinaryExpr) simplify(lookup ConverterLookup) error {
	var err error
	expr.Left, err = simplifyExpr(expr.Left, lookup)
	if err != nil {
		return err
	}
	expr.Right, err = simplifyExpr(expr.Right, lookup)
	if err != nil {
		return err
	}
	return nil
}

func (expr *LikeExpr) simplify(lookup ConverterLookup) error {
	if err := expr.BinaryExpr.simplify(lookup); err != nil {
		return err
	}

	lit2, _ := expr.Right.(*Literal)
	if lit2 != nil && lit2.Val.textual() {
		expr.MatchCollation = lit2.Val.collation().Collation
		coll := collations.Local().LookupByID(expr.MatchCollation)
		expr.Match = coll.Wildcard(lit2.Val.bytes(), 0, 0, 0)
	}
	return nil
}

func (inexpr *InExpr) simplify(lookup ConverterLookup) error {
	if err := inexpr.BinaryExpr.simplify(lookup); err != nil {
		return err
	}

	tuple, ok := inexpr.Right.(TupleExpr)
	if !ok {
		return nil
	}

	var (
		collation collations.ID
		typ       querypb.Type
		optimize  = true
	)

	for i, expr := range tuple {
		if lit, ok := expr.(*Literal); ok {
			thisColl := lit.Val.collation().Collation
			thisTyp := lit.Val.typeof()
			if i == 0 {
				collation = thisColl
				typ = thisTyp
				continue
			}
			if collation == thisColl && typ == thisTyp {
				continue
			}
		}
		optimize = false
		break
	}

	if optimize {
		inexpr.Hashed = make(map[HashCode]int)
		for i, expr := range tuple {
			lit := expr.(*Literal)
			hash, err := lit.Val.nullSafeHashcode()
			if err != nil {
				inexpr.Hashed = nil
				break
			}
			if collidx, collision := inexpr.Hashed[hash]; collision {
				cmp, _, err := evalCompareAll(&lit.Val, &tuple[collidx].(*Literal).Val, true)
				if cmp != 0 || err != nil {
					inexpr.Hashed = nil
					break
				}
				continue
			}
			inexpr.Hashed[hash] = i
		}
	}
	return nil
}

func (expr TupleExpr) simplify(lookup ConverterLookup) error {
	var err error
	for i, subexpr := range expr {
		expr[i], err = simplifyExpr(subexpr, lookup)
		if err != nil {
			return err
		}
	}
	return nil
}

func (expr *UnaryExpr) simplify(lookup ConverterLookup) error {
	var err error
	expr.Inner, err = simplifyExpr(expr.Inner, lookup)
	return err
}

func (c *CallExpression) constant() bool {
	return c.Arguments.constant()
}

func (c *CallExpression) simplify(lookup ConverterLookup) error {
	return c.Arguments.simplify(lookup)
}

func simplifyExpr(e Expr, lookup ConverterLookup) (Expr, error) {
	if e.constant() {
		var env ExpressionEnv
		env.DefaultCollation = lookup.DefaultCollation()

		res, err := env.Evaluate(e)
		if err != nil {
			return nil, err
		}
		return &Literal{Val: res}, nil
	}
	if err := e.simplify(lookup); err != nil {
		return nil, err
	}
	return e, nil
}
