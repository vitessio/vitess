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

func (expr *Literal) simplify() error {
	return nil
}

func (expr *BindVariable) simplify() error {
	return nil
}

func (expr *Column) simplify() error {
	return nil
}

func (expr *BinaryExpr) simplify() error {
	var err error
	expr.Left, err = simplifyExpr(expr.Left)
	if err != nil {
		return err
	}
	expr.Right, err = simplifyExpr(expr.Right)
	if err != nil {
		return err
	}
	return nil
}

func (expr *LikeExpr) simplify() error {
	if err := expr.BinaryCoercedExpr.simplify(); err != nil {
		return err
	}

	lit2, _ := expr.Right.(*Literal)
	if lit2 != nil && lit2.Val.textual() && expr.MergedCollation.Valid() {
		coll := collations.Local().LookupByID(expr.MergedCollation.Collation)
		expr.Match = coll.Wildcard(lit2.Val.bytes(), 0, 0, 0)
	}
	return nil
}

func (inexpr *InExpr) simplify() error {
	if err := inexpr.BinaryExpr.simplify(); err != nil {
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

func (expr *BinaryCoercedExpr) simplify() error {
	var err error

	if err = expr.BinaryExpr.simplify(); err != nil {
		return err
	}

	lit1, _ := expr.Left.(*Literal)
	lit2, _ := expr.Right.(*Literal)

	if lit1 != nil && expr.CoerceLeft != nil {
		b, _ := expr.CoerceLeft(nil, lit1.Val.bytes())
		lit1.Val.replaceBytes(b, expr.MergedCollation)
		expr.CoerceLeft = nil
	}
	if lit2 != nil && expr.CoerceRight != nil {
		b, _ := expr.CoerceRight(nil, lit2.Val.bytes())
		lit2.Val.replaceBytes(b, expr.MergedCollation)
		expr.CoerceRight = nil
	}

	return nil
}

func (expr TupleExpr) simplify() error {
	var err error
	for i, subexpr := range expr {
		expr[i], err = simplifyExpr(subexpr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (expr *UnaryExpr) simplify() error {
	var err error
	expr.Inner, err = simplifyExpr(expr.Inner)
	return err
}

func simplifyExpr(e Expr) (Expr, error) {
	if e.constant() {
		res, err := noenv.Evaluate(e)
		if err != nil {
			return nil, err
		}
		return &Literal{Val: res}, nil
	}
	if err := e.simplify(); err != nil {
		return nil, err
	}
	return e, nil
}
