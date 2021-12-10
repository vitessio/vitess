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

func (expr *ComparisonExpr) constant() bool {
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

func (expr *CollateExpr) constant() bool {
	return expr.Expr.constant()
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

func (expr *ComparisonExpr) simplify() error {
	var err error

	expr.Left, err = simplifyExpr(expr.Left)
	if err != nil {
		return err
	}
	expr.Right, err = simplifyExpr(expr.Right)
	if err != nil {
		return err
	}

	lit1, _ := expr.Left.(*Literal)
	lit2, _ := expr.Right.(*Literal)

	if lit1 != nil && expr.CoerceLeft != nil {
		lit1.Val.bytes, _ = expr.CoerceLeft(nil, lit1.Val.bytes)
		lit1.Val.collation = expr.TypedCollation
		expr.CoerceLeft = nil
	}
	if lit2 != nil && expr.CoerceRight != nil {
		lit2.Val.bytes, _ = expr.CoerceRight(nil, lit2.Val.bytes)
		lit2.Val.collation = expr.TypedCollation
		expr.CoerceRight = nil
	}

	switch op := expr.Op.(type) {
	case *LikeOp:
		if lit2 != nil && lit2.Val.textual() && expr.TypedCollation.Valid() {
			coll := collations.Local().LookupByID(expr.TypedCollation.Collation)
			op.Match = coll.Wildcard(lit2.Val.bytes, 0, 0, 0)
		}

	case *InOp:
		if tuple, ok := expr.Right.(TupleExpr); ok {
			var (
				collation collations.ID
				typ       querypb.Type
				optimize  = true
			)

			for i, expr := range tuple {
				if lit, ok := expr.(*Literal); ok {
					thisColl := lit.Val.collation.Collation
					thisTyp := lit.Val.typ
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
				op.Hashed = make(map[HashCode]int)
				for i, expr := range tuple {
					lit := expr.(*Literal)
					hash, err := lit.Val.nullSafeHashcode()
					if err != nil {
						op.Hashed = nil
						break
					}
					if collidx, collision := op.Hashed[hash]; collision {
						cmp, _, err := evalCompareAll(lit.Val, tuple[collidx].(*Literal).Val)
						if cmp != 0 || err != nil {
							op.Hashed = nil
							break
						}
						continue
					}
					op.Hashed[hash] = i
				}
			}
		}
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

func (expr *CollateExpr) simplify() error {
	var err error
	expr.Expr, err = simplifyExpr(expr.Expr)
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
