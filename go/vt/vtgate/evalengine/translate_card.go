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
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func errCardinality(expected int) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", expected)
}

func (card cardinalityCheck) cardinality(expr Expr) int {
	switch expr := expr.(type) {
	case *BindVariable:
		if expr.tuple {
			return -1
		}
		return 1
	case TupleExpr:
		return len(expr)
	default:
		return 1
	}
}

func (card cardinalityCheck) ensure(expr Expr, expected int) error {
	c := card.cardinality(expr)
	if c != expected {
		return errCardinality(expected)
	}
	return nil
}

func (card cardinalityCheck) comparison(expr1 Expr, expr2 Expr) error {
	card1 := card.cardinality(expr1)
	card2 := card.cardinality(expr2)

	switch {
	case card1 < 0:
		return errCardinality(card2)

	case card2 < 0:
		return errCardinality(card1)

	case card1 == 1 && card2 == 1:
		if err := card.expr(expr1); err != nil {
			return err
		}
		if err := card.expr(expr2); err != nil {
			return err
		}
		return nil

	case card1 == card2:
		expr1 := expr1.(TupleExpr)
		expr2 := expr2.(TupleExpr)
		if len(expr1) != len(expr2) {
			panic("did not check cardinalities?")
		}
		for n := range expr1 {
			if err := card.comparison(expr1[n], expr2[n]); err != nil {
				return err
			}
		}
		return nil

	default:
		if err := card.expr(expr1); err != nil {
			return err
		}
		if err := card.expr(expr2); err != nil {
			return err
		}
		return errCardinality(card1)
	}
}

func (card cardinalityCheck) binary(left, right Expr) error {
	if err := card.expr(left); err != nil {
		return err
	}
	if err := card.ensure(left, 1); err != nil {
		return err
	}
	if err := card.expr(right); err != nil {
		return err
	}
	if err := card.ensure(right, 1); err != nil {
		return err
	}
	return nil
}

func (card cardinalityCheck) unary(inner Expr) error {
	if err := card.expr(inner); err != nil {
		return err
	}
	return card.ensure(inner, 1)
}

type cardinalityCheck struct{}

func (card cardinalityCheck) expr(expr Expr) error {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *ConvertExpr:
		return card.unary(expr.Inner)
	case *ConvertUsingExpr:
		return card.unary(expr.Inner)
	case *NegateExpr:
		return card.unary(expr.Inner)
	case *CollateExpr:
		return card.unary(expr.Inner)
	case *IsExpr:
		return card.unary(expr.Inner)
	case *BitwiseNotExpr:
		return card.unary(expr.Inner)
	case *ArithmeticExpr:
		return card.binary(expr.Left, expr.Right)
	case *LogicalExpr:
		return card.binary(expr.Left, expr.Right)
	case *BitwiseExpr:
		return card.binary(expr.Left, expr.Right)
	case *LikeExpr:
		return card.binary(expr.Left, expr.Right)
	case *ComparisonExpr:
		return card.comparison(expr.Left, expr.Right)
	case *InExpr:
		if err := card.expr(expr.Left); err != nil {
			return err
		}

		left := card.cardinality(expr.Left)
		switch r := expr.Right.(type) {
		case TupleExpr:
			for _, subexpr := range r {
				subcard := card.cardinality(subexpr)
				if err := card.expr(subexpr); err != nil {
					return err
				}
				if left != subcard {
					return errCardinality(left)
				}
			}
		case *BindVariable:
			if !r.tuple {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple")
			}
			if left != 1 {
				return errCardinality(1)
			}
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple")
		}

	case TupleExpr:
		for _, subexpr := range expr {
			if err := card.expr(subexpr); err != nil {
				return err
			}
		}
	case callable:
		return card.expr(TupleExpr(expr.callable()))
	case *Literal, *Column, *BindVariable, *CaseExpr: // noop
	default:
		panic(fmt.Sprintf("unhandled cardinality: %T", expr))
	}
	return nil
}
