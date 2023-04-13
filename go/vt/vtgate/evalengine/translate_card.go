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

	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func errCardinality(expected int) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", expected)
}

func (ast *astCompiler) cardinality(expr Expr) int {
	switch expr := expr.(type) {
	case *BindVariable:
		if expr.Type == sqltypes.Tuple {
			return -1
		}
		return 1
	case TupleExpr:
		return len(expr)
	default:
		return 1
	}
}

func (ast *astCompiler) ensureCardinality(expr Expr, expected int) error {
	c := ast.cardinality(expr)
	if c != expected {
		return errCardinality(expected)
	}
	return nil
}

func (ast *astCompiler) cardComparison(expr1 Expr, expr2 Expr) error {
	card1 := ast.cardinality(expr1)
	card2 := ast.cardinality(expr2)

	switch {
	case card1 < 0:
		return errCardinality(card2)

	case card2 < 0:
		return errCardinality(card1)

	case card1 == 1 && card2 == 1:
		if err := ast.cardExpr(expr1); err != nil {
			return err
		}
		if err := ast.cardExpr(expr2); err != nil {
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
			if err := ast.cardComparison(expr1[n], expr2[n]); err != nil {
				return err
			}
		}
		return nil

	default:
		if err := ast.cardExpr(expr1); err != nil {
			return err
		}
		if err := ast.cardExpr(expr2); err != nil {
			return err
		}
		return errCardinality(card1)
	}
}

func (ast *astCompiler) cardBinary(left, right Expr) error {
	if err := ast.cardExpr(left); err != nil {
		return err
	}
	if err := ast.ensureCardinality(left, 1); err != nil {
		return err
	}
	if err := ast.cardExpr(right); err != nil {
		return err
	}
	if err := ast.ensureCardinality(right, 1); err != nil {
		return err
	}
	return nil
}

func (ast *astCompiler) cardUnary(inner Expr) error {
	if err := ast.cardExpr(inner); err != nil {
		return err
	}
	return ast.ensureCardinality(inner, 1)
}

func (ast *astCompiler) cardExpr(expr Expr) error {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *ConvertExpr:
		return ast.cardUnary(expr.Inner)
	case *ConvertUsingExpr:
		return ast.cardUnary(expr.Inner)
	case *NegateExpr:
		return ast.cardUnary(expr.Inner)
	case *CollateExpr:
		return ast.cardUnary(expr.Inner)
	case *IsExpr:
		return ast.cardUnary(expr.Inner)
	case *BitwiseNotExpr:
		return ast.cardUnary(expr.Inner)
	case *NotExpr:
		return ast.cardUnary(expr.Inner)
	case *ArithmeticExpr:
		return ast.cardBinary(expr.Left, expr.Right)
	case *LogicalExpr:
		return ast.cardBinary(expr.Left, expr.Right)
	case *BitwiseExpr:
		return ast.cardBinary(expr.Left, expr.Right)
	case *LikeExpr:
		return ast.cardBinary(expr.Left, expr.Right)
	case *ComparisonExpr:
		return ast.cardComparison(expr.Left, expr.Right)
	case *InExpr:
		if err := ast.cardExpr(expr.Left); err != nil {
			return err
		}

		left := ast.cardinality(expr.Left)
		switch r := expr.Right.(type) {
		case TupleExpr:
			for _, subexpr := range r {
				subcard := ast.cardinality(subexpr)
				if err := ast.cardExpr(subexpr); err != nil {
					return err
				}
				if left != subcard {
					return errCardinality(left)
				}
			}
		case *BindVariable:
			if r.Type != sqltypes.Tuple {
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
			if err := ast.cardExpr(subexpr); err != nil {
				return err
			}
		}
	case callable:
		return ast.cardExpr(TupleExpr(expr.callable()))
	case *Literal, *Column, *BindVariable, *CaseExpr: // noop
	default:
		panic(fmt.Sprintf("unhandled cardinality: %T", expr))
	}
	return nil
}
