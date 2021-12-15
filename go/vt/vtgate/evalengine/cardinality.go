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
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func cardinalityError(expected int) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", expected)
}

func checkTupleCardinality(lVal, rVal item) (bool, error) {
	switch {
	case lVal.typeof() == querypb.Type_TUPLE && rVal.typeof() == querypb.Type_TUPLE:
		return true, nil
	case lVal.typeof() == querypb.Type_TUPLE:
		return false, cardinalityError(len(lVal.tuple()))
	case rVal.typeof() == querypb.Type_TUPLE:
		return false, cardinalityError(1)
	default:
		return false, nil
	}
}

func (expr *Literal) cardinality(*ExpressionEnv) (int, error) {
	return 1, nil
}

func (expr *BindVariable) cardinality(env *ExpressionEnv) (int, error) {
	val, found := env.BindVars[expr.Key]
	if !found {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", expr.Key)
	}
	switch val.Type {
	case querypb.Type_TUPLE:
		return len(val.Values), nil
	default:
		return 1, nil
	}
}

func (expr *Column) cardinality(*ExpressionEnv) (int, error) {
	// columns cannot be tuples
	return 1, nil
}

func (expr *BinaryExpr) cardinality(env *ExpressionEnv) (int, error) {
	card1, err := expr.Left.cardinality(env)
	if err != nil {
		return 1, err
	}
	if card1 != 1 {
		return 1, cardinalityError(1)
	}
	card2, err := expr.Right.cardinality(env)
	if err != nil {
		return 1, err
	}
	if card2 != 1 {
		return 1, cardinalityError(1)
	}
	return 1, nil
}

func subcardinality(env *ExpressionEnv, expr Expr, n int) (int, error) {
	switch expr := expr.(type) {
	case TupleExpr:
		return expr[n].cardinality(env)
	case *BindVariable:
		val, found := env.BindVars[expr.Key]
		if !found {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", expr.Key)
		}
		switch val.Type {
		case querypb.Type_TUPLE:
			// values under this tuple cannot be nested tuples
			return 1, nil
		}
	}
	return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple")
}

func (expr *InExpr) cardinality(env *ExpressionEnv) (int, error) {
	card1, err := expr.Left.cardinality(env)
	if err != nil {
		return 1, err
	}
	card2, _ := expr.Right.cardinality(env)
	for n := 0; n < card2; n++ {
		subcard2, err := subcardinality(env, expr.Right, n)
		if err != nil {
			return 1, err
		}
		if card1 != subcard2 {
			return 1, cardinalityError(card1)
		}
	}
	return 1, nil
}

func (expr *ComparisonExpr) cardinality(env *ExpressionEnv) (int, error) {
	card1, err := expr.Left.cardinality(env)
	if err != nil {
		return 1, err
	}
	card2, err := expr.Right.cardinality(env)
	if err != nil {
		return 1, err
	}
	if card1 != card2 {
		return 1, cardinalityError(card1)
	}
	if card1 > 1 {
		for n := 0; n < card1; n++ {
			subcard1, err := subcardinality(env, expr.Left, n)
			if err != nil {
				return 1, err
			}
			subcard2, err := subcardinality(env, expr.Right, n)
			if err != nil {
				return 1, err
			}
			if subcard1 != subcard2 {
				return 1, cardinalityError(subcard1)
			}
		}
	}
	return 1, nil
}

func (expr TupleExpr) cardinality(env *ExpressionEnv) (int, error) {
	for _, subexpr := range expr {
		if _, err := subexpr.cardinality(env); err != nil {
			return len(expr), err
		}
	}
	return len(expr), nil
}

func (expr *CollateExpr) cardinality(env *ExpressionEnv) (int, error) {
	return expr.Inner.cardinality(env)
}

func (n *NotExpr) cardinality(env *ExpressionEnv) (int, error) {
	card, err := n.Inner.cardinality(env)
	if err != nil {
		return 1, err
	}
	if card != 1 {
		return 1, cardinalityError(1)
	}
	return 1, nil
}
