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
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	// ComparisonOp interfaces all the possible comparison operations we have, it eases the job of ComparisonExpr
	// when evaluating the whole comparison
	ComparisonOp interface {
		Evaluate(left, right EvalResult) (EvalResult, error)
		Type(left querypb.Type) querypb.Type
		String() string
	}

	ComparisonExpr struct {
		Op          ComparisonOp
		Left, Right Expr
	}

	EqualOp         struct{}
	NotEqualOp      struct{}
	NullSafeEqualOp struct{}
	LessThanOp      struct{}
	LessEqualOp     struct{}
	GreaterThanOp   struct{}
	GreaterEqualOp  struct{}
	InOp            struct{}
	NotInOp         struct{}
	LikeOp          struct{}
	NotLikeOp       struct{}
	RegexpOp        struct{}
	NotRegexpOp     struct{}
)

var _ ComparisonOp = (*EqualOp)(nil)
var _ ComparisonOp = (*NotEqualOp)(nil)
var _ ComparisonOp = (*NullSafeEqualOp)(nil)
var _ ComparisonOp = (*LessThanOp)(nil)
var _ ComparisonOp = (*LessEqualOp)(nil)
var _ ComparisonOp = (*GreaterThanOp)(nil)
var _ ComparisonOp = (*GreaterEqualOp)(nil)
var _ ComparisonOp = (*InOp)(nil)
var _ ComparisonOp = (*NotInOp)(nil)
var _ ComparisonOp = (*LikeOp)(nil)
var _ ComparisonOp = (*NotLikeOp)(nil)
var _ ComparisonOp = (*RegexpOp)(nil)
var _ ComparisonOp = (*NotRegexpOp)(nil)

func evaluateSideOfComparison(expr Expr, env ExpressionEnv) (EvalResult, error) {
	val, err := expr.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	if val.typ == sqltypes.Null {
		return NullEvalResult(), nil
	}
	return makeNumeric(val), nil
}

// Evaluate implements the Expr interface
func (e *ComparisonExpr) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := evaluateSideOfComparison(e.Left, env)
	if lVal.typ == sqltypes.Null || err != nil {
		return lVal, err
	}

	rVal, err := evaluateSideOfComparison(e.Right, env)
	if rVal.typ == sqltypes.Null || err != nil {
		return rVal, err
	}

	numeric, err := compareNumeric(lVal, rVal)
	if err != nil {
		return EvalResult{}, err
	}
	value := int64(0)
	if numeric == 0 {
		value = 1
	}
	return EvalResult{
		typ:  sqltypes.Int32,
		ival: value,
	}, nil
}

// Type implements the Expr interface
func (e *ComparisonExpr) Type(ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_INT32, nil
}

// String implements the Expr interface
func (e *ComparisonExpr) String() string {
	return e.Left.String() + " = " + e.Right.String()
}

func (e *EqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (e *EqualOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (e *EqualOp) String() string {
	panic("implement me")
}

func (n *NotEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (n *NotEqualOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (n *NotEqualOp) String() string {
	panic("implement me")
}

func (n *NullSafeEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (n *NullSafeEqualOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (n *NullSafeEqualOp) String() string {
	panic("implement me")
}

func (l *LessThanOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (l *LessThanOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (l *LessThanOp) String() string {
	panic("implement me")
}

func (l *LessEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (l *LessEqualOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (l *LessEqualOp) String() string {
	panic("implement me")
}

func (g *GreaterThanOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (g *GreaterThanOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (g *GreaterThanOp) String() string {
	panic("implement me")
}

func (g *GreaterEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (g *GreaterEqualOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (g *GreaterEqualOp) String() string {
	panic("implement me")
}

func (i *InOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (i *InOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (i *InOp) String() string {
	panic("implement me")
}

func (n *NotInOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (n *NotInOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (n *NotInOp) String() string {
	panic("implement me")
}

func (l *LikeOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (l *LikeOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (l *LikeOp) String() string {
	panic("implement me")
}

func (n *NotLikeOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (n *NotLikeOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (n *NotLikeOp) String() string {
	panic("implement me")
}

func (r *RegexpOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (r *RegexpOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (r *RegexpOp) String() string {
	panic("implement me")
}

func (n *NotRegexpOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

func (n *NotRegexpOp) Type(left querypb.Type) querypb.Type {
	panic("implement me")
}

func (n *NotRegexpOp) String() string {
	panic("implement me")
}
