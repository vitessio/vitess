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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// ComparisonOp interfaces all the possible comparison operations we have, it eases the job of ComparisonExpr
	// when evaluating the whole comparison
	ComparisonOp interface {
		Evaluate(left, right EvalResult) (EvalResult, error)
		IsTrue(left, right EvalResult) (bool, error)
		Type() querypb.Type
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

var (
	resultTrue  = EvalResult{typ: sqltypes.Int32, ival: 1}
	resultFalse = EvalResult{typ: sqltypes.Int32, ival: 0}
	resultNull  = EvalResult{typ: sqltypes.Null}
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
		return resultNull, nil
	}
	return makeNumeric(val), nil
}

// Evaluate implements the Expr interface
func (e *ComparisonExpr) Evaluate(env ExpressionEnv) (EvalResult, error) {
	if e.Op == nil {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "a comparison expression needs a comparison operator")
	}

	lVal, err := evaluateSideOfComparison(e.Left, env)
	if lVal.typ == sqltypes.Null || err != nil {
		return lVal, err
	}

	rVal, err := evaluateSideOfComparison(e.Right, env)
	if rVal.typ == sqltypes.Null || err != nil {
		return rVal, err
	}

	return e.Op.Evaluate(lVal, rVal)
}

// Type implements the Expr interface
func (e *ComparisonExpr) Type(ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_INT32, nil
}

// String implements the Expr interface
func (e *ComparisonExpr) String() string {
	return e.Left.String() + " " + e.Op.String() + " " + e.Right.String()
}

// Evaluate implements the ComparisonOp interface
func (e *EqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	if out, err := e.IsTrue(left, right); err != nil || !out {
		return resultFalse, err
	}
	return resultTrue, nil
}

// IsTrue implements the ComparisonOp interface
func (e *EqualOp) IsTrue(left, right EvalResult) (bool, error) {
	numeric, err := compareNumeric(left, right)
	if err != nil {
		return false, err
	}
	return numeric == 0, nil
}

// Type implements the ComparisonOp interface
func (e *EqualOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (e *EqualOp) String() string {
	return "="
}

// Evaluate implements the ComparisonOp interface
func (n *NotEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	if out, err := n.IsTrue(left, right); err != nil || !out {
		return resultFalse, err
	}
	return resultTrue, nil
}

// IsTrue implements the ComparisonOp interface
func (n *NotEqualOp) IsTrue(left, right EvalResult) (bool, error) {
	numeric, err := compareNumeric(left, right)
	if err != nil {
		return false, err
	}
	return numeric != 0, nil
}

// Type implements the ComparisonOp interface
func (n *NotEqualOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (n *NotEqualOp) String() string {
	return "!="
}

// Evaluate implements the ComparisonOp interface
func (n *NullSafeEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

// IsTrue implements the ComparisonOp interface
func (n *NullSafeEqualOp) IsTrue(left, right EvalResult) (bool, error) {
	return false, nil
}

// Type implements the ComparisonOp interface
func (n *NullSafeEqualOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (n *NullSafeEqualOp) String() string {
	return "<=>"
}

// Evaluate implements the ComparisonOp interface
func (l *LessThanOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	if out, err := l.IsTrue(left, right); err != nil || !out {
		return resultFalse, err
	}
	return resultTrue, nil
}

// IsTrue implements the ComparisonOp interface
func (l *LessThanOp) IsTrue(left, right EvalResult) (bool, error) {
	numeric, err := compareNumeric(left, right)
	if err != nil {
		return false, err
	}
	return numeric < 0, nil
}

// Type implements the ComparisonOp interface
func (l *LessThanOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (l *LessThanOp) String() string {
	return "<"
}

// Evaluate implements the ComparisonOp interface
func (l *LessEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	if out, err := l.IsTrue(left, right); err != nil || !out {
		return resultFalse, err
	}
	return resultTrue, nil
}

// IsTrue implements the ComparisonOp interface
func (l *LessEqualOp) IsTrue(left, right EvalResult) (bool, error) {
	numeric, err := compareNumeric(left, right)
	if err != nil {
		return false, err
	}
	return numeric <= 0, nil
}

// Type implements the ComparisonOp interface
func (l *LessEqualOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (l *LessEqualOp) String() string {
	return "<="
}

// Evaluate implements the ComparisonOp interface
func (g *GreaterThanOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	if out, err := g.IsTrue(left, right); err != nil || !out {
		return resultFalse, err
	}
	return resultTrue, nil
}

// IsTrue implements the ComparisonOp interface
func (g *GreaterThanOp) IsTrue(left, right EvalResult) (bool, error) {
	numeric, err := compareNumeric(left, right)
	if err != nil {
		return false, err
	}
	return numeric > 0, nil
}

// Type implements the ComparisonOp interface
func (g *GreaterThanOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (g *GreaterThanOp) String() string {
	return ">"
}

// Evaluate implements the ComparisonOp interface
func (g *GreaterEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	if out, err := g.IsTrue(left, right); err != nil || !out {
		return resultFalse, err
	}
	return resultTrue, nil
}

// IsTrue implements the ComparisonOp interface
func (g *GreaterEqualOp) IsTrue(left, right EvalResult) (bool, error) {
	numeric, err := compareNumeric(left, right)
	if err != nil {
		return false, err
	}
	return numeric >= 0, nil
}

// Type implements the ComparisonOp interface
func (g *GreaterEqualOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (g *GreaterEqualOp) String() string {
	return ">="
}

// Evaluate implements the ComparisonOp interface
func (i *InOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

// IsTrue implements the ComparisonOp interface
func (i *InOp) IsTrue(left, right EvalResult) (bool, error) {
	return false, nil
}

// Type implements the ComparisonOp interface
func (i *InOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (i *InOp) String() string {
	return "in"
}

// Evaluate implements the ComparisonOp interface
func (n *NotInOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

// IsTrue implements the ComparisonOp interface
func (n *NotInOp) IsTrue(left, right EvalResult) (bool, error) {
	return false, nil
}

// Type implements the ComparisonOp interface
func (n *NotInOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (n *NotInOp) String() string {
	return "not in"
}

// Evaluate implements the ComparisonOp interface
func (l *LikeOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

// IsTrue implements the ComparisonOp interface
func (l *LikeOp) IsTrue(left, right EvalResult) (bool, error) {
	return false, nil
}

// Type implements the ComparisonOp interface
func (l *LikeOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (l *LikeOp) String() string {
	return "like"
}

// Evaluate implements the ComparisonOp interface
func (n *NotLikeOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

// IsTrue implements the ComparisonOp interface
func (n *NotLikeOp) IsTrue(left, right EvalResult) (bool, error) {
	return false, nil
}

// Type implements the ComparisonOp interface
func (n *NotLikeOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (n *NotLikeOp) String() string {
	return "not like"
}

// Evaluate implements the ComparisonOp interface
func (r *RegexpOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

// IsTrue implements the ComparisonOp interface
func (r *RegexpOp) IsTrue(left, right EvalResult) (bool, error) {
	return false, nil
}

// Type implements the ComparisonOp interface
func (r *RegexpOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (r *RegexpOp) String() string {
	return "regexp"
}

// Evaluate implements the ComparisonOp interface
func (n *NotRegexpOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

// IsTrue implements the ComparisonOp interface
func (n *NotRegexpOp) IsTrue(left, right EvalResult) (bool, error) {
	return false, nil
}

// Type implements the ComparisonOp interface
func (n *NotRegexpOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (n *NotRegexpOp) String() string {
	return "not regexp"
}
