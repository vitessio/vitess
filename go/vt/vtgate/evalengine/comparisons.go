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

func (c *ComparisonExpr) evaluateComparisonExprs(env ExpressionEnv) (EvalResult, EvalResult, error) {
	var lVal, rVal EvalResult
	var err error
	if lVal, err = c.Left.Evaluate(env); err != nil {
		return EvalResult{}, EvalResult{}, err
	}
	if rVal, err = c.Right.Evaluate(env); err != nil {
		return EvalResult{}, EvalResult{}, err
	}
	return lVal, rVal, nil
}

func hasNullEvalResult(l, r EvalResult) bool {
	return l.typ == sqltypes.Null || r.typ == sqltypes.Null
}

func evalResultsAreStrings(l, r EvalResult) bool {
	return (sqltypes.IsText(l.typ) || sqltypes.IsBinary(l.typ)) && (sqltypes.IsText(r.typ) || sqltypes.IsBinary(r.typ))
}

func evalResultsAreSameNumericType(l, r EvalResult) bool {
	if sqltypes.IsIntegral(l.typ) && sqltypes.IsIntegral(r.typ) {
		return true
	}
	if sqltypes.IsFloat(l.typ) && sqltypes.IsFloat(r.typ) {
		return true
	}
	if l.typ == sqltypes.Decimal && r.typ == sqltypes.Decimal {
		return true
	}
	return false
}

func needsDecimalHandling(l, r EvalResult) bool {
	return l.typ == sqltypes.Decimal && (sqltypes.IsIntegral(r.typ) || sqltypes.IsFloat(r.typ)) ||
		r.typ == sqltypes.Decimal && (sqltypes.IsIntegral(l.typ) || sqltypes.IsFloat(l.typ))
}

func evalResultsAreDates(l, r EvalResult) bool {
	return sqltypes.IsDate(l.typ) && sqltypes.IsDate(r.typ)
}

func evalResultsAreDateAndString(l, r EvalResult) bool {
	return sqltypes.IsDate(l.typ) && (sqltypes.IsText(r.typ) || sqltypes.IsBinary(r.typ)) ||
		(sqltypes.IsText(l.typ) || sqltypes.IsBinary(l.typ)) && sqltypes.IsDate(r.typ)
}

func evalResultsAreDateAndNumeric(l, r EvalResult) bool {
	return sqltypes.IsDate(l.typ) && sqltypes.IsNumber(r.typ) || sqltypes.IsNumber(l.typ) && sqltypes.IsDate(r.typ)
}

// For more details on comparison expression evaluation and type conversion:
// 		- https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
func executeComparison(lVal, rVal EvalResult) (int, error) {
	switch {
	case evalResultsAreStrings(lVal, rVal):
		return compareStrings(lVal, rVal)

	case evalResultsAreSameNumericType(lVal, rVal), needsDecimalHandling(lVal, rVal):
		return compareNumeric(lVal, rVal)

	case evalResultsAreDates(lVal, rVal):
		return compareDates(lVal, rVal)

	case evalResultsAreDateAndString(lVal, rVal):
		return compareDateAndString(lVal, rVal)

	case evalResultsAreDateAndNumeric(lVal, rVal):
		// TODO: support comparison between a date and a numeric value
		// 		queries like the ones below should be supported:
		// 			- select 1 where 20210101 = cast("2021-01-01" as date)
		// 			- select 1 where 2021210101 = cast("2021-01-01" as date)
		// 			- select 1 where 104200 = cast("10:42:00" as time)
		return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot compare a date with a numeric value")

	default:
		// Quoting MySQL Docs:
		//
		// 		"In all other cases, the arguments are compared as floating-point (real) numbers.
		// 		For example, a comparison of string and numeric operands takes place as a
		// 		comparison of floating-point numbers."
		//
		//		https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
		return compareNumeric(makeFloat(lVal), makeFloat(rVal))
	}
}

// Evaluate implements the Expr interface
func (c *ComparisonExpr) Evaluate(env ExpressionEnv) (EvalResult, error) {
	if c.Op == nil {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "a comparison expression needs a comparison operator")
	}

	lVal, rVal, err := c.evaluateComparisonExprs(env)
	if err != nil {
		return EvalResult{}, err
	}

	if hasNullEvalResult(lVal, rVal) {
		// Comparison operation NullSafeEqual (<=>) does not care if one or two sides are NULL
		if _, isNullsafe := c.Op.(*NullSafeEqualOp); !isNullsafe {
			// If a side of the comparison is NULL, result will always be NULL
			return resultNull, nil
		}
	}
	return c.Op.Evaluate(lVal, rVal)
}

// Type implements the Expr interface
func (c *ComparisonExpr) Type(ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_INT32, nil
}

// String implements the Expr interface
func (c *ComparisonExpr) String() string {
	return c.Left.String() + " " + c.Op.String() + " " + c.Right.String()
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
	numeric, err := executeComparison(left, right)
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
	numeric, err := executeComparison(left, right)
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
	numeric, err := executeComparison(left, right)
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
	numeric, err := executeComparison(left, right)
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
	numeric, err := executeComparison(left, right)
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
	numeric, err := executeComparison(left, right)
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
	if right.typ != querypb.Type_TUPLE {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple")
	}
	returnValue := resultFalse
	for _, result := range right.results {
		if result.typ == querypb.Type_NULL_TYPE {
			returnValue = resultNull
			continue
		}
		isEqual, err := (&EqualOp{}).IsTrue(left, result)
		if err != nil {
			return EvalResult{}, err
		}
		if isEqual {
			return resultTrue, nil
		}
	}
	return returnValue, nil
}

// IsTrue implements the ComparisonOp interface
func (i *InOp) IsTrue(left, right EvalResult) (bool, error) {
	res, err := i.Evaluate(left, right)
	return convertEvalResultToBoolean(res), err
}

func convertEvalResultToBoolean(result EvalResult) bool {
	if result.typ == querypb.Type_NULL_TYPE {
		return false
	}
	if sqltypes.IsIntegral(result.typ) && result.ival == 0 {
		return false
	}
	return true
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
	res, err := (&InOp{}).Evaluate(left, right)
	res.ival = 1 - res.ival
	return res, err
}

// IsTrue implements the ComparisonOp interface
func (n *NotInOp) IsTrue(left, right EvalResult) (bool, error) {
	res, err := (&InOp{}).IsTrue(left, right)
	return !res, err
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
