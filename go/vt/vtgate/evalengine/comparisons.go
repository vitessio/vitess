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
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
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
		Type() querypb.Type
		String() string
	}

	ComparisonExpr struct {
		Op                      ComparisonOp
		Left, Right             Expr
		CoerceLeft, CoerceRight collations.Coercion
		TypedCollation          collations.TypedCollation
	}

	EqualOp struct {
		Operator string
		Compare  func(cmp int) bool
	}

	NullSafeEqualOp struct{}

	InOp struct {
		Negate bool
		Hashed map[uintptr]int
	}
	LikeOp struct {
		Negate bool
		Match  collations.WildcardPattern
	}
	RegexpOp struct {
		Negate bool
	}
)

var (
	resultTrue  = EvalResult{typ: sqltypes.Int64, numval: 1, collation: collationNumeric}
	resultFalse = EvalResult{typ: sqltypes.Int64, numval: 0, collation: collationNumeric}
	resultNull  = EvalResult{typ: sqltypes.Null, collation: collationNull}
)

var _ ComparisonOp = (*EqualOp)(nil)
var _ ComparisonOp = (*InOp)(nil)
var _ ComparisonOp = (*LikeOp)(nil)
var _ ComparisonOp = (*RegexpOp)(nil)

func (c *ComparisonExpr) Collation() collations.TypedCollation {
	return c.TypedCollation
}

func (c *ComparisonExpr) mergeCollations() error {
	var err error

	leftColl := c.Left.Collation()
	rightColl := c.Right.Collation()

	if leftColl.Valid() && rightColl.Valid() {
		env := collations.Local()
		c.TypedCollation, c.CoerceLeft, c.CoerceRight, err =
			env.MergeCollations(leftColl, rightColl, collations.CoercionOptions{
				ConvertToSuperset:   true,
				ConvertWithCoercion: true,
			})
	}
	return err
}

func (c *ComparisonExpr) evaluateComparisonExprs(env *ExpressionEnv) (EvalResult, EvalResult, error) {
	var lVal, rVal EvalResult
	var err error
	if lVal, err = c.Left.Evaluate(env); err != nil {
		return EvalResult{}, EvalResult{}, err
	}
	if lVal.textual() && c.CoerceLeft != nil {
		lVal.bytes, _ = c.CoerceLeft(nil, lVal.bytes)
	}
	lVal.collation = c.TypedCollation
	if rVal, err = c.Right.Evaluate(env); err != nil {
		return EvalResult{}, EvalResult{}, err
	}
	if rVal.textual() && c.CoerceRight != nil {
		rVal.bytes, _ = c.CoerceRight(nil, rVal.bytes)
	}
	rVal.collation = c.TypedCollation
	return lVal, rVal, nil
}

func hasNullEvalResult(l, r EvalResult) bool {
	return l.typ == sqltypes.Null || r.typ == sqltypes.Null
}

func evalResultsAreStrings(l, r EvalResult) bool {
	return l.textual() && r.textual()
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
	return (sqltypes.IsDate(l.typ) && r.textual()) || (l.textual() && sqltypes.IsDate(r.typ))
}

func evalResultsAreDateAndNumeric(l, r EvalResult) bool {
	return sqltypes.IsDate(l.typ) && sqltypes.IsNumber(r.typ) || sqltypes.IsNumber(l.typ) && sqltypes.IsDate(r.typ)
}

func nullSafeCoerceAndCompare(lVal, rVal EvalResult) (comp int, isNull bool, err error) {
	if lVal.collation.Collation != rVal.collation.Collation {
		lVal, rVal, err = mergeCollations(lVal, rVal)
		if err != nil {
			return 0, false, err
		}
	}
	return nullSafeCompare(lVal, rVal)
}

func nullSafeTypecheck(lVal, rVal EvalResult) error {
	switch {
	case lVal.typ == querypb.Type_TUPLE && rVal.typ == querypb.Type_TUPLE:
	case lVal.typ == querypb.Type_TUPLE:
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", len(*lVal.tuple))
	case rVal.typ == querypb.Type_TUPLE:
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain 1 column(s)")
	}
	return nil
}

// For more details on comparison expression evaluation and type conversion:
// 		- https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
func nullSafeCompare(lVal, rVal EvalResult) (comp int, isNull bool, err error) {
	lVal = foldSingleLenTuples(lVal)
	rVal = foldSingleLenTuples(rVal)
	if hasNullEvalResult(lVal, rVal) {
		return 0, true, nil
	}
	switch {
	case evalResultsAreStrings(lVal, rVal):
		comp = compareStrings(lVal, rVal)
		return comp, false, nil

	case evalResultsAreSameNumericType(lVal, rVal), needsDecimalHandling(lVal, rVal):
		comp, err = compareNumeric(lVal, rVal)
		return comp, false, err

	case evalResultsAreDates(lVal, rVal):
		comp, err = compareDates(lVal, rVal)
		return comp, false, err

	case evalResultsAreDateAndString(lVal, rVal):
		comp, err = compareDateAndString(lVal, rVal)
		return comp, false, err

	case evalResultsAreDateAndNumeric(lVal, rVal):
		// TODO: support comparison between a date and a numeric value
		// 		queries like the ones below should be supported:
		// 			- select 1 where 20210101 = cast("2021-01-01" as date)
		// 			- select 1 where 2021210101 = cast("2021-01-01" as date)
		// 			- select 1 where 104200 = cast("10:42:00" as time)
		return 0, false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot compare a date with a numeric value")

	case lVal.typ == querypb.Type_TUPLE && rVal.typ == querypb.Type_TUPLE:
		return compareTuples(lVal, rVal)
	case lVal.typ == querypb.Type_TUPLE:
		return 0, false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", len(*lVal.tuple))
	case rVal.typ == querypb.Type_TUPLE:
		return 0, false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain 1 column(s)")

	default:
		// Quoting MySQL Docs:
		//
		// 		"In all other cases, the arguments are compared as floating-point (real) numbers.
		// 		For example, a comparison of string and numeric operands takes place as a
		// 		comparison of floating-point numbers."
		//
		//		https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
		comp, err = compareNumeric(makeFloat(lVal), makeFloat(rVal))
		return comp, false, err
	}
}

func foldSingleLenTuples(val EvalResult) EvalResult {
	if val.typ == querypb.Type_TUPLE && len(*val.tuple) == 1 {
		return (*val.tuple)[0]
	}
	return val
}

func boolResult(result, negate bool) EvalResult {
	if result == !negate {
		return resultTrue
	}
	return resultFalse
}

// Evaluate implements the Expr interface
func (c *ComparisonExpr) Evaluate(env *ExpressionEnv) (EvalResult, error) {
	if c.Op == nil {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "a comparison expression needs a comparison operator")
	}

	lVal, rVal, err := c.evaluateComparisonExprs(env)
	if err != nil {
		return EvalResult{}, err
	}

	return c.Op.Evaluate(lVal, rVal)
}

// Type implements the Expr interface
func (c *ComparisonExpr) Type(*ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_INT32, nil
}

// Evaluate implements the ComparisonOp interface
func (e *EqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	// No need to coerce here because the caller ComparisonExpr.Evaluate has coerced for us
	numeric, isNull, err := nullSafeCompare(left, right)
	if err != nil {
		return EvalResult{}, err
	}
	if isNull {
		return resultNull, err
	}
	if e.Compare(numeric) {
		return resultTrue, nil
	}
	return resultFalse, nil
}

// Type implements the ComparisonOp interface
func (e *EqualOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (e *EqualOp) String() string {
	return e.Operator
}

// Evaluate implements the ComparisonOp interface
func (n *NullSafeEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
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
func (i *InOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	if right.typ != querypb.Type_TUPLE {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple")
	}

	var foundNull, found bool

	if i.Hashed != nil {
		hash, err := left.nullSafeHashcode()
		if err != nil {
			return EvalResult{}, err
		}
		if idx, ok := i.Hashed[hash]; ok {
			var numeric int
			numeric, foundNull, err = nullSafeCoerceAndCompare(left, (*right.tuple)[idx])
			if err != nil {
				return EvalResult{}, err
			}
			found = numeric == 0
		}
	} else {
		for _, rtuple := range *right.tuple {
			if found {
				if err := nullSafeTypecheck(left, rtuple); err != nil {
					return EvalResult{}, err
				}
				continue
			}

			numeric, isNull, err := nullSafeCoerceAndCompare(left, rtuple)
			if err != nil {
				return EvalResult{}, err
			}
			if isNull {
				foundNull = true
				continue
			}
			if numeric == 0 {
				found = true
			}
		}
	}

	if found {
		return boolResult(found, i.Negate), nil
	}
	if foundNull {
		return resultNull, nil
	}
	return boolResult(found, i.Negate), nil
}

// Type implements the ComparisonOp interface
func (i *InOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (i *InOp) String() string {
	if i.Negate {
		return "not in"
	}
	return "in"
}

func (l *LikeOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	if left.collation.Collation != right.collation.Collation {
		panic(fmt.Sprintf("LikeOp: did not coerce, left=%d right=%d",
			left.collation.Collation, right.collation.Collation))
	}
	var matched bool
	if l.Match != nil {
		matched = l.Match.Match(left.bytes)
	} else {
		coll := collations.Local().LookupByID(left.collation.Collation)
		wc := coll.Wildcard(right.bytes, 0, 0, 0)
		matched = wc.Match(left.bytes)
	}
	return boolResult(matched, l.Negate), nil
}

// Type implements the ComparisonOp interface
func (l *LikeOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (l *LikeOp) String() string {
	if l.Negate {
		return "not like"
	}
	return "like"
}

func (r *RegexpOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	panic("implement me")
}

// Type implements the ComparisonOp interface
func (r *RegexpOp) Type() querypb.Type {
	return querypb.Type_INT32
}

// String implements the ComparisonOp interface
func (r *RegexpOp) String() string {
	return "regexp"
}
