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
	resultNull = EvalResult{typ: sqltypes.Null, collation: collationNull}
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
	if rVal, err = c.Right.Evaluate(env); err != nil {
		return EvalResult{}, EvalResult{}, err
	}

	// If we have pre-calculated a merged collation for this comparison,
	// coerce both operands here. Otherwise, we'll to the coercion lazily
	// when we have to actually compare the two values.
	if c.TypedCollation.Valid() {
		lVal.collation = c.TypedCollation
		rVal.collation = c.TypedCollation
		if lVal.textual() && c.CoerceLeft != nil {
			lVal.bytes, _ = c.CoerceLeft(nil, lVal.bytes)
		}
		if rVal.textual() && c.CoerceRight != nil {
			rVal.bytes, _ = c.CoerceRight(nil, rVal.bytes)
		}
	}

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

func evalCoerceAndCompare(lVal, rVal EvalResult) (comp int, isNull bool, err error) {
	if lVal.collation.Collation != rVal.collation.Collation {
		lVal, rVal, err = mergeCollations(lVal, rVal)
		if err != nil {
			return 0, false, err
		}
	}
	return evalCompare(lVal, rVal)
}

func evalCoerceAndCompareNullSafe(lVal, rVal EvalResult) (comp int, err error) {
	if lVal.collation.Collation != rVal.collation.Collation {
		lVal, rVal, err = mergeCollations(lVal, rVal)
		if err != nil {
			return 0, err
		}
	}
	return evalCompareNullSafe(lVal, rVal)
}

func evalTypecheckTuples(lVal, rVal EvalResult) (bool, error) {
	switch {
	case lVal.typ == querypb.Type_TUPLE && rVal.typ == querypb.Type_TUPLE:
		return true, nil
	case lVal.typ == querypb.Type_TUPLE:
		return false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", len(*lVal.tuple))
	case rVal.typ == querypb.Type_TUPLE:
		return false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", 1)
	default:
		return false, nil
	}
}

func evalCompareNullSafe(lVal, rVal EvalResult) (cmp int, err error) {
	tuple, err := evalTypecheckTuples(lVal, rVal)
	if err != nil {
		return 0, err
	}
	if tuple {
		return evalCompareTuplesNullSafe(lVal, rVal)
	}
	if hasNullEvalResult(lVal, rVal) {
		if lVal.typ == rVal.typ {
			return 0, nil
		}
		return 1, nil
	}
	return evalCompare1(lVal, rVal)
}

func evalCompare(lVal, rVal EvalResult) (comp int, isNull bool, err error) {
	tuple, err := evalTypecheckTuples(lVal, rVal)
	if err != nil {
		return 0, false, err
	}
	if tuple {
		return evalCompareTuples(lVal, rVal)
	}
	if hasNullEvalResult(lVal, rVal) {
		return 0, true, nil
	}
	comp, err = evalCompare1(lVal, rVal)
	return comp, false, err
}

// For more details on comparison expression evaluation and type conversion:
// 		- https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
func evalCompare1(lVal, rVal EvalResult) (comp int, err error) {
	switch {
	case evalResultsAreStrings(lVal, rVal):
		return compareStrings(lVal, rVal), nil

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

	case lVal.typ == querypb.Type_TUPLE || rVal.typ == querypb.Type_TUPLE:
		panic("evalCompare1: tuple comparison should be handled early")

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

func evalCompareTuplesNullSafe(lVal EvalResult, rVal EvalResult) (int, error) {
	if len(*lVal.tuple) != len(*rVal.tuple) {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", len(*lVal.tuple))
	}
	for idx, lResult := range *lVal.tuple {
		rResult := (*rVal.tuple)[idx]
		res, err := evalCoerceAndCompareNullSafe(lResult, rResult)
		if res != 0 || err != nil {
			return res, err
		}
	}
	return 0, nil
}

func evalCompareTuples(lVal EvalResult, rVal EvalResult) (int, bool, error) {
	if len(*lVal.tuple) != len(*rVal.tuple) {
		return 0, false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", len(*lVal.tuple))
	}
	hasSeenNull := false
	for idx, lResult := range *lVal.tuple {
		rResult := (*rVal.tuple)[idx]
		res, isNull, err := evalCoerceAndCompare(lResult, rResult)
		if isNull {
			hasSeenNull = true
		}
		if res != 0 || err != nil {
			return res, false, err
		}
	}
	return 0, hasSeenNull, nil
}

var mysql8 = true

func evalResultBool(b bool) EvalResult {
	var typ = sqltypes.Int64
	if mysql8 {
		typ = sqltypes.Uint64
	}
	if b {
		return EvalResult{typ: typ, collation: collationNumeric, numval: 1}
	}
	return EvalResult{typ: typ, collation: collationNumeric, numval: 0}
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
	return c.Op.Type(), nil
}

// Evaluate implements the ComparisonOp interface
func (e *EqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	// No need to coerce here because the caller ComparisonExpr.Evaluate has coerced for us
	numeric, isNull, err := evalCompare(left, right)
	if err != nil {
		return EvalResult{}, err
	}
	if isNull {
		return resultNull, err
	}
	return evalResultBool(e.Compare(numeric)), nil
}

// Type implements the ComparisonOp interface
func (e *EqualOp) Type() querypb.Type {
	return querypb.Type_UINT64
}

// String implements the ComparisonOp interface
func (e *EqualOp) String() string {
	return e.Operator
}

// Evaluate implements the ComparisonOp interface
func (n *NullSafeEqualOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	cmp, err := evalCompareNullSafe(left, right)
	if err != nil {
		return EvalResult{}, err
	}
	return evalResultBool(cmp == 0), nil
}

// Type implements the ComparisonOp interface
func (n *NullSafeEqualOp) Type() querypb.Type {
	return querypb.Type_UINT64
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
		if left.typ == querypb.Type_TUPLE {
			for _, rtuple := range *right.tuple {
				if _, err := evalTypecheckTuples(left, rtuple); err != nil {
					return EvalResult{}, err
				}
			}
			panic("should have failed typecheck for tuple")
		}
		hash, err := left.nullSafeHashcode()
		if err != nil {
			return EvalResult{}, err
		}
		if idx, ok := i.Hashed[hash]; ok {
			var numeric int
			numeric, foundNull, err = evalCoerceAndCompare(left, (*right.tuple)[idx])
			if err != nil {
				return EvalResult{}, err
			}
			found = numeric == 0
		}
	} else {
		for _, rtuple := range *right.tuple {
			if found {
				if _, err := evalTypecheckTuples(left, rtuple); err != nil {
					return EvalResult{}, err
				}
				continue
			}

			numeric, isNull, err := evalCoerceAndCompare(left, rtuple)
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

	boolResult := func(result, negate bool) EvalResult {
		// results from IN operations are always Int64 in MySQL 5.7 and 8+
		if result == !negate {
			return EvalResult{typ: sqltypes.Int64, collation: collationNumeric, numval: 1}
		}
		return EvalResult{typ: sqltypes.Int64, collation: collationNumeric, numval: 0}
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
	return querypb.Type_INT64
}

// String implements the ComparisonOp interface
func (i *InOp) String() string {
	if i.Negate {
		return "not in"
	}
	return "in"
}

func (l *LikeOp) matchWildcard(left, right []byte, coll collations.ID) bool {
	if l.Match != nil {
		return l.Match.Match(left)
	} else {
		coll := collations.Local().LookupByID(coll)
		wc := coll.Wildcard(right, 0, 0, 0)
		return wc.Match(left)
	}
}

func (l *LikeOp) Evaluate(left, right EvalResult) (EvalResult, error) {
	var matched bool

	switch {
	case left.typ == querypb.Type_TUPLE || right.typ == querypb.Type_TUPLE:
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain 1 column(s)")

	case left.textual() && right.textual():
		if left.collation.Collation != right.collation.Collation {
			panic(fmt.Sprintf("LikeOp: did not coerce, left=%d right=%d",
				left.collation.Collation, right.collation.Collation))
		}
		matched = l.matchWildcard(left.bytes, right.bytes, right.collation.Collation)

	case right.textual():
		matched = l.matchWildcard(left.Value().Raw(), right.bytes, right.collation.Collation)

	case left.textual():
		matched = l.matchWildcard(left.bytes, right.Value().Raw(), left.collation.Collation)

	case hasNullEvalResult(left, right):
		return resultNull, nil

	default:
		matched = l.matchWildcard(left.Value().Raw(), right.Value().Raw(), collations.CollationBinaryID)
	}

	return evalResultBool(matched == !l.Negate), nil
}

// Type implements the ComparisonOp interface
func (l *LikeOp) Type() querypb.Type {
	return querypb.Type_UINT64
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
	return querypb.Type_UINT64
}

// String implements the ComparisonOp interface
func (r *RegexpOp) String() string {
	return "regexp"
}
