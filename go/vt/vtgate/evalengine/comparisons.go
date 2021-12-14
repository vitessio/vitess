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
	BinaryCoercedExpr struct {
		BinaryExpr
		CoerceLeft, CoerceRight collations.Coercion
		MergedCollation         collations.TypedCollation
	}

	ComparisonExpr struct {
		BinaryCoercedExpr
		Op ComparisonOp
	}

	NullSafeComparisonExpr struct {
		BinaryCoercedExpr
	}

	LikeExpr struct {
		BinaryCoercedExpr
		Negate bool
		Match  collations.WildcardPattern
	}

	InExpr struct {
		BinaryExpr
		Negate bool
		Hashed map[uintptr]int
	}

	ComparisonOp interface {
		fulleq() bool
		resolve(int) bool
		String() string
	}

	compareEQ struct{}
	compareNE struct{}
	compareLT struct{}
	compareLE struct{}
	compareGT struct{}
	compareGE struct{}
)

var (
	resultNull = EvalResult{typ: sqltypes.Null, collation: collationNull}
)

func (compareEQ) resolve(n int) bool { return n == 0 }
func (compareEQ) fulleq() bool       { return true }
func (compareEQ) String() string     { return "=" }

func (compareNE) resolve(n int) bool { return n != 0 }
func (compareNE) fulleq() bool       { return true }
func (compareNE) String() string     { return "!=" }

func (compareLT) resolve(n int) bool { return n < 0 }
func (compareLT) fulleq() bool       { return false }
func (compareLT) String() string     { return "<" }

func (compareLE) resolve(n int) bool { return n <= 0 }
func (compareLE) fulleq() bool       { return false }
func (compareLE) String() string     { return "<=" }

func (compareGT) resolve(n int) bool { return n > 0 }
func (compareGT) fulleq() bool       { return false }
func (compareGT) String() string     { return ">" }

func (compareGE) resolve(n int) bool { return n >= 0 }
func (compareGE) fulleq() bool       { return false }
func (compareGE) String() string     { return ">=" }

func (c *BinaryCoercedExpr) Collation() collations.TypedCollation {
	// the collation of a binary operation is always integer, not the shared collation
	// between the two subexpressions
	return collationNumeric
}

func (c *BinaryCoercedExpr) coerce() error {
	var err error

	leftColl := c.Left.Collation()
	rightColl := c.Right.Collation()

	if leftColl.Valid() && rightColl.Valid() {
		env := collations.Local()
		c.MergedCollation, c.CoerceLeft, c.CoerceRight, err =
			env.MergeCollations(leftColl, rightColl, collations.CoercionOptions{
				ConvertToSuperset:   true,
				ConvertWithCoercion: true,
			})
	}
	return err
}

func (expr *BinaryExpr) evalInner(env *ExpressionEnv) (lVal EvalResult, rVal EvalResult, err error) {
	if lVal, err = expr.Left.eval(env); err != nil {
		return
	}
	if rVal, err = expr.Right.eval(env); err != nil {
		return
	}
	return
}

func (expr *BinaryCoercedExpr) evalInner(env *ExpressionEnv) (lVal EvalResult, rVal EvalResult, err error) {
	lVal, rVal, err = expr.BinaryExpr.evalInner(env)
	if err != nil {
		return
	}

	// If we have pre-calculated a merged collation for this comparison,
	// coerce both operands here. Otherwise, we'll to the coercion lazily
	// when we have to actually compare the two values.
	if expr.MergedCollation.Valid() {
		lVal.collation = expr.MergedCollation
		rVal.collation = expr.MergedCollation
		if lVal.textual() && expr.CoerceLeft != nil {
			lVal.bytes, _ = expr.CoerceLeft(nil, lVal.bytes)
		}
		if rVal.textual() && expr.CoerceRight != nil {
			rVal.bytes, _ = expr.CoerceRight(nil, rVal.bytes)
		}
	}

	return
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

func evalCoerceAndCompare(lVal, rVal EvalResult, fulleq bool) (int, bool, error) {
	if lVal.collation.Collation != rVal.collation.Collation {
		var err error
		lVal, rVal, err = mergeCollations(lVal, rVal)
		if err != nil {
			return 0, false, err
		}
	}
	return evalCompareAll(lVal, rVal, fulleq)
}

func evalCoerceAndCompareNullSafe(lVal, rVal EvalResult) (bool, error) {
	if lVal.collation.Collation != rVal.collation.Collation {
		var err error
		lVal, rVal, err = mergeCollations(lVal, rVal)
		if err != nil {
			return false, err
		}
	}
	return evalCompareNullSafe(lVal, rVal)
}

func evalCompareNullSafe(lVal, rVal EvalResult) (bool, error) {
	tuple, err := checkTupleCardinality(lVal, rVal)
	if err != nil {
		return false, err
	}
	if tuple {
		return evalCompareTuplesNullSafe(lVal, rVal)
	}
	if hasNullEvalResult(lVal, rVal) {
		return lVal.typ == rVal.typ, nil
	}
	n, err := evalCompare(lVal, rVal)
	return n == 0, err
}

func evalCompareMany(left, right []EvalResult, fulleq bool) (int, bool, error) {
	// For row comparisons, (a, b) = (x, y) is equivalent to: (a = x) AND (b = y)
	var seenNull bool
	for idx, lResult := range left {
		rResult := right[idx]
		n, isNull, err := evalCoerceAndCompare(lResult, rResult, fulleq)
		if err != nil {
			return 0, false, err
		}
		switch {
		case isNull:
			seenNull = true
		case n != 0:
			if fulleq {
				return n, false, nil
			}
			return n, seenNull, nil
		}
	}
	return 0, seenNull, nil
}

func evalCompareAll(lVal, rVal EvalResult, fulleq bool) (int, bool, error) {
	tuple, err := checkTupleCardinality(lVal, rVal)
	if err != nil {
		return 0, false, err
	}
	if tuple {
		return evalCompareMany(*lVal.tuple, *rVal.tuple, fulleq)
	}
	if hasNullEvalResult(lVal, rVal) {
		return 0, true, nil
	}
	n, err := evalCompare(lVal, rVal)
	return n, false, err
}

// For more details on comparison expression evaluation and type conversion:
// 		- https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
func evalCompare(lVal, rVal EvalResult) (comp int, err error) {
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
		panic("evalCompare: tuple comparison should be handled early")

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

func evalCompareTuplesNullSafe(lVal EvalResult, rVal EvalResult) (bool, error) {
	if len(*lVal.tuple) != len(*rVal.tuple) {
		panic("did not typecheck cardinality")
	}
	for idx, lResult := range *lVal.tuple {
		rResult := (*rVal.tuple)[idx]
		res, err := evalCoerceAndCompareNullSafe(lResult, rResult)
		if err != nil {
			return false, err
		}
		if !res {
			return false, nil
		}
	}
	return true, nil
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
func (c *ComparisonExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	lVal, rVal, err := c.evalInner(env)
	if err != nil {
		return EvalResult{}, err
	}
	n, isNull, err := evalCompareAll(lVal, rVal, c.Op.fulleq())
	if isNull {
		return resultNull, nil
	}
	return evalResultBool(c.Op.resolve(n)), nil
}

// Type implements the Expr interface
func (c *ComparisonExpr) Type(*ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_UINT64, nil
}

func (n *NullSafeComparisonExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	lVal, rVal, err := n.evalInner(env)
	if err != nil {
		return EvalResult{}, err
	}
	cmp, err := evalCompareNullSafe(lVal, rVal)
	if err != nil {
		return EvalResult{}, err
	}
	return evalResultBool(cmp), nil
}

// Type implements the ComparisonOp interface
func (n *NullSafeComparisonExpr) Type(env *ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_UINT64, nil
}

// Evaluate implements the ComparisonOp interface
func (i *InExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	left, right, err := i.evalInner(env)
	if err != nil {
		return EvalResult{}, err
	}

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
			numeric, foundNull, err = evalCoerceAndCompare(left, (*right.tuple)[idx], true)
			if err != nil {
				return EvalResult{}, err
			}
			found = numeric == 0
		}
	} else {
		for _, rtuple := range *right.tuple {
			numeric, isNull, err := evalCoerceAndCompare(left, rtuple, true)
			if err != nil {
				return EvalResult{}, err
			}
			if isNull {
				foundNull = true
				continue
			}
			if numeric == 0 {
				found = true
				break
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

func (i *InExpr) Type(env *ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_INT64, nil
}

func (i *InExpr) Collation() collations.TypedCollation {
	return collationNumeric
}

func (l *LikeExpr) matchWildcard(left, right []byte, coll collations.ID) bool {
	if l.Match != nil {
		return l.Match.Match(left)
	}
	fullColl := collations.Local().LookupByID(coll)
	wc := fullColl.Wildcard(right, 0, 0, 0)
	return wc.Match(left)
}

func (l *LikeExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	left, right, err := l.evalInner(env)
	if err != nil {
		return EvalResult{}, err
	}

	var matched bool
	switch {
	case left.typ == querypb.Type_TUPLE || right.typ == querypb.Type_TUPLE:
		return EvalResult{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain 1 column(s)")

	case hasNullEvalResult(left, right):
		return resultNull, nil

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

	default:
		matched = l.matchWildcard(left.Value().Raw(), right.Value().Raw(), collations.CollationBinaryID)
	}

	return evalResultBool(matched == !l.Negate), nil
}

// Type implements the ComparisonOp interface
func (l *LikeExpr) Type(env *ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_UINT64, nil
}

func (l *LikeExpr) Collation() collations.TypedCollation {
	return collationNumeric
}
