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
		String() string
		compare(left, right *EvalResult) (boolean, error)
	}

	compareEQ         struct{}
	compareNE         struct{}
	compareLT         struct{}
	compareLE         struct{}
	compareGT         struct{}
	compareGE         struct{}
	compareNullSafeEQ struct{}
)

func (compareEQ) String() string { return "=" }
func (compareEQ) compare(left, right *EvalResult) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, true)
	return makeboolean2(cmp == 0, isNull), err
}

func (compareNE) String() string { return "!=" }
func (compareNE) compare(left, right *EvalResult) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, true)
	return makeboolean2(cmp != 0, isNull), err
}

func (compareLT) String() string { return "<" }
func (compareLT) compare(left, right *EvalResult) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false)
	return makeboolean2(cmp < 0, isNull), err
}

func (compareLE) String() string { return "<=" }
func (compareLE) compare(left, right *EvalResult) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false)
	return makeboolean2(cmp <= 0, isNull), err
}

func (compareGT) String() string { return ">" }
func (compareGT) compare(left, right *EvalResult) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false)
	return makeboolean2(cmp > 0, isNull), err
}

func (compareGE) String() string { return ">=" }
func (compareGE) compare(left, right *EvalResult) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false)
	return makeboolean2(cmp >= 0, isNull), err
}

func (compareNullSafeEQ) String() string { return "<=>" }
func (compareNullSafeEQ) compare(left, right *EvalResult) (boolean, error) {
	cmp, err := evalCompareNullSafe(left, right)
	return makeboolean(cmp), err
}

func (c *BinaryCoercedExpr) collation() collations.TypedCollation {
	// the collation of a binary operation is always integer, not the shared collation
	// between the two subexpressions
	return collationNumeric
}

func (c *BinaryCoercedExpr) coerce() error {
	var err error

	leftColl := c.Left.collation()
	rightColl := c.Right.collation()

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

func evalResultsAreStrings(l, r *EvalResult) bool {
	return l.textual() && r.textual()
}

func evalResultsAreSameNumericType(l, r *EvalResult) bool {
	ltype := l.typeof()
	rtype := r.typeof()
	if sqltypes.IsIntegral(ltype) && sqltypes.IsIntegral(rtype) {
		return true
	}
	if sqltypes.IsFloat(ltype) && sqltypes.IsFloat(rtype) {
		return true
	}
	if ltype == sqltypes.Decimal && rtype == sqltypes.Decimal {
		return true
	}
	return false
}

func needsDecimalHandling(l, r *EvalResult) bool {
	ltype := l.typeof()
	rtype := r.typeof()
	return ltype == sqltypes.Decimal && (sqltypes.IsIntegral(rtype) || sqltypes.IsFloat(rtype)) ||
		rtype == sqltypes.Decimal && (sqltypes.IsIntegral(ltype) || sqltypes.IsFloat(ltype))
}

func evalResultsAreDates(l, r *EvalResult) bool {
	return sqltypes.IsDate(l.typeof()) && sqltypes.IsDate(r.typeof())
}

func evalResultsAreDateAndString(l, r *EvalResult) bool {
	ltype := l.typeof()
	rtype := r.typeof()
	return (sqltypes.IsDate(ltype) && r.textual()) || (l.textual() && sqltypes.IsDate(rtype))
}

func evalResultsAreDateAndNumeric(l, r *EvalResult) bool {
	ltype := l.typeof()
	rtype := r.typeof()
	return sqltypes.IsDate(ltype) && sqltypes.IsNumber(rtype) || sqltypes.IsNumber(ltype) && sqltypes.IsDate(rtype)
}

func evalCoerceAndCompare(lVal, rVal *EvalResult, fulleq bool) (int, bool, error) {
	if lVal.collation().Collation != rVal.collation().Collation {
		if err := mergeCollations(lVal, rVal); err != nil {
			return 0, false, err
		}
	}
	return evalCompareAll(lVal, rVal, fulleq)
}

func evalCoerceAndCompareNullSafe(lVal, rVal *EvalResult) (bool, error) {
	if lVal.collation().Collation != rVal.collation().Collation {
		if err := mergeCollations(lVal, rVal); err != nil {
			return false, err
		}
	}
	return evalCompareNullSafe(lVal, rVal)
}

func compareAsTuples(lVal, rVal *EvalResult) bool {
	left := lVal.typeof() == querypb.Type_TUPLE
	right := rVal.typeof() == querypb.Type_TUPLE
	if left && right {
		return true
	}
	if left || right {
		panic("did not typecheck tuples")
	}
	return false
}

func evalCompareNullSafe(lVal, rVal *EvalResult) (bool, error) {
	if lVal.null() || rVal.null() {
		return lVal.null() == rVal.null(), nil
	}
	if compareAsTuples(lVal, rVal) {
		return evalCompareTuplesNullSafe(lVal, rVal)
	}
	n, err := evalCompare(lVal, rVal)
	return n == 0, err
}

func evalCompareMany(left, right []EvalResult, fulleq bool) (int, bool, error) {
	// For row comparisons, (a, b) = (x, y) is equivalent to: (a = x) AND (b = y)
	var seenNull bool
	for idx, lResult := range left {
		rResult := right[idx]
		n, isNull, err := evalCoerceAndCompare(&lResult, &rResult, fulleq)
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

func evalCompareAll(lVal, rVal *EvalResult, fulleq bool) (int, bool, error) {
	if lVal.null() || rVal.null() {
		return 0, true, nil
	}
	if compareAsTuples(lVal, rVal) {
		return evalCompareMany(lVal.tuple(), rVal.tuple(), fulleq)
	}
	n, err := evalCompare(lVal, rVal)
	return n, false, err
}

// For more details on comparison expression evaluation and type conversion:
// 		- https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
func evalCompare(lVal, rVal *EvalResult) (comp int, err error) {
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

	case lVal.typeof() == querypb.Type_TUPLE || rVal.typeof() == querypb.Type_TUPLE:
		panic("evalCompare: tuple comparison should be handled early")

	default:
		// Quoting MySQL Docs:
		//
		// 		"In all other cases, the arguments are compared as floating-point (real) numbers.
		// 		For example, a comparison of string and numeric operands takes place as a
		// 		comparison of floating-point numbers."
		//
		//		https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
		lVal.makeFloat()
		rVal.makeFloat()
		return compareNumeric(lVal, rVal)
	}
}

func evalCompareTuplesNullSafe(lVal, rVal *EvalResult) (bool, error) {
	left := lVal.tuple()
	right := rVal.tuple()
	if len(left) != len(right) {
		panic("did not typecheck cardinality")
	}
	for idx, lResult := range left {
		rResult := &right[idx]
		res, err := evalCoerceAndCompareNullSafe(&lResult, rResult)
		if err != nil {
			return false, err
		}
		if !res {
			return false, nil
		}
	}
	return true, nil
}

// eval implements the Expr interface
func (c *ComparisonExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var left, right EvalResult
	left.init(env, c.Left)
	right.init(env, c.Right)
	cmp, err := c.Op.compare(&left, &right)
	if err != nil {
		throwEvalError(err)
	}
	result.setBoolean(cmp)
}

// typeof implements the Expr interface
func (c *ComparisonExpr) typeof(*ExpressionEnv) querypb.Type {
	// TODO: make this less aggressive
	return -1
}

// eval implements the ComparisonOp interface
func (i *InExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var left, right EvalResult
	left.init(env, i.Left)
	right.init(env, i.Right)

	if right.typeof() != querypb.Type_TUPLE {
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple"))
	}

	var foundNull, found bool
	var righttuple = right.tuple()

	if i.Hashed != nil {
		hash, err := left.nullSafeHashcode()
		if err != nil {
			throwEvalError(err)
		}
		if idx, ok := i.Hashed[hash]; ok {
			var numeric int
			numeric, foundNull, err = evalCoerceAndCompare(&left, &righttuple[idx], true)
			if err != nil {
				throwEvalError(err)
			}
			found = numeric == 0
		}
	} else {
		for _, rtuple := range righttuple {
			numeric, isNull, err := evalCoerceAndCompare(&left, &rtuple, true)
			if err != nil {
				throwEvalError(err)
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

	boolResult := func(b, negate bool, result *EvalResult) {
		// results from IN operations are always Int64 in MySQL 5.7 and 8+
		if b == !negate {
			result.setInt64(1)
		} else {
			result.setInt64(0)
		}
	}

	if found {
		boolResult(found, i.Negate, result)
		return
	}
	if foundNull {
		result.setNull()
		return
	}
	boolResult(found, i.Negate, result)
}

func (i *InExpr) typeof(env *ExpressionEnv) querypb.Type {
	// TODO: make this less aggressive
	// return querypb.Type_INT64, nil
	return -1
}

func (i *InExpr) collation() collations.TypedCollation {
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

func (l *LikeExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var left, right EvalResult
	left.init(env, l.Left)
	right.init(env, l.Right)

	var matched bool
	var leftcoll = left.collation()
	var rightcoll = right.collation()

	switch {
	case left.typeof() == querypb.Type_TUPLE || right.typeof() == querypb.Type_TUPLE:
		panic("failed to typecheck tuples")

	case left.null() || right.null():
		result.setNull()
		return

	case left.textual() && right.textual():
		if leftcoll.Collation != rightcoll.Collation {
			panic(fmt.Sprintf("LikeOp: did not coerce, left=%d right=%d",
				leftcoll.Collation, rightcoll.Collation))
		}
		matched = l.matchWildcard(left.bytes(), right.bytes(), rightcoll.Collation)

	case right.textual():
		matched = l.matchWildcard(left.value().Raw(), right.bytes(), rightcoll.Collation)

	case left.textual():
		matched = l.matchWildcard(left.bytes(), right.value().Raw(), leftcoll.Collation)

	default:
		matched = l.matchWildcard(left.value().Raw(), right.value().Raw(), collations.CollationBinaryID)
	}

	result.setBool(matched == !l.Negate)
}

// typeof implements the ComparisonOp interface
func (l *LikeExpr) typeof(env *ExpressionEnv) querypb.Type {
	// TODO: make this less aggressive
	// return querypb.Type_UINT64, nil
	return -1
}

func (l *LikeExpr) collation() collations.TypedCollation {
	return collationNumeric
}
