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
	"bytes"
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	Filter interface {
		Expr
		LeftExpr() Expr
		RightExpr() Expr
		filterExpr()
	}

	ComparisonExpr struct {
		BinaryExpr
		Op ComparisonOp
	}

	LikeExpr struct {
		BinaryExpr
		Negate         bool
		Match          collations.WildcardPattern
		MatchCollation collations.ID
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

func (*ComparisonExpr) filterExpr() {}
func (*InExpr) filterExpr()         {}

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

func evalResultsAreStrings(l, r *EvalResult) bool {
	return l.isTextual() && r.isTextual()
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
	return (sqltypes.IsDate(ltype) && r.isTextual()) || (l.isTextual() && sqltypes.IsDate(rtype))
}

func evalResultsAreDateAndNumeric(l, r *EvalResult) bool {
	ltype := l.typeof()
	rtype := r.typeof()
	return sqltypes.IsDate(ltype) && sqltypes.IsNumber(rtype) || sqltypes.IsNumber(ltype) && sqltypes.IsDate(rtype)
}

func compareAsTuples(lVal, rVal *EvalResult) bool {
	left := lVal.typeof() == sqltypes.Tuple
	right := rVal.typeof() == sqltypes.Tuple
	if right && left {
		return true
	}
	if left || right {
		panic("did not typecheck tuples")
	}
	return false
}

func evalCompareNullSafe(lVal, rVal *EvalResult) (bool, error) {
	if lVal.isNull() || rVal.isNull() {
		return lVal.isNull() == rVal.isNull(), nil
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
		n, isNull, err := evalCompareAll(&lResult, &rResult, fulleq)
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
	if lVal.isNull() || rVal.isNull() {
		return 0, true, nil
	}
	if compareAsTuples(lVal, rVal) {
		return evalCompareMany(lVal.tuple(), rVal.tuple(), fulleq)
	}
	n, err := evalCompare(lVal, rVal)
	return n, false, err
}

// For more details on comparison expression evaluation and type conversion:
//   - https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
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
	case lVal.typeof() == sqltypes.Tuple || rVal.typeof() == sqltypes.Tuple:
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: evalCompare: tuple comparison should be handled early")
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
		res, err := evalCompareNullSafe(&lResult, rResult)
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
func (c *ComparisonExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	_, f1 := c.Left.typeof(env)
	_, f2 := c.Right.typeof(env)
	return sqltypes.Int64, f1 | f2
}

// eval implements the ComparisonOp interface
func (i *InExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var left, right EvalResult
	left.init(env, i.Left)
	right.init(env, i.Right)

	if right.typeof() != sqltypes.Tuple {
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple"))
	}
	if left.isNull() {
		result.setNull()
		return
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
			numeric, foundNull, err = evalCompareAll(&left, &righttuple[idx], true)
			if err != nil {
				throwEvalError(err)
			}
			found = numeric == 0
		}
	} else {
		for _, rtuple := range righttuple {
			numeric, isNull, err := evalCompareAll(&left, &rtuple, true)
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

	switch {
	case found:
		result.setBool(!i.Negate)
	case foundNull:
		result.setNull()
	default:
		result.setBool(i.Negate)
	}
}

func (i *InExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	_, f1 := i.Left.typeof(env)
	_, f2 := i.Right.typeof(env)
	return sqltypes.Int64, f1 | f2
}

func (l *LikeExpr) matchWildcard(left, right []byte, coll collations.ID) bool {
	if l.Match != nil && l.MatchCollation == coll {
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

	coll, err := mergeCollations(&left, &right)
	if err != nil {
		throwEvalError(err)
	}

	var matched bool
	switch {
	case left.typeof() == sqltypes.Tuple || right.typeof() == sqltypes.Tuple:
		panic("failed to typecheck tuples")
	case left.isNull() || right.isNull():
		result.setNull()
		return
	case left.isTextual() && right.isTextual():
		matched = l.matchWildcard(left.bytes(), right.bytes(), coll)
	case right.isTextual():
		matched = l.matchWildcard(left.value().Raw(), right.bytes(), coll)
	case left.isTextual():
		matched = l.matchWildcard(left.bytes(), right.value().Raw(), coll)
	default:
		matched = l.matchWildcard(left.value().Raw(), right.value().Raw(), collations.CollationBinaryID)
	}

	result.setBool(matched == !l.Negate)
}

// typeof implements the ComparisonOp interface
func (l *LikeExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	_, f1 := l.Left.typeof(env)
	_, f2 := l.Right.typeof(env)
	return sqltypes.Int64, f1 | f2
}

// UnsupportedComparisonError represents the error where the comparison between the two types is unsupported on vitess
type UnsupportedComparisonError struct {
	Type1 sqltypes.Type
	Type2 sqltypes.Type
}

// Error function implements the error interface
func (err UnsupportedComparisonError) Error() string {
	return fmt.Sprintf("types are not comparable: %v vs %v", err.Type1, err.Type2)
}

// UnsupportedCollationError represents the error where the comparison using provided collation is unsupported on vitess
type UnsupportedCollationError struct {
	ID collations.ID
}

// Error function implements the error interface
func (err UnsupportedCollationError) Error() string {
	return fmt.Sprintf("cannot compare strings, collation is unknown or unsupported (collation ID: %d)", err.ID)
}

// UnsupportedCollationHashError is returned when we try to get the hash value and are missing the collation to use
var UnsupportedCollationHashError = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "text type with an unknown/unsupported collation cannot be hashed")

// NullsafeCompare returns 0 if v1==v2, -1 if v1<v2, and 1 if v1>v2.
// NULL is the lowest value. If any value is
// numeric, then a numeric comparison is performed after
// necessary conversions. If none are numeric, then it's
// a simple binary comparison. Uncomparable values return an error.
func NullsafeCompare(v1, v2 sqltypes.Value, collationID collations.ID) (int, error) {
	// Based on the categorization defined for the types,
	// we're going to allow comparison of the following:
	// Null, isNumber, IsBinary. This will exclude IsQuoted
	// types that are not Binary, and Expression.
	if v1.IsNull() {
		if v2.IsNull() {
			return 0, nil
		}
		return -1, nil
	}
	if v2.IsNull() {
		return 1, nil
	}

	if isByteComparable(v1.Type(), collationID) && isByteComparable(v2.Type(), collationID) {
		return bytes.Compare(v1.Raw(), v2.Raw()), nil
	}

	typ, err := CoerceTo(v1.Type(), v2.Type()) // TODO systay we should add a method where this decision is done at plantime
	if err != nil {
		return 0, err
	}

	switch {
	case sqltypes.IsText(typ):
		if collationID == collations.Unknown {
			return 0, UnsupportedCollationError{ID: collationID}
		}
		collation := collations.Local().LookupByID(collationID)
		if collation == nil {
			return 0, UnsupportedCollationError{ID: collationID}
		}

		v1Bytes, err := v1.ToBytes()
		if err != nil {
			return 0, err
		}
		v2Bytes, err := v2.ToBytes()
		if err != nil {
			return 0, err
		}

		switch result := collation.Collate(v1Bytes, v2Bytes, false); {
		case result < 0:
			return -1, nil
		case result > 0:
			return 1, nil
		default:
			return 0, nil
		}

	case sqltypes.IsNumber(typ):
		v1cast := borrowEvalResult()
		v2cast := borrowEvalResult()

		defer func() {
			v1cast.unborrow()
			v2cast.unborrow()
		}()

		if err := v1cast.setValueCast(v1, typ); err != nil {
			return 0, err
		}
		if err := v2cast.setValueCast(v2, typ); err != nil {
			return 0, err
		}
		return compareNumeric(v1cast, v2cast)

	default:
		return 0, UnsupportedComparisonError{Type1: v1.Type(), Type2: v2.Type()}
	}
}

// isByteComparable returns true if the type is binary or date/time.
func isByteComparable(typ sqltypes.Type, collationID collations.ID) bool {
	if sqltypes.IsBinary(typ) {
		return true
	}
	if sqltypes.IsText(typ) {
		return collationID == collations.CollationBinaryID
	}
	switch typ {
	case sqltypes.Timestamp, sqltypes.Date, sqltypes.Time, sqltypes.Datetime, sqltypes.Enum, sqltypes.Set, sqltypes.TypeJSON, sqltypes.Bit:
		return true
	default:
		return false
	}
}

// Min returns the minimum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Min(v1, v2 sqltypes.Value, collation collations.ID) (sqltypes.Value, error) {
	return minmax(v1, v2, true, collation)
}

// Max returns the maximum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Max(v1, v2 sqltypes.Value, collation collations.ID) (sqltypes.Value, error) {
	return minmax(v1, v2, false, collation)
}

func minmax(v1, v2 sqltypes.Value, min bool, collation collations.ID) (sqltypes.Value, error) {
	if v1.IsNull() {
		return v2, nil
	}
	if v2.IsNull() {
		return v1, nil
	}

	n, err := NullsafeCompare(v1, v2, collation)
	if err != nil {
		return sqltypes.NULL, err
	}

	// XNOR construct. See tests.
	v1isSmaller := n < 0
	if min == v1isSmaller {
		return v1, nil
	}
	return v2, nil
}
