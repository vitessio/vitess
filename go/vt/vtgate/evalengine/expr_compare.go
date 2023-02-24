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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

type (
	FilterExpr interface {
		BinaryExpr
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
		Hashed map[vthash.Hash]int
	}

	ComparisonOp interface {
		String() string
		compare(left, right eval) (boolean, error)
	}

	compareEQ         struct{}
	compareNE         struct{}
	compareLT         struct{}
	compareLE         struct{}
	compareGT         struct{}
	compareGE         struct{}
	compareNullSafeEQ struct{}
)

var _ Expr = (*ComparisonExpr)(nil)
var _ Expr = (*InExpr)(nil)
var _ Expr = (*LikeExpr)(nil)

func (*ComparisonExpr) filterExpr() {}
func (*InExpr) filterExpr()         {}

func (compareEQ) String() string { return "=" }
func (compareEQ) compare(left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, true)
	return makeboolean2(cmp == 0, isNull), err
}

func (compareNE) String() string { return "!=" }
func (compareNE) compare(left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, true)
	return makeboolean2(cmp != 0, isNull), err
}

func (compareLT) String() string { return "<" }
func (compareLT) compare(left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false)
	return makeboolean2(cmp < 0, isNull), err
}

func (compareLE) String() string { return "<=" }
func (compareLE) compare(left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false)
	return makeboolean2(cmp <= 0, isNull), err
}

func (compareGT) String() string { return ">" }
func (compareGT) compare(left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false)
	return makeboolean2(cmp > 0, isNull), err
}

func (compareGE) String() string { return ">=" }
func (compareGE) compare(left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false)
	return makeboolean2(cmp >= 0, isNull), err
}

func (compareNullSafeEQ) String() string { return "<=>" }
func (compareNullSafeEQ) compare(left, right eval) (boolean, error) {
	cmp, err := evalCompareNullSafe(left, right)
	return makeboolean(cmp), err
}

func evalResultIsTextual(e eval) bool {
	tt := e.SQLType()
	return sqltypes.IsText(tt) || sqltypes.IsBinary(tt)
}

func evalResultsAreStrings(l, r eval) bool {
	return evalResultIsTextual(l) && evalResultIsTextual(r)
}

func evalResultsAreSameNumericType(l, r eval) bool {
	ltype := l.SQLType()
	rtype := r.SQLType()
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

func needsDecimalHandling(l, r eval) bool {
	ltype := l.SQLType()
	rtype := r.SQLType()
	return ltype == sqltypes.Decimal && (sqltypes.IsIntegral(rtype) || sqltypes.IsFloat(rtype)) ||
		rtype == sqltypes.Decimal && (sqltypes.IsIntegral(ltype) || sqltypes.IsFloat(ltype))
}

func evalResultsAreDates(l, r eval) bool {
	return sqltypes.IsDate(l.SQLType()) && sqltypes.IsDate(r.SQLType())
}

func evalResultsAreDateAndString(l, r eval) bool {
	ltype := l.SQLType()
	rtype := r.SQLType()
	return (sqltypes.IsDate(ltype) && evalResultIsTextual(r)) || (evalResultIsTextual(l) && sqltypes.IsDate(rtype))
}

func evalResultsAreDateAndNumeric(l, r eval) bool {
	ltype := l.SQLType()
	rtype := r.SQLType()
	return sqltypes.IsDate(ltype) && sqltypes.IsNumber(rtype) || sqltypes.IsNumber(ltype) && sqltypes.IsDate(rtype)
}

func compareAsTuples(left, right eval) (*evalTuple, *evalTuple, bool) {
	if left, ok := left.(*evalTuple); ok {
		if right, ok := right.(*evalTuple); ok {
			return left, right, true
		}
	}
	return nil, nil, false
}

func evalCompareNullSafe(lVal, rVal eval) (bool, error) {
	if lVal == nil || rVal == nil {
		return lVal == rVal, nil
	}
	if left, right, ok := compareAsTuples(lVal, rVal); ok {
		return evalCompareTuplesNullSafe(left.t, right.t)
	}
	n, err := evalCompare(lVal, rVal)
	return n == 0, err
}

func evalCompareMany(left, right []eval, fulleq bool) (int, bool, error) {
	// For row comparisons, (a, b) = (x, y) is equivalent to: (a = x) AND (b = y)
	var seenNull bool
	for idx, lResult := range left {
		rResult := right[idx]
		n, isNull, err := evalCompareAll(lResult, rResult, fulleq)
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

func evalCompareAll(lVal, rVal eval, fulleq bool) (int, bool, error) {
	if lVal == nil || rVal == nil {
		return 0, true, nil
	}
	if left, right, ok := compareAsTuples(lVal, rVal); ok {
		return evalCompareMany(left.t, right.t, fulleq)
	}
	n, err := evalCompare(lVal, rVal)
	return n, false, err
}

// For more details on comparison expression evaluation and type conversion:
//   - https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
func evalCompare(left, right eval) (comp int, err error) {
	switch {
	case evalResultsAreStrings(left, right):
		return compareStrings(left, right)
	case evalResultsAreSameNumericType(left, right), needsDecimalHandling(left, right):
		return compareNumeric(left, right)
	case evalResultsAreDates(left, right):
		return compareDates(left, right)
	case evalResultsAreDateAndString(left, right):
		return compareDateAndString(left, right)
	case evalResultsAreDateAndNumeric(left, right):
		// TODO: support comparison between a date and a numeric value
		// 		queries like the ones below should be supported:
		// 			- select 1 where 20210101 = cast("2021-01-01" as date)
		// 			- select 1 where 2021210101 = cast("2021-01-01" as date)
		// 			- select 1 where 104200 = cast("10:42:00" as time)
		return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot compare a date with a numeric value")
	case left.SQLType() == sqltypes.Tuple || right.SQLType() == sqltypes.Tuple:
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: evalCompare: tuple comparison should be handled early")
	default:
		// Quoting MySQL Docs:
		//
		// 		"In all other cases, the arguments are compared as floating-point (real) numbers.
		// 		For example, a comparison of string and numeric operands takes place as a
		// 		comparison of floating-point numbers."
		//
		//		https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
		lf, _ := evalToNumeric(left).toFloat()
		rf, _ := evalToNumeric(right).toFloat()
		return compareNumeric(lf, rf)
	}
}

func evalCompareTuplesNullSafe(left, right []eval) (bool, error) {
	if len(left) != len(right) {
		panic("did not typecheck cardinality")
	}
	for idx, lResult := range left {
		res, err := evalCompareNullSafe(lResult, right[idx])
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
func (c *ComparisonExpr) eval(env *ExpressionEnv) (eval, error) {
	left, right, err := c.arguments(env)
	if err != nil {
		return nil, err
	}
	cmp, err := c.Op.compare(left, right)
	if err != nil {
		return nil, err
	}
	return cmp.eval(), nil
}

// typeof implements the Expr interface
func (c *ComparisonExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f1 := c.Left.typeof(env)
	_, f2 := c.Right.typeof(env)
	return sqltypes.Int64, f1 | f2
}

// eval implements the ComparisonOp interface
func (i *InExpr) eval(env *ExpressionEnv) (eval, error) {
	left, right, err := i.arguments(env)
	if err != nil {
		return nil, err
	}
	rtuple, ok := right.(*evalTuple)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple")
	}
	if left == nil {
		return nil, nil
	}

	var foundNull, found bool
	var hasher = vthash.New()
	if i.Hashed != nil {
		if left, ok := left.(hashable); ok {
			left.Hash(&hasher)

			hash := hasher.Sum128()
			hasher.Reset()

			if idx, ok := i.Hashed[hash]; ok {
				var numeric int
				numeric, foundNull, err = evalCompareAll(left, rtuple.t[idx], true)
				if err != nil {
					return nil, err
				}
				found = numeric == 0
			}
		}
	} else {
		for _, rtuple := range rtuple.t {
			numeric, isNull, err := evalCompareAll(left, rtuple, true)
			if err != nil {
				return nil, err
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
		return newEvalBool(!i.Negate), nil
	case foundNull:
		return nil, nil
	default:
		return newEvalBool(i.Negate), nil
	}
}

func (i *InExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
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

func (l *LikeExpr) eval(env *ExpressionEnv) (eval, error) {
	left, right, err := l.arguments(env)
	if left == nil || right == nil || err != nil {
		return nil, err
	}

	var col collations.ID
	left, right, col, err = mergeCollations(left, right)
	if err != nil {
		return nil, err
	}

	var matched bool
	switch {
	case evalResultIsTextual(left) && evalResultIsTextual(right):
		matched = l.matchWildcard(left.(*evalBytes).bytes, right.(*evalBytes).bytes, col)
	case evalResultIsTextual(right):
		matched = l.matchWildcard(left.ToRawBytes(), right.(*evalBytes).bytes, col)
	case evalResultIsTextual(left):
		matched = l.matchWildcard(left.(*evalBytes).bytes, right.ToRawBytes(), col)
	default:
		matched = l.matchWildcard(left.ToRawBytes(), right.ToRawBytes(), collations.CollationBinaryID)
	}
	return newEvalBool(matched == !l.Negate), nil
}

// typeof implements the ComparisonOp interface
func (l *LikeExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f1 := l.Left.typeof(env)
	_, f2 := l.Right.typeof(env)
	return sqltypes.Int64, f1 | f2
}
