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
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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

func typeIsTextual(tt sqltypes.Type) bool {
	return sqltypes.IsText(tt) || sqltypes.IsBinary(tt) || tt == sqltypes.Time
}

func compareAsStrings(l, r sqltypes.Type) bool {
	return typeIsTextual(l) && typeIsTextual(r)
}

func compareAsSameNumericType(l, r sqltypes.Type) bool {
	if sqltypes.IsIntegral(l) && sqltypes.IsIntegral(r) {
		return true
	}
	if sqltypes.IsFloat(l) && sqltypes.IsFloat(r) {
		return true
	}
	if sqltypes.IsDecimal(l) && sqltypes.IsDecimal(r) {
		return true
	}
	return false
}

func compareAsDecimal(ltype, rtype sqltypes.Type) bool {
	return sqltypes.IsDecimal(ltype) && (sqltypes.IsIntegral(rtype) || sqltypes.IsFloat(rtype)) ||
		sqltypes.IsDecimal(rtype) && (sqltypes.IsIntegral(ltype) || sqltypes.IsFloat(ltype))
}

func compareAsDates(l, r sqltypes.Type) bool {
	return sqltypes.IsDateOrTime(l) && sqltypes.IsDateOrTime(r)
}

func compareAsDateAndString(l, r sqltypes.Type) bool {
	return (sqltypes.IsDate(l) && typeIsTextual(r)) || (typeIsTextual(l) && sqltypes.IsDate(r))
}

func compareAsDateAndNumeric(ltype, rtype sqltypes.Type) bool {
	return sqltypes.IsDateOrTime(ltype) && sqltypes.IsNumber(rtype) || sqltypes.IsNumber(ltype) && sqltypes.IsDateOrTime(rtype)
}

func compareAsTuples(left, right eval) (*evalTuple, *evalTuple, bool) {
	if left, ok := left.(*evalTuple); ok {
		if right, ok := right.(*evalTuple); ok {
			return left, right, true
		}
	}
	return nil, nil, false
}

func compareAsJSON(l, r sqltypes.Type) bool {
	return l == sqltypes.TypeJSON || r == sqltypes.TypeJSON
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
	lt := left.SQLType()
	rt := right.SQLType()

	switch {
	case compareAsDates(lt, rt):
		return compareDates(left, right)
	case compareAsStrings(lt, rt):
		return compareStrings(left, right)
	case compareAsSameNumericType(lt, rt) || compareAsDecimal(lt, rt):
		return compareNumeric(left, right)
	case compareAsDateAndString(lt, rt):
		return compareDateAndString(left, right)
	case compareAsDateAndNumeric(lt, rt):
		if sqltypes.IsDateOrTime(lt) {
			left = evalToNumeric(left)
		}
		if sqltypes.IsDateOrTime(rt) {
			right = evalToNumeric(right)
		}
		return compareNumeric(left, right)
	case compareAsJSON(lt, rt):
		return compareJSON(left, right)
	case lt == sqltypes.Tuple || rt == sqltypes.Tuple:
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
	left, err := c.Left.eval(env)
	if err != nil {
		return nil, err
	}
	if _, ok := c.Op.(compareNullSafeEQ); !ok && left == nil {
		return nil, nil
	}
	right, err := c.Right.eval(env)
	if err != nil {
		return nil, err
	}

	if _, ok := c.Op.(compareNullSafeEQ); !ok && right == nil {
		return nil, nil
	}
	cmp, err := c.Op.compare(left, right)
	if err != nil {
		return nil, err
	}
	return cmp.eval(), nil
}

// typeof implements the Expr interface
func (c *ComparisonExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := c.Left.typeof(env, fields)
	_, f2 := c.Right.typeof(env, fields)
	return sqltypes.Int64, f1 | f2
}

func evalInExpr(lhs eval, rhs *evalTuple) (boolean, error) {
	if lhs == nil {
		return boolNULL, nil
	}

	var foundNull, found bool
	for _, rtuple := range rhs.t {
		numeric, isNull, err := evalCompareAll(lhs, rtuple, true)
		if err != nil {
			return boolNULL, err
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

	switch {
	case found:
		return boolTrue, nil
	case foundNull:
		return boolNULL, nil
	default:
		return boolFalse, nil
	}
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
	in, err := evalInExpr(left, rtuple)
	if err != nil {
		return nil, err
	}
	if i.Negate {
		in = in.not()
	}
	return in.eval(), nil
}

func (i *InExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := i.Left.typeof(env, fields)
	_, f2 := i.Right.typeof(env, fields)
	return sqltypes.Int64, f1 | f2
}

func (l *LikeExpr) matchWildcard(left, right []byte, coll collations.ID) bool {
	if l.Match != nil && l.MatchCollation == coll {
		return l.Match.Match(left)
	}
	fullColl := coll.Get()
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
	case typeIsTextual(left.SQLType()) && typeIsTextual(right.SQLType()):
		matched = l.matchWildcard(left.(*evalBytes).bytes, right.(*evalBytes).bytes, col)
	case typeIsTextual(right.SQLType()):
		matched = l.matchWildcard(left.ToRawBytes(), right.(*evalBytes).bytes, col)
	case typeIsTextual(left.SQLType()):
		matched = l.matchWildcard(left.(*evalBytes).bytes, right.ToRawBytes(), col)
	default:
		matched = l.matchWildcard(left.ToRawBytes(), right.ToRawBytes(), collations.CollationBinaryID)
	}
	return newEvalBool(matched == !l.Negate), nil
}

// typeof implements the ComparisonOp interface
func (l *LikeExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := l.Left.typeof(env, fields)
	_, f2 := l.Right.typeof(env, fields)
	return sqltypes.Int64, f1 | f2
}
