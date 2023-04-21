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
		return compareDates(left.(*evalTemporal), right.(*evalTemporal)), nil
	case compareAsStrings(lt, rt):
		return compareStrings(left, right)
	case compareAsSameNumericType(lt, rt) || compareAsDecimal(lt, rt):
		return compareNumeric(left, right)
	case compareAsDateAndString(lt, rt):
		return compareDateAndString(left, right), nil
	case compareAsDateAndNumeric(lt, rt):
		if sqltypes.IsDateOrTime(lt) {
			left = evalToNumeric(left, false)
		}
		if sqltypes.IsDateOrTime(rt) {
			right = evalToNumeric(right, false)
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
		lf, _ := evalToFloat(left)
		rf, _ := evalToFloat(right)
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

func (expr *ComparisonExpr) compileAsTuple(c *compiler) (ctype, error) {
	switch expr.Op.(type) {
	case compareNullSafeEQ:
		c.asm.CmpTupleNullsafe()
		return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}, nil
	case compareEQ:
		c.asm.CmpTuple(true)
		c.asm.Cmp_eq_n()
	case compareNE:
		c.asm.CmpTuple(true)
		c.asm.Cmp_ne_n()
	case compareLT:
		c.asm.CmpTuple(false)
		c.asm.Cmp_lt_n()
	case compareLE:
		c.asm.CmpTuple(false)
		c.asm.Cmp_le_n()
	case compareGT:
		c.asm.CmpTuple(false)
		c.asm.Cmp_gt_n()
	case compareGE:
		c.asm.CmpTuple(false)
		c.asm.Cmp_ge_n()
	default:
		panic("invalid comparison operator")
	}
	return ctype{Type: sqltypes.Int64, Flag: flagNullable | flagIsBoolean, Col: collationNumeric}, nil
}

func (expr *ComparisonExpr) compile(c *compiler) (ctype, error) {
	lt, err := expr.Left.compile(c)
	if err != nil {
		return ctype{}, err
	}

	var skip1 *jump
	switch expr.Op.(type) {
	case compareNullSafeEQ:
	default:
		skip1 = c.compileNullCheck1(lt)
	}

	rt, err := expr.Right.compile(c)
	if err != nil {
		return ctype{}, err
	}

	if lt.Type == sqltypes.Tuple || rt.Type == sqltypes.Tuple {
		if lt.Type != rt.Type {
			return ctype{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "did not typecheck tuples during comparison")
		}
		return expr.compileAsTuple(c)
	}

	swapped := false
	var skip2 *jump

	switch expr.Op.(type) {
	case compareNullSafeEQ:
		skip2 = c.asm.jumpFrom()
		c.asm.Cmp_nullsafe(skip2)
	default:
		skip2 = c.compileNullCheck1r(rt)
	}

	switch {
	case compareAsDates(lt.Type, rt.Type):
		c.asm.CmpDates()
	case compareAsStrings(lt.Type, rt.Type):
		if err := c.compareAsStrings(lt, rt); err != nil {
			return ctype{}, err
		}
	case compareAsSameNumericType(lt.Type, rt.Type) || compareAsDecimal(lt.Type, rt.Type):
		swapped = c.compareNumericTypes(lt, rt)
	case compareAsDateAndString(lt.Type, rt.Type):
		c.asm.CmpDateString()
	case compareAsDateAndNumeric(lt.Type, rt.Type):
		if sqltypes.IsDateOrTime(lt.Type) {
			c.asm.Convert_Ti(2)
			lt.Type = sqltypes.Int64
		}
		if sqltypes.IsDateOrTime(rt.Type) {
			c.asm.Convert_Ti(1)
			rt.Type = sqltypes.Int64
		}
		swapped = c.compareNumericTypes(lt, rt)
	case compareAsJSON(lt.Type, rt.Type):
		if err := c.compareAsJSON(lt, rt); err != nil {
			return ctype{}, err
		}

	default:
		lt = c.compileToFloat(lt, 2)
		rt = c.compileToFloat(rt, 1)
		c.asm.CmpNum_ff()
	}

	cmptype := ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}

	switch expr.Op.(type) {
	case compareEQ:
		c.asm.Cmp_eq()
	case compareNE:
		c.asm.Cmp_ne()
	case compareLT:
		if swapped {
			c.asm.Cmp_gt()
		} else {
			c.asm.Cmp_lt()
		}
	case compareLE:
		if swapped {
			c.asm.Cmp_ge()
		} else {
			c.asm.Cmp_le()
		}
	case compareGT:
		if swapped {
			c.asm.Cmp_lt()
		} else {
			c.asm.Cmp_gt()
		}
	case compareGE:
		if swapped {
			c.asm.Cmp_le()
		} else {
			c.asm.Cmp_ge()
		}
	case compareNullSafeEQ:
		c.asm.jumpDestination(skip2)
		c.asm.Cmp_eq()
		return cmptype, nil

	default:
		panic("unexpected comparison operator")
	}

	c.asm.jumpDestination(skip1, skip2)
	return cmptype, nil
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

func (i *InExpr) compileTable(lhs ctype, rhs TupleExpr) map[vthash.Hash]struct{} {
	var (
		table  = make(map[vthash.Hash]struct{})
		hasher = vthash.New()
	)

	for _, expr := range rhs {
		lit, ok := expr.(*Literal)
		if !ok {
			return nil
		}
		inner, ok := lit.inner.(hashable)
		if !ok {
			return nil
		}

		thisColl := evalCollation(lit.inner).Collation
		thisTyp := lit.inner.SQLType()

		if thisTyp != lhs.Type || thisColl != lhs.Col.Collation {
			return nil
		}

		inner.Hash(&hasher)
		table[hasher.Sum128()] = struct{}{}
		hasher.Reset()
	}

	return table
}

func (expr *InExpr) compile(c *compiler) (ctype, error) {
	lhs, err := expr.Left.compile(c)
	if err != nil {
		return ctype{}, nil
	}

	rhs := expr.Right.(TupleExpr)

	if table := expr.compileTable(lhs, rhs); table != nil {
		c.asm.In_table(expr.Negate, table)
	} else {
		_, err := rhs.compile(c)
		if err != nil {
			return ctype{}, err
		}
		c.asm.In_slow(expr.Negate)
	}
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}, nil
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
	left, right, col, err = mergeAndCoerceCollations(left, right)
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

func (expr *LikeExpr) compile(c *compiler) (ctype, error) {
	lt, err := expr.Left.compile(c)
	if err != nil {
		return ctype{}, err
	}

	rt, err := expr.Right.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck2(lt, rt)

	if !lt.isTextual() {
		c.asm.Convert_xc(2, sqltypes.VarChar, c.cfg.Collation, 0, false)
		lt.Col = collations.TypedCollation{
			Collation:    c.cfg.Collation,
			Coercibility: collations.CoerceCoercible,
			Repertoire:   collations.RepertoireASCII,
		}
	}

	if !rt.isTextual() {
		c.asm.Convert_xc(1, sqltypes.VarChar, c.cfg.Collation, 0, false)
		rt.Col = collations.TypedCollation{
			Collation:    c.cfg.Collation,
			Coercibility: collations.CoerceCoercible,
			Repertoire:   collations.RepertoireASCII,
		}
	}

	var merged collations.TypedCollation
	var coerceLeft collations.Coercion
	var coerceRight collations.Coercion
	var env = collations.Local()

	if lt.Col.Collation != rt.Col.Collation {
		merged, coerceLeft, coerceRight, err = env.MergeCollations(lt.Col, rt.Col, collations.CoercionOptions{
			ConvertToSuperset:   true,
			ConvertWithCoercion: true,
		})
	} else {
		merged = lt.Col
	}
	if err != nil {
		return ctype{}, err
	}

	if coerceLeft == nil && coerceRight == nil {
		c.asm.Like_collate(expr, merged.Collation.Get())
	} else {
		if coerceLeft == nil {
			coerceLeft = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		if coerceRight == nil {
			coerceRight = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		c.asm.Like_coerce(expr, &compiledCoercion{
			col:   merged.Collation.Get(),
			left:  coerceLeft,
			right: coerceRight,
		})
	}

	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}, nil
}
