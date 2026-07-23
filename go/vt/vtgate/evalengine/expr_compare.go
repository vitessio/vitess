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
	"bytes"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/mysql/datetime"
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
		Match          colldata.WildcardPattern
		MatchCollation collations.ID
	}

	InExpr struct {
		BinaryExpr
		Negate bool
	}

	BetweenExpr struct {
		Left   IR
		From   IR
		To     IR
		Negate bool
		// TemporalDomain is the temporal comparison domain MySQL selects for
		// the operator statically: sqltypes.Datetime, sqltypes.Time, or
		// sqltypes.Unknown for none. It only takes effect when a JSON operand
		// participates at runtime; see betweenTemporalDomain.
		TemporalDomain sqltypes.Type
	}

	ComparisonOp interface {
		String() string
		compare(collationEnv *collations.Environment, left, right eval) (boolean, error)
	}

	compareEQ         struct{}
	compareNE         struct{}
	compareLT         struct{}
	compareLE         struct{}
	compareGT         struct{}
	compareGE         struct{}
	compareNullSafeEQ struct{}

	// compareCaseEQ is the equality between the base operand of a simple
	// CASE and its WHEN operands: like MySQL, it never uses the JSON
	// comparator.
	compareCaseEQ struct{}
)

var (
	_ IR = (*ComparisonExpr)(nil)
	_ IR = (*InExpr)(nil)
	_ IR = (*BetweenExpr)(nil)
	_ IR = (*LikeExpr)(nil)
)

func (*ComparisonExpr) filterExpr() {}
func (*InExpr) filterExpr()         {}

func (compareEQ) String() string { return "=" }
func (compareEQ) compare(collationEnv *collations.Environment, left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, true, collationEnv)
	return makeboolean2(cmp == 0, isNull), err
}

func (compareNE) String() string { return "!=" }
func (compareNE) compare(collationEnv *collations.Environment, left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, true, collationEnv)
	return makeboolean2(cmp != 0, isNull), err
}

func (compareLT) String() string { return "<" }
func (compareLT) compare(collationEnv *collations.Environment, left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false, collationEnv)
	return makeboolean2(cmp < 0, isNull), err
}

func (compareLE) String() string { return "<=" }
func (compareLE) compare(collationEnv *collations.Environment, left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false, collationEnv)
	return makeboolean2(cmp <= 0, isNull), err
}

func (compareGT) String() string { return ">" }
func (compareGT) compare(collationEnv *collations.Environment, left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false, collationEnv)
	return makeboolean2(cmp > 0, isNull), err
}

func (compareGE) String() string { return ">=" }
func (compareGE) compare(collationEnv *collations.Environment, left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareAll(left, right, false, collationEnv)
	return makeboolean2(cmp >= 0, isNull), err
}

func (compareNullSafeEQ) String() string { return "<=>" }
func (compareNullSafeEQ) compare(collationEnv *collations.Environment, left, right eval) (boolean, error) {
	cmp, err := evalCompareNullSafe(left, right, collationEnv)
	return makeboolean(cmp == 0), err
}

func (compareCaseEQ) String() string { return "=" }
func (compareCaseEQ) compare(collationEnv *collations.Environment, left, right eval) (boolean, error) {
	cmp, isNull, err := evalCompareCase(left, right, collationEnv)
	return makeboolean2(cmp == 0, isNull), err
}

func typeIsTextual(tt sqltypes.Type) bool {
	return sqltypes.IsTextOrBinary(tt) || tt == sqltypes.Time || tt == sqltypes.Enum || tt == sqltypes.Set
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

func compareAsEnums(l, r sqltypes.Type) bool {
	return sqltypes.IsEnum(l) && sqltypes.IsEnum(r)
}

func compareAsSets(l, r sqltypes.Type) bool {
	return sqltypes.IsSet(l) && sqltypes.IsSet(r)
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

func evalCompareNullSafe(lVal, rVal eval, collationEnv *collations.Environment) (int, error) {
	if lVal == nil {
		if rVal == nil {
			return 0, nil
		}
		return -1, nil
	}
	if rVal == nil {
		return 1, nil
	}
	if left, right, ok := compareAsTuples(lVal, rVal); ok {
		return evalCompareTuplesNullSafe(left.t, right.t, collationEnv)
	}
	n, err := evalCompare(lVal, rVal, collationEnv)
	return n, err
}

func evalCompareMany(left, right []eval, fulleq bool, collationEnv *collations.Environment) (int, bool, error) {
	// For row comparisons, (a, b) = (x, y) is equivalent to: (a = x) AND (b = y)
	var seenNull bool
	for idx, lResult := range left {
		rResult := right[idx]
		n, isNull, err := evalCompareAll(lResult, rResult, fulleq, collationEnv)
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

func evalCompareAll(lVal, rVal eval, fulleq bool, collationEnv *collations.Environment) (int, bool, error) {
	if lVal == nil || rVal == nil {
		return 0, true, nil
	}
	if left, right, ok := compareAsTuples(lVal, rVal); ok {
		return evalCompareMany(left.t, right.t, fulleq, collationEnv)
	}
	n, err := evalCompare(lVal, rVal, collationEnv)
	return n, false, err
}

// evalCompareCase compares the base operand of a simple CASE with a WHEN
// operand: it matches evalCompareAll except that JSON pairs go through
// evalCompareCaseJSON, as MySQL never uses the JSON comparator here.
func evalCompareCase(lVal, rVal eval, collationEnv *collations.Environment) (int, bool, error) {
	if lVal == nil || rVal == nil {
		return 0, true, nil
	}
	if left, right, ok := compareAsTuples(lVal, rVal); ok {
		return evalCompareMany(left.t, right.t, true, collationEnv)
	}
	if compareAsJSON(lVal.SQLType(), rVal.SQLType()) {
		n, err := evalCompareCaseJSON(lVal, rVal, collationEnv)
		return n, false, err
	}
	n, err := evalCompare(lVal, rVal, collationEnv)
	return n, false, err
}

// For more details on comparison expression evaluation and type conversion:
//   - https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
func evalCompare(left, right eval, collationEnv *collations.Environment) (comp int, err error) {
	lt := left.SQLType()
	rt := right.SQLType()

	switch {
	case compareAsDates(lt, rt):
		return compareDates(left.(*evalTemporal), right.(*evalTemporal)), nil
	case compareAsEnums(lt, rt):
		return compareEnums(left.(*evalEnum), right.(*evalEnum)), nil
	case compareAsSets(lt, rt):
		return compareSets(left.(*evalSet), right.(*evalSet)), nil
	case compareAsStrings(lt, rt):
		return compareStrings(left, right, collationEnv)
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
	case lt == rt && fallbackBinary(lt):
		return bytes.Compare(left.ToRawBytes(), right.ToRawBytes()), nil
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

// fallbackBinary compares two values of the same type using the fallback binary comparison.
// This is for types we don't yet properly support otherwise but do end up being used
// for comparisons, for example when using vdiff.
// TODO: Clean this up as we add more properly supported types and comparisons.
func fallbackBinary(t sqltypes.Type) bool {
	switch t {
	case sqltypes.Bit, sqltypes.Enum, sqltypes.Set, sqltypes.Geometry, sqltypes.Vector:
		return true
	}
	return false
}

func evalCompareTuplesNullSafe(left, right []eval, collationEnv *collations.Environment) (int, error) {
	if len(left) != len(right) {
		panic("did not typecheck cardinality")
	}
	for idx, lResult := range left {
		res, err := evalCompareNullSafe(lResult, right[idx], collationEnv)
		if err != nil {
			return 0, err
		}
		if res != 0 {
			return res, nil
		}
	}
	return 0, nil
}

// eval implements the expression interface
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
	cmp, err := c.Op.compare(env.collationEnv, left, right)
	if err != nil {
		return nil, err
	}
	return cmp.eval(), nil
}

func (expr *ComparisonExpr) compileAsTuple(c *compiler) (ctype, error) {
	switch expr.Op.(type) {
	case compareNullSafeEQ:
		c.asm.CmpTupleNullsafe(c.env.CollationEnv())
		return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}, nil
	case compareEQ, compareCaseEQ:
		c.asm.CmpTuple(c.env.CollationEnv(), true)
		c.asm.Cmp_eq_n()
	case compareNE:
		c.asm.CmpTuple(c.env.CollationEnv(), true)
		c.asm.Cmp_ne_n()
	case compareLT:
		c.asm.CmpTuple(c.env.CollationEnv(), false)
		c.asm.Cmp_lt_n()
	case compareLE:
		c.asm.CmpTuple(c.env.CollationEnv(), false)
		c.asm.Cmp_le_n()
	case compareGT:
		c.asm.CmpTuple(c.env.CollationEnv(), false)
		c.asm.Cmp_gt_n()
	case compareGE:
		c.asm.CmpTuple(c.env.CollationEnv(), false)
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
	nullable := true

	switch expr.Op.(type) {
	case compareNullSafeEQ:
		skip2 = c.asm.jumpFrom()
		c.asm.Cmp_nullsafe(skip2)
		nullable = false
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
			if lt.Size == 0 {
				c.asm.Convert_Ti(2)
				lt.Type = sqltypes.Int64
			} else {
				c.asm.Convert_Tf(2)
				lt.Type = sqltypes.Float64
			}
		}
		if sqltypes.IsDateOrTime(rt.Type) {
			if rt.Size == 0 {
				c.asm.Convert_Ti(1)
				rt.Type = sqltypes.Int64
			} else {
				c.asm.Convert_Tf(1)
				rt.Type = sqltypes.Float64
			}
		}
		swapped = c.compareNumericTypes(lt, rt)
	case compareAsJSON(lt.Type, rt.Type):
		if _, ok := expr.Op.(compareCaseEQ); ok {
			c.asm.CmpCaseJSON(c.env.CollationEnv())
		} else if err := c.compareAsJSON(lt, rt); err != nil {
			return ctype{}, err
		}

	default:
		lt = c.compileToFloat(lt, 2)
		rt = c.compileToFloat(rt, 1)
		c.asm.CmpNum_ff()
	}

	cmptype := ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}
	if nullable {
		cmptype.Flag |= nullableFlags(lt.Flag | rt.Flag)
	}

	switch expr.Op.(type) {
	case compareEQ, compareCaseEQ:
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

func evalInExpr(collationEnv *collations.Environment, lhs eval, rhs *evalTuple) (boolean, error) {
	if lhs == nil {
		return boolNULL, nil
	}

	// When any operand of IN is a JSON value, MySQL compares every pair as
	// JSON, converting the non-JSON operands to JSON scalars (strings become
	// string scalars, not parsed documents). Row (tuple) operands keep their
	// per-column comparisons.
	asJSON := lhs.SQLType() == sqltypes.TypeJSON
	if !asJSON {
		for _, rtuple := range rhs.t {
			if rtuple != nil && rtuple.SQLType() == sqltypes.TypeJSON {
				asJSON = true
				break
			}
		}
	}

	var foundNull, found bool
	for _, rtuple := range rhs.t {
		var numeric int
		var isNull bool
		var err error
		switch {
		case asJSON && rtuple == nil:
			isNull = true
		case asJSON:
			numeric, err = compareJSON(lhs, rtuple)
		default:
			numeric, isNull, err = evalCompareAll(lhs, rtuple, true, collationEnv)
		}
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
	in, err := evalInExpr(env.collationEnv, left, rtuple)
	if err != nil {
		return nil, err
	}
	if i.Negate {
		in = in.not()
	}
	return in.eval(), nil
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

	switch rhs := expr.Right.(type) {
	case TupleExpr:
		var rt ctype
		if table := expr.compileTable(lhs, rhs); table != nil {
			c.asm.In_table(expr.Negate, table)
		} else {
			rt, err = rhs.compile(c)
			if err != nil {
				return ctype{}, err
			}
			c.asm.In_slow(c.env.CollationEnv(), expr.Negate)
		}

		return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean | (nullableFlags(lhs.Flag) | (rt.Flag & flagNullable))}, nil
	case *BindVariable:

		if rhs.Type != sqltypes.Tuple {
			return ctype{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple")
		}

		rt, err := rhs.compile(c)
		if err != nil {
			return ctype{}, err
		}

		c.asm.In_slow(c.env.CollationEnv(), expr.Negate)
		return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean | (nullableFlags(lhs.Flag) | (rt.Flag & flagNullable))}, nil
	default:
		panic("unreachable")
	}
}

// betweenToDateTime coerces a BETWEEN operand in the DATETIME domain:
// unconvertible values truncate to the zero DATETIME rather than NULL or an
// error, matching MySQL's non-strict implicit conversion.
func betweenToDateTime(e eval, now time.Time) datetime.DateTime {
	if t := evalToDateTime(e, -1, now, true); t != nil {
		return t.dt
	}
	return datetime.DateTime{}
}

// betweenToTime is betweenToDateTime's TIME-domain counterpart, truncating
// unconvertible values to the zero TIME.
func betweenToTime(e eval) datetime.Time {
	if t := evalToTime(e, -1); t != nil {
		return t.dt.Time
	}
	return datetime.Time{}
}

// evalBetweenExpr evaluates `left [NOT] BETWEEN from AND to`. MySQL never
// compares BETWEEN operands as JSON: when a JSON operand participates, both
// comparisons happen in the statically selected temporal domain if there is
// one, as strings if every non-NULL operand is a JSON or textual value, and
// as DOUBLE otherwise. Without a JSON operand, the operands compare pairwise
// exactly like the equivalent `left >= from AND left <= to` conjunction.
func evalBetweenExpr(collationEnv *collations.Environment, left, from, to eval, negate bool, temporalDomain sqltypes.Type, now time.Time) (boolean, error) {
	if left == nil {
		return boolNULL, nil
	}

	var hasJSON bool
	textualOnly := true
	for _, e := range [3]eval{left, from, to} {
		if e == nil {
			continue
		}
		switch tt := e.SQLType(); {
		case tt == sqltypes.TypeJSON:
			hasJSON = true
		case !typeIsTextual(tt):
			textualOnly = false
		}
	}

	var cmp func(r eval) (int, bool, error)
	switch {
	case !hasJSON:
		cmp = func(r eval) (int, bool, error) {
			return evalCompareAll(left, r, false, collationEnv)
		}
	case temporalDomain == sqltypes.Datetime:
		leftDT := betweenToDateTime(left, now)
		cmp = func(r eval) (int, bool, error) {
			if r == nil {
				return 0, true, nil
			}
			return leftDT.Compare(betweenToDateTime(r, now)), false, nil
		}
	case temporalDomain == sqltypes.Time:
		leftTime := betweenToTime(left)
		cmp = func(r eval) (int, bool, error) {
			if r == nil {
				return 0, true, nil
			}
			return leftTime.Compare(betweenToTime(r)), false, nil
		}
	case textualOnly:
		// MySQL aggregates a single collation across the whole operand set:
		// the JSON operands make utf8mb4_bin win for every comparison.
		var ca collationAggregation
		for _, e := range [3]eval{left, from, to} {
			if e == nil {
				continue
			}
			if err := ca.add(evalCollation(e), collationEnv); err != nil {
				return boolNULL, err
			}
		}
		col := colldata.Lookup(ca.result().Collation)
		toMerged := func(e eval) ([]byte, error) {
			return charset.Convert(nil, col.Charset(), e.ToRawBytes(), colldata.Lookup(evalCollation(e).Collation).Charset())
		}
		leftBytes, err := toMerged(left)
		if err != nil {
			return boolNULL, err
		}
		cmp = func(r eval) (int, bool, error) {
			if r == nil {
				return 0, true, nil
			}
			rightBytes, err := toMerged(r)
			if err != nil {
				return 0, false, err
			}
			return col.Collate(leftBytes, rightBytes, false), false, nil
		}
	default:
		leftFloat, _ := evalToFloat(left)
		cmp = func(r eval) (int, bool, error) {
			if r == nil {
				return 0, true, nil
			}
			rightFloat, _ := evalToFloat(r)
			n, err := compareNumeric(leftFloat, rightFloat)
			return n, false, err
		}
	}

	n, isNull, err := cmp(from)
	if err != nil {
		return boolNULL, err
	}
	cmpFrom := makeboolean2(n >= 0, isNull)
	if cmpFrom == boolFalse {
		// The result is decided: skip the upper-bound comparison, like the
		// equivalent `left >= from AND left <= to` conjunction would.
		return makeboolean(negate), nil
	}
	n, isNull, err = cmp(to)
	if err != nil {
		return boolNULL, err
	}
	cmpTo := makeboolean2(n <= 0, isNull)

	// Combine both comparisons like the AND operator would; cmpFrom is either
	// true or NULL here.
	var result boolean
	switch {
	case cmpTo == boolFalse:
		result = boolFalse
	case cmpFrom == boolTrue && cmpTo == boolTrue:
		result = boolTrue
	default:
		result = boolNULL
	}
	if negate {
		result = result.not()
	}
	return result, nil
}

// eval implements the expression interface
func (b *BetweenExpr) eval(env *ExpressionEnv) (eval, error) {
	left, err := b.Left.eval(env)
	if err != nil || left == nil {
		return nil, err
	}
	from, err := b.From.eval(env)
	if err != nil {
		return nil, err
	}
	to, err := b.To.eval(env)
	if err != nil {
		return nil, err
	}
	in, err := evalBetweenExpr(env.collationEnv, left, from, to, b.Negate, b.TemporalDomain, env.now)
	if err != nil {
		return nil, err
	}
	return in.eval(), nil
}

func (expr *BetweenExpr) compile(c *compiler) (ctype, error) {
	lt, err := expr.Left.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(lt)

	ft, err := expr.From.compile(c)
	if err != nil {
		return ctype{}, err
	}
	tt, err := expr.To.compile(c)
	if err != nil {
		return ctype{}, err
	}

	c.asm.Between(c.env.CollationEnv(), expr.Negate, expr.TemporalDomain)
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean | nullableFlags(lt.Flag|ft.Flag|tt.Flag)}, nil
}

func (l *LikeExpr) matchWildcard(left, right []byte, coll collations.ID) bool {
	if l.Match != nil && l.MatchCollation == coll {
		return l.Match.Match(left)
	}
	fullColl := colldata.Lookup(coll)
	wc := fullColl.Wildcard(right, 0, 0, 0)
	return wc.Match(left) == !l.Negate
}

func (l *LikeExpr) eval(env *ExpressionEnv) (eval, error) {
	left, err := l.Left.eval(env)
	if err != nil || left == nil {
		return left, err
	}

	right, err := l.Right.eval(env)
	if err != nil || right == nil {
		return right, err
	}

	var col collations.TypedCollation
	left, right, col, err = mergeAndCoerceCollations(left, right, env.collationEnv)
	if err != nil {
		return nil, err
	}

	matched := l.matchWildcard(left.ToRawBytes(), right.ToRawBytes(), col.Collation)

	return newEvalBool(matched), nil
}

func (expr *LikeExpr) compile(c *compiler) (ctype, error) {
	lt, err := expr.Left.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(lt)

	rt, err := expr.Right.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1(rt)

	if !lt.isTextual() {
		c.asm.Convert_xc(2, sqltypes.VarChar, c.collation, nil)
		lt.Col = collations.TypedCollation{
			Collation:    c.collation,
			Coercibility: collations.CoerceCoercible,
			Repertoire:   collations.RepertoireASCII,
		}
	}

	if !rt.isTextual() {
		c.asm.Convert_xc(1, sqltypes.VarChar, c.collation, nil)
		rt.Col = collations.TypedCollation{
			Collation:    c.collation,
			Coercibility: collations.CoerceCoercible,
			Repertoire:   collations.RepertoireASCII,
		}
	}

	var merged collations.TypedCollation
	var coerceLeft colldata.Coercion
	var coerceRight colldata.Coercion

	if lt.Col.Collation != rt.Col.Collation {
		merged, coerceLeft, coerceRight, err = colldata.Merge(c.env.CollationEnv(), lt.Col, rt.Col, colldata.CoercionOptions{
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
		c.asm.Like_collate(expr, colldata.Lookup(merged.Collation))
	} else {
		if coerceLeft == nil {
			coerceLeft = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		if coerceRight == nil {
			coerceRight = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		c.asm.Like_coerce(expr, &compiledCoercion{
			col:   colldata.Lookup(merged.Collation),
			left:  coerceLeft,
			right: coerceRight,
		})
	}

	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean | flagNullable}, nil
}
