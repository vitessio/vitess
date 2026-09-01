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
	"math"
	"slices"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/mysql/json"
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
		// PrefixJSON aggregates the declared-JSON tri-state over the left
		// operand and the RHS prefix MySQL's type scan inspects, proved at
		// translation time before constant folding can erase a typed SQL
		// NULL. Row elements never contribute.
		PrefixJSON inJSONDomain
		// JSONScanRHS counts the leading RHS elements of that scan:
		// everything up to and including the first element that is not
		// constant for one execution.
		JSONScanRHS int
	}

	BetweenExpr struct {
		Left   IR
		From   IR
		To     IR
		Negate bool
		// StaticClasses are the comparison classes of (Left, From, To) proved
		// at translation time, before constant folding can erase a typed SQL
		// NULL; classUnknown when the type resolves only at evaluation time.
		StaticClasses [3]cmpClass
	}

	// cmpClass is the comparison result class of an operand's declared type,
	// deciding the comparison domain of IN and BETWEEN when a JSON operand
	// participates.
	cmpClass uint8

	// inJSONDomain is the declared-JSON comparison plan of a scalar IN:
	// definitely JSON, definitely not, or resolvable only from the element
	// types of an execution-expanded tuple bind.
	inJSONDomain uint8

	// betweenPair is the comparator selected for one bound pair of a
	// JSON-participating BETWEEN.
	betweenPair uint8

	// betweenPlan is the comparison plan of a BETWEEN: the legacy pairwise
	// comparison without a JSON operand, or one comparator per bound pair.
	// Plans are captured by value, never stored on shared IR.
	betweenPlan struct {
		legacy bool
		pairs  [2]betweenPair
	}
)

const (
	inJSONUnknown inJSONDomain = iota
	inJSONNo
	inJSONYes
)

const (
	classUnknown cmpClass = iota
	classNull             // untyped SQL NULL: neutral for domain selection
	classJSON
	classDateLike // DATE, DATETIME, TIMESTAMP
	classTime
	classString  // textual, binary, ENUM, SET
	classNumeric // integral, decimal, float, YEAR, BIT
)

const (
	// pairString compares the serialized text forms under the collation
	// aggregated across all three operands; only ever set for both pairs.
	pairString betweenPair = iota
	// pairNumeric compares as DOUBLE; it is also the integer fallback of the
	// TIME column branch.
	pairNumeric
	// pairDatetimeText compares as DATETIME, converting operands from their
	// text forms: a JSON operand converts from its serialized text, quotes
	// included, so JSON strings and temporal scalars fail to the zero date.
	pairDatetimeText
	// pairDatetimeField compares as DATETIME through the field-store
	// conversion MySQL applies to a constant bound of a DATE-family column:
	// JSON strings unquote and JSON date scalars convert natively, while
	// other JSON values fail to the zero date.
	pairDatetimeField
	// pairTime compares as packed TIME against a bare TIME column: a JSON
	// string unquotes and a JSON TIME scalar converts natively, while other
	// JSON values fail to the zero time.
	pairTime
)

type (
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

func evalInExpr(collationEnv *collations.Environment, lhs eval, rhs *evalTuple, plan inJSONDomain) (boolean, error) {
	if lhs == nil {
		return boolNULL, nil
	}

	// When a JSON operand participates in IN — by declared type, even when
	// the value is SQL NULL — MySQL compares every pair as JSON, converting
	// the non-JSON operands to JSON scalars (strings become string scalars,
	// not parsed documents). Row (tuple) operands keep their per-column
	// comparisons. An unresolved plan is the execution-expanded tuple bind,
	// whose element values carry the declared types of the expanded list.
	asJSON := plan == inJSONYes
	if plan == inJSONUnknown {
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

// resolvePrefixJSON resolves an unknown declared-JSON plan during
// evaluation: the classes of the left operand and the scanned RHS prefix,
// late-resolved from their declared types. An execution-expanded tuple bind
// stays unresolved unless the left operand declares JSON: its element types
// are inspected on the expanded values.
func (i *InExpr) resolvePrefixJSON(env *ExpressionEnv, lhs eval, rtuple *evalTuple) inJSONDomain {
	tuple, ok := i.Right.(TupleExpr)
	if !ok {
		class, provisional := evalCmpClass(env, classUnknown, i.Left, lhs)
		if class == classJSON && provisional {
			if declared, ok := declaredCmpClass(env, i.Left); ok {
				class = declared
			}
		}
		if class == classJSON {
			return inJSONYes
		}
		return inJSONUnknown
	}

	ops := make([]IR, 1, i.JSONScanRHS+1)
	vals := make([]eval, 1, i.JSONScanRHS+1)
	ops[0], vals[0] = i.Left, lhs
	for idx := 0; idx < i.JSONScanRHS && idx < len(tuple); idx++ {
		var val eval
		if idx < len(rtuple.t) {
			val = rtuple.t[idx]
		}
		ops = append(ops, tuple[idx])
		vals = append(vals, val)
	}

	classes := make([]cmpClass, len(ops))
	provisional := make([]bool, len(ops))
	for k := range ops {
		classes[k], provisional[k] = evalCmpClass(env, classUnknown, ops[k], vals[k])
	}
	resolveJSONCmpClasses(env, classes, provisional, ops)
	if slices.Contains(classes, classJSON) {
		return inJSONYes
	}
	return inJSONNo
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
	plan := i.PrefixJSON
	if plan == inJSONUnknown {
		plan = i.resolvePrefixJSON(env, left, rtuple)
	}
	in, err := evalInExpr(env.collationEnv, left, rtuple, plan)
	if err != nil {
		return nil, err
	}
	if i.Negate {
		in = in.not()
	}
	return in.eval(), nil
}

func (i *InExpr) compileTable(lhs ctype, rhs TupleExpr) map[vthash.Hash]struct{} {
	// JSON hashes fingerprint arrays and objects by kind and cardinality
	// only: a hash hit is not equality, so JSON stays on the comparing
	// slow path.
	if lhs.Type == sqltypes.TypeJSON {
		return nil
	}

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
		return ctype{}, err
	}

	plan := expr.PrefixJSON
	if plan == inJSONUnknown && lhs.Type == sqltypes.TypeJSON {
		plan = inJSONYes
	}

	switch rhs := expr.Right.(type) {
	case TupleExpr:
		var rt ctype
		if table := expr.compileTable(lhs, rhs); table != nil {
			c.asm.In_table(expr.Negate, table)
		} else {
			// Compile the elements individually instead of through
			// TupleExpr.compile, which discards the element types.
			for idx, el := range rhs {
				et, err := el.compile(c)
				if err != nil {
					return ctype{}, err
				}
				if plan == inJSONUnknown && idx < expr.JSONScanRHS && et.Type == sqltypes.TypeJSON {
					plan = inJSONYes
				}
			}
			if plan == inJSONUnknown {
				plan = inJSONNo
			}
			c.asm.PackTuple(len(rhs))
			c.asm.In_slow(c.env.CollationEnv(), expr.Negate, plan)
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

		// The expanded list's element declarations resolve at execution.
		if plan == inJSONNo {
			plan = inJSONUnknown
		}
		c.asm.In_slow(c.env.CollationEnv(), expr.Negate, plan)
		return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean | (nullableFlags(lhs.Flag) | (rt.Flag & flagNullable))}, nil
	default:
		panic("unreachable")
	}
}

// classFromType maps a declared SQL type onto its comparison class.
func classFromType(tt sqltypes.Type) cmpClass {
	switch {
	case tt == sqltypes.Null:
		return classNull
	case tt == sqltypes.TypeJSON:
		return classJSON
	case tt == sqltypes.Date, tt == sqltypes.Datetime, tt == sqltypes.Timestamp:
		return classDateLike
	case tt == sqltypes.Time:
		return classTime
	case sqltypes.IsNumber(tt), tt == sqltypes.Year, tt == sqltypes.Bit:
		return classNumeric
	case sqltypes.IsTextOrBinary(tt), tt == sqltypes.Enum, tt == sqltypes.Set:
		return classString
	default:
		return classUnknown
	}
}

// mergeCmpClass merges the translation-time class with the operand's
// late-resolved type. The static class wins: constant folding erases typed
// SQL NULLs.
func mergeCmpClass(static cmpClass, tt sqltypes.Type) cmpClass {
	if static != classUnknown && static != classNull {
		return static
	}
	if c := classFromType(tt); c != classUnknown {
		return c
	}
	return static
}

// evalCmpClass resolves an operand's comparison class during evaluation:
// the translation-time class, the type of the evaluated value, or the type
// the operand declares in the evaluation environment when the value is SQL
// NULL. provisional marks a composite classified from its runtime value,
// whose declared type may still disagree with the selected child's
// representation.
func evalCmpClass(env *ExpressionEnv, static cmpClass, op IR, val eval) (cmpClass, bool) {
	if static != classUnknown && static != classNull {
		return static, false
	}
	if val != nil {
		return mergeCmpClass(static, val.SQLType()), compositeIR(op)
	}
	if typed, ok := op.(typedIR); ok {
		if ct, err := typed.typeof(env); err == nil {
			return mergeCmpClass(static, ct.Type), false
		}
		return static, false
	}
	if compositeIR(op) {
		if class, ok := declaredCmpClass(env, op); ok {
			return class, false
		}
	}
	return static, false
}

// compositeIR reports whether op is a composite whose runtime value may not
// carry its declared result type: SQL NULL erases it, and an evaluator may
// return a selected child's internal representation unchanged.
func compositeIR(op IR) bool {
	switch op.(type) {
	case *Literal, *Column, *BindVariable, *TupleBindVariable, TupleExpr:
		return false
	}
	return true
}

// declaredCmpClass derives a composite operand's declared comparison class
// through the compiler's type aggregation, resolving untyped leaves from the
// evaluation environment; the type-only program is discarded.
func declaredCmpClass(env *ExpressionEnv, op IR) (cmpClass, bool) {
	venv := env.VCursor().Environment()
	comp := compiler{
		env:       venv,
		collation: venv.CollationEnv().DefaultConnectionCharset(),
		sqlmode:   env.sqlmode,
		typeEnv:   env,
	}
	typ, err := op.compile(&comp)
	if err != nil {
		return classUnknown, false
	}
	if class := classFromType(typ.Type); class != classUnknown {
		return class, true
	}
	return classUnknown, false
}

// resolveJSONCmpClasses re-resolves provisional composite classes from their
// declared types once a JSON operand participates: the declaration wins over
// the selected child's runtime representation.
func resolveJSONCmpClasses(env *ExpressionEnv, classes []cmpClass, provisional []bool, ops []IR) {
	if !slices.Contains(classes, classJSON) {
		return
	}
	for k, prov := range provisional {
		if !prov {
			continue
		}
		if class, ok := declaredCmpClass(env, ops[k]); ok {
			classes[k] = class
		}
	}
}

// betweenToFloat coerces a numeric-domain BETWEEN operand: a JSON temporal
// scalar converts from the numeric prefix of its unquoted text, like a JSON
// string; everything else uses the standard DOUBLE coercion.
func betweenToFloat(e eval) *evalFloat {
	if j, ok := e.(*evalJSON); ok {
		switch j.Type() {
		case json.TypeDate, json.TypeDateTime, json.TypeTime:
			f, _ := fastparse.ParseFloat64(j.Raw())
			return &evalFloat{f: f}
		}
	}
	f, _ := evalToFloat(e)
	return f
}

// betweenToDateTime coerces a datetime-domain BETWEEN operand from its text
// form, quotes included for JSON, so JSON strings and temporal scalars fail
// while JSON numbers parse; failures truncate to the zero DATETIME, never
// NULL or an error.
func betweenToDateTime(e eval, now time.Time) datetime.DateTime {
	if t := evalToDateTime(evalJSONToText(e), -1, now, true); t != nil {
		return t.dt
	}
	return datetime.DateTime{}
}

// betweenFieldToDateTime coerces a constant bound of a DATE-family column
// pair through MySQL's field-store conversion: a JSON date-carrying scalar
// converts natively and a JSON string unquotes, while other JSON values
// fail; failures truncate to the zero DATETIME.
func betweenFieldToDateTime(e eval, now time.Time) datetime.DateTime {
	j, ok := e.(*evalJSON)
	if !ok {
		return betweenToDateTime(e, now)
	}
	switch j.Type() {
	case json.TypeDate, json.TypeDateTime:
		if dt, ok := j.DateTime(); ok {
			return dt
		}
	case json.TypeString:
		if t := evalToDateTime(newEvalText([]byte(j.Raw()), collationJSON), -1, now, true); t != nil {
			return t.dt
		}
	}
	return datetime.DateTime{}
}

// betweenToTime coerces a bound of a TIME-column pair: a JSON TIME scalar
// converts natively and a JSON string unquotes, while other JSON values
// fail; failures truncate to the zero TIME.
func betweenToTime(e eval) datetime.Time {
	if j, ok := e.(*evalJSON); ok {
		switch j.Type() {
		case json.TypeTime:
			if t, ok := j.Time(); ok {
				return t
			}
		case json.TypeString:
			if t, ok := parseTimeConstant(j.Raw()); ok {
				return t
			}
		}
		return datetime.Time{}
	}
	if t := evalToTime(e, -1); t != nil {
		return t.dt.Time
	}
	return datetime.Time{}
}

// parseTimeConstant parses a string as a TIME under MySQL's constant
// conversion: overflow does not clamp and a date-only string does not
// convert, but a full datetime literal contributes its time part.
func parseTimeConstant(s string) (datetime.Time, bool) {
	if t, _, state := datetime.ParseTime(s, -1); state == datetime.TimeOK {
		return t, true
	}
	if strings.ContainsAny(s, " T") {
		if dt, _, ok := datetime.ParseDateTime(s, -1); ok {
			return dt.Time, true
		}
	}
	return datetime.Time{}, false
}

// betweenTimeConvertible reports whether a constant bound converts to TIME
// under MySQL's strict constant conversion.
func betweenTimeConvertible(e eval) bool {
	switch e := e.(type) {
	case *evalTemporal:
		return true
	case *evalBytes:
		_, ok := parseTimeConstant(e.string())
		return ok
	case *evalInt64:
		return timeConvertibleInt64(e.i)
	case *evalUint64:
		return e.u <= math.MaxInt64 && timeConvertibleInt64(int64(e.u))
	case *evalFloat:
		_, _, ok := datetime.ParseTimeFloat(e.f, -1)
		return ok
	case *evalDecimal:
		_, _, ok := datetime.ParseTimeDecimal(e.dec, e.length, -1)
		return ok
	}
	return false
}

func timeConvertibleInt64(i int64) bool {
	if _, ok := datetime.ParseTimeInt64(i); ok {
		return true
	}
	_, ok := datetime.ParseDateTimeInt64(i)
	return ok
}

// resolveBetweenPlan selects the comparison plan of a BETWEEN. MySQL never
// compares BETWEEN operands as JSON: numerics win over the temporal domains,
// a DATE-family operand selects DATETIME in any position, only a bare TIME
// column on the left selects TIME, and strings otherwise. fromVal and toVal
// carry the bound values when known: always during evaluation and inside a
// per-execution compiled plan, constants during compilation.
func resolveBetweenPlan(classes [3]cmpClass, left, from, to IR, fromVal, toVal eval) betweenPlan {
	if classes[0] != classJSON && classes[1] != classJSON && classes[2] != classJSON {
		return betweenPlan{legacy: true}
	}

	hasNumeric := classes[0] == classNumeric || classes[1] == classNumeric || classes[2] == classNumeric
	hasDateLike := classes[0] == classDateLike || classes[1] == classDateLike || classes[2] == classDateLike
	_, leftIsColumn := left.(*Column)

	if hasNumeric && !(leftIsColumn && (classes[0] == classDateLike || classes[0] == classTime)) {
		return betweenPlan{pairs: [2]betweenPair{pairNumeric, pairNumeric}}
	}
	if leftIsColumn && classes[0] == classDateLike && hasNumeric {
		pair := func(class cmpClass, op IR) betweenPair {
			switch {
			case class == classNumeric:
				return pairNumeric
			case class == classJSON && op.constForExecution():
				return pairDatetimeField
			case class == classJSON:
				// The field-store conversion applies only to bounds that
				// are constant for one execution: a row-dependent JSON
				// bound stays numeric.
				return pairNumeric
			default:
				return pairDatetimeText
			}
		}
		return betweenPlan{pairs: [2]betweenPair{pair(classes[1], from), pair(classes[2], to)}}
	}
	if hasDateLike {
		return betweenPlan{pairs: [2]betweenPair{pairDatetimeText, pairDatetimeText}}
	}
	if leftIsColumn && classes[0] == classTime {
		// A bound that is constant for one execution replays the constant
		// conversion rule from its per-execution value: normalization
		// rewrites literal bounds into bind variables, so this keeps
		// normalized queries matching MySQL's literal behavior. For a bound
		// that does not convert, this deliberately diverges from MySQL's
		// prepared-parameter semantics (NULL through a failed field
		// conversion) and keeps the literal rule's numeric fallback.
		fromConv := from.constForExecution() && fromVal != nil && betweenTimeConvertible(fromVal)
		toConv := to.constForExecution() && toVal != nil && betweenTimeConvertible(toVal)
		// A typed-NULL JSON constant does not activate the per-pair paths:
		// the predicate stays on the whole-string fallback, unless a numeric
		// participant keeps the aggregate domain numeric.
		fromJSONConst := classes[1] == classJSON && from.constForExecution() && fromVal != nil
		toJSONConst := classes[2] == classJSON && to.constForExecution() && toVal != nil
		if !fromConv && !toConv && !fromJSONConst && !toJSONConst {
			if hasNumeric {
				return betweenPlan{pairs: [2]betweenPair{pairNumeric, pairNumeric}}
			}
			return betweenPlan{pairs: [2]betweenPair{pairString, pairString}}
		}
		pair := func(class cmpClass, jsonConst, conv bool) betweenPair {
			if jsonConst || conv || class == classTime {
				return pairTime
			}
			// Unconvertible constants and non-TIME columns compare on their
			// numeric forms.
			return pairNumeric
		}
		return betweenPlan{pairs: [2]betweenPair{
			pair(classes[1], fromJSONConst, fromConv),
			pair(classes[2], toJSONConst, toConv),
		}}
	}
	return betweenPlan{pairs: [2]betweenPair{pairString, pairString}}
}

// betweenPlanPerExecution reports whether the comparison plan depends on the
// per-execution value of a bound that is constant for one execution: only
// the TIME column branch of resolveBetweenPlan consults bound values, so a
// compiled BETWEEN with such a bound re-resolves its plan on every execution
// instead of baking one value into the type-keyed program.
func betweenPlanPerExecution(classes [3]cmpClass, left, from, to IR) bool {
	if classes[0] != classJSON && classes[1] != classJSON && classes[2] != classJSON {
		return false
	}
	if classes[0] == classDateLike || classes[1] == classDateLike || classes[2] == classDateLike {
		return false
	}
	if _, leftIsColumn := left.(*Column); !leftIsColumn || classes[0] != classTime {
		return false
	}
	perExecution := func(op IR) bool {
		return op.constForExecution() && !op.constant()
	}
	return perExecution(from) || perExecution(to)
}

// evalBetweenExpr evaluates `left [NOT] BETWEEN from AND to` under the
// resolved comparison plan.
func evalBetweenExpr(collationEnv *collations.Environment, left, from, to eval, negate bool, plan betweenPlan, now time.Time) (boolean, error) {
	if left == nil {
		return boolNULL, nil
	}

	var cmp func(pair betweenPair, r eval) (int, bool, error)
	switch {
	case plan.legacy:
		cmp = func(_ betweenPair, r eval) (int, bool, error) {
			return evalCompareAll(left, r, false, collationEnv)
		}
	case plan.pairs[0] == pairString:
		// MySQL aggregates a single collation across the whole operand set:
		// the JSON operands make utf8mb4_bin win. Seeding it unconditionally
		// keeps a declared JSON operand whose value is SQL NULL in the
		// aggregation.
		var ca collationAggregation
		if err := ca.add(collationJSON, collationEnv); err != nil {
			return boolNULL, err
		}
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
		cmp = func(_ betweenPair, r eval) (int, bool, error) {
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
		cmp = func(pair betweenPair, r eval) (int, bool, error) {
			if r == nil {
				return 0, true, nil
			}
			switch pair {
			case pairNumeric:
				n, err := compareNumeric(betweenToFloat(left), betweenToFloat(r))
				return n, false, err
			case pairDatetimeText:
				return betweenToDateTime(left, now).Compare(betweenToDateTime(r, now)), false, nil
			case pairDatetimeField:
				return betweenToDateTime(left, now).Compare(betweenFieldToDateTime(r, now)), false, nil
			default: // pairTime
				return betweenToTime(left).Compare(betweenToTime(r)), false, nil
			}
		}
	}

	n, isNull, err := cmp(plan.pairs[0], from)
	if err != nil {
		return boolNULL, err
	}
	cmpFrom := makeboolean2(n >= 0, isNull)
	if cmpFrom == boolFalse {
		// The result is decided: skip the upper-bound comparison, like the
		// equivalent `left >= from AND left <= to` conjunction would.
		return makeboolean(negate), nil
	}
	n, isNull, err = cmp(plan.pairs[1], to)
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
	ops := [3]IR{b.Left, b.From, b.To}
	vals := [3]eval{left, from, to}
	var classes [3]cmpClass
	var provisional [3]bool
	for k := range ops {
		classes[k], provisional[k] = evalCmpClass(env, b.StaticClasses[k], ops[k], vals[k])
	}
	resolveJSONCmpClasses(env, classes[:], provisional[:], ops[:])
	plan := resolveBetweenPlan(classes, b.Left, b.From, b.To, from, to)
	in, err := evalBetweenExpr(env.collationEnv, left, from, to, b.Negate, plan, env.now)
	if err != nil {
		return nil, err
	}
	return in.eval(), nil
}

// compileBoundVal evaluates a constant bound so the plan resolver can test
// its TIME convertibility, exactly like constant folding would.
func compileBoundVal(c *compiler, op IR) eval {
	if lit, ok := op.(*Literal); ok {
		return lit.inner
	}
	if !op.constant() {
		return nil
	}
	v, err := op.eval(EmptyExpressionEnv(c.env))
	if err != nil {
		return nil
	}
	return v
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

	classes := [3]cmpClass{
		mergeCmpClass(expr.StaticClasses[0], lt.Type),
		mergeCmpClass(expr.StaticClasses[1], ft.Type),
		mergeCmpClass(expr.StaticClasses[2], tt.Type),
	}
	if betweenPlanPerExecution(classes, expr.Left, expr.From, expr.To) {
		c.asm.BetweenPerExecution(c.env.CollationEnv(), expr.Negate, classes, expr.Left, expr.From, expr.To)
	} else {
		plan := resolveBetweenPlan(classes, expr.Left, expr.From, expr.To,
			compileBoundVal(c, expr.From), compileBoundVal(c, expr.To))
		c.asm.Between(c.env.CollationEnv(), expr.Negate, plan)
	}
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
