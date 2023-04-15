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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

func (c *compiler) compileComparisonTuple(expr *ComparisonExpr) (ctype, error) {
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

func (c *compiler) compileComparison(expr *ComparisonExpr) (ctype, error) {
	lt, err := c.compileExpr(expr.Left)
	if err != nil {
		return ctype{}, err
	}

	var skip1 *jump
	switch expr.Op.(type) {
	case compareNullSafeEQ:
	default:
		skip1 = c.compileNullCheck1(lt)
	}

	rt, err := c.compileExpr(expr.Right)
	if err != nil {
		return ctype{}, err
	}

	if lt.Type == sqltypes.Tuple || rt.Type == sqltypes.Tuple {
		if lt.Type != rt.Type {
			return ctype{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "did not typecheck tuples during comparison")
		}
		return c.compileComparisonTuple(expr)
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
		}
		if sqltypes.IsDateOrTime(rt.Type) {
			c.asm.Convert_Ti(1)
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

func (c *compiler) compareNumericTypes(lt ctype, rt ctype) (swapped bool) {
	switch lt.Type {
	case sqltypes.Int64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.CmpNum_ii()
		case sqltypes.Uint64:
			c.asm.CmpNum_iu(2, 1)
		case sqltypes.Float64:
			c.asm.CmpNum_if(2, 1)
		case sqltypes.Decimal:
			c.asm.CmpNum_id(2, 1)
		}
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.CmpNum_iu(1, 2)
			swapped = true
		case sqltypes.Uint64:
			c.asm.CmpNum_uu()
		case sqltypes.Float64:
			c.asm.CmpNum_uf(2, 1)
		case sqltypes.Decimal:
			c.asm.CmpNum_ud(2, 1)
		}
	case sqltypes.Float64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.CmpNum_if(1, 2)
			swapped = true
		case sqltypes.Uint64:
			c.asm.CmpNum_uf(1, 2)
			swapped = true
		case sqltypes.Float64:
			c.asm.CmpNum_ff()
		case sqltypes.Decimal:
			c.asm.CmpNum_fd(2, 1)
		}

	case sqltypes.Decimal:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.CmpNum_id(1, 2)
			swapped = true
		case sqltypes.Uint64:
			c.asm.CmpNum_ud(1, 2)
			swapped = true
		case sqltypes.Float64:
			c.asm.CmpNum_fd(1, 2)
			swapped = true
		case sqltypes.Decimal:
			c.asm.CmpNum_dd()
		}
	}
	return
}

func (c *compiler) compareAsStrings(lt ctype, rt ctype) error {
	merged, coerceLeft, coerceRight, err := mergeCollations(lt.Col, rt.Col, lt.Type, rt.Type)
	if err != nil {
		return err
	}
	if coerceLeft == nil && coerceRight == nil {
		c.asm.CmpString_collate(merged.Collation.Get())
	} else {
		if coerceLeft == nil {
			coerceLeft = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		if coerceRight == nil {
			coerceRight = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		c.asm.CmpString_coerce(&compiledCoercion{
			col:   merged.Collation.Get(),
			left:  coerceLeft,
			right: coerceRight,
		})
	}
	return nil
}

func (c *compiler) compareAsJSON(lt ctype, rt ctype) error {
	_, err := c.compileArgToJSON(lt, 2)
	if err != nil {
		return err
	}

	_, err = c.compileArgToJSON(rt, 1)
	if err != nil {
		return err
	}
	c.asm.CmpJSON()

	return nil
}

func (c *compiler) compileCheckTrue(when ctype, offset int) error {
	switch when.Type {
	case sqltypes.Int64:
		c.asm.Convert_iB(offset)
	case sqltypes.Uint64:
		c.asm.Convert_uB(offset)
	case sqltypes.Float64:
		c.asm.Convert_fB(offset)
	case sqltypes.Decimal:
		c.asm.Convert_dB(offset)
	case sqltypes.VarChar, sqltypes.VarBinary:
		c.asm.Convert_bB(offset)
	case sqltypes.Timestamp, sqltypes.Datetime, sqltypes.Time, sqltypes.Date:
		c.asm.Convert_TB(offset)
	case sqltypes.Null:
		c.asm.SetBool(offset, false)
	default:
		return vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unsupported Truth check: %s", when.Type)
	}
	return nil
}

func (c *compiler) compileLike(expr *LikeExpr) (ctype, error) {
	lt, err := c.compileExpr(expr.Left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(expr.Right)
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

func (c *compiler) compileInTable(lhs ctype, rhs TupleExpr) map[vthash.Hash]struct{} {
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

func (c *compiler) compileIn(expr *InExpr) (ctype, error) {
	lhs, err := c.compileExpr(expr.Left)
	if err != nil {
		return ctype{}, nil
	}

	rhs := expr.Right.(TupleExpr)

	if table := c.compileInTable(lhs, rhs); table != nil {
		c.asm.In_table(expr.Negate, table)
	} else {
		_, err := c.compileTuple(rhs)
		if err != nil {
			return ctype{}, err
		}
		c.asm.In_slow(expr.Negate)
	}
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}, nil
}

func (c *compiler) compileNot(expr *NotExpr) (ctype, error) {
	arg, err := c.compileExpr(expr.Inner)
	if err != nil {
		return ctype{}, nil
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Null:
		// No-op.
	case sqltypes.Int64:
		c.asm.Not_i()
	case sqltypes.Uint64:
		c.asm.Not_u()
	case sqltypes.Float64:
		c.asm.Not_f()
	case sqltypes.Decimal:
		c.asm.Not_d()
	case sqltypes.VarChar, sqltypes.VarBinary:
		if arg.isHexOrBitLiteral() {
			c.asm.Convert_xu(1)
			c.asm.Not_u()
		} else {
			c.asm.Convert_bB(1)
			c.asm.Not_i()
		}
	case sqltypes.TypeJSON:
		c.asm.Convert_jB(1)
		c.asm.Not_i()
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Date, sqltypes.Timestamp:
		c.asm.Convert_TB(1)
		c.asm.Not_i()
	default:
		c.asm.Convert_bB(1)
		c.asm.Not_i()
	}
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Flag: flagNullable | flagIsBoolean, Col: collationNumeric}, nil
}

func (c *compiler) compileLogical(expr *LogicalExpr) (ctype, error) {
	lt, err := c.compileExpr(expr.Left)
	if err != nil {
		return ctype{}, err
	}

	switch lt.Type {
	case sqltypes.Null, sqltypes.Int64:
		// No-op.
	case sqltypes.Uint64:
		c.asm.Convert_uB(1)
	case sqltypes.Float64:
		c.asm.Convert_fB(1)
	case sqltypes.Decimal:
		c.asm.Convert_dB(1)
	case sqltypes.VarChar, sqltypes.VarBinary:
		if lt.isHexOrBitLiteral() {
			c.asm.Convert_xu(1)
			c.asm.Convert_uB(1)
		} else {
			c.asm.Convert_bB(1)
		}
	case sqltypes.TypeJSON:
		c.asm.Convert_jB(1)
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Date, sqltypes.Timestamp:
		c.asm.Convert_TB(1)
	default:
		c.asm.Convert_bB(1)
	}

	jump := c.asm.LogicalLeft(expr.opname)

	rt, err := c.compileExpr(expr.Right)
	if err != nil {
		return ctype{}, err
	}

	switch rt.Type {
	case sqltypes.Null, sqltypes.Int64:
		// No-op.
	case sqltypes.Uint64:
		c.asm.Convert_uB(1)
	case sqltypes.Float64:
		c.asm.Convert_fB(1)
	case sqltypes.Decimal:
		c.asm.Convert_dB(1)
	case sqltypes.VarChar, sqltypes.VarBinary:
		if rt.isHexOrBitLiteral() {
			c.asm.Convert_xu(1)
			c.asm.Convert_uB(1)
		} else {
			c.asm.Convert_bB(1)
		}
	case sqltypes.TypeJSON:
		c.asm.Convert_jB(1)
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Date, sqltypes.Timestamp:
		c.asm.Convert_TB(1)
	default:
		c.asm.Convert_bB(1)
	}

	c.asm.LogicalRight(expr.opname)
	c.asm.jumpDestination(jump)
	return ctype{Type: sqltypes.Int64, Flag: flagNullable | flagIsBoolean, Col: collationNumeric}, nil
}
