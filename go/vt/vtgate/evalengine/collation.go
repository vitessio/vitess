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
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
)

func typedCoercionCollation(typ sqltypes.Type, id collations.ID) collations.TypedCollation {
	switch {
	case sqltypes.IsNull(typ):
		return collationNull
	case sqltypes.IsNumber(typ) || sqltypes.IsDateOrTime(typ):
		return collationNumeric
	case typ == sqltypes.TypeJSON:
		return collationJSON
	default:
		return collations.TypedCollation{
			Collation:    id,
			Coercibility: collations.CoerceCoercible,
			Repertoire:   collations.RepertoireUnicode,
		}
	}
}

// typedColumnCollation returns the collation of a column of type typ.
//
// It differs from typedCoercionCollation in the coercibility of a string
// column, which is implicit where the one of a literal is coercible. When
// the two meet, the collation of the column wins: with a text column s,
// s = 0x41 compares with the collation of s, not byte by byte.
func typedColumnCollation(typ sqltypes.Type, id collations.ID) collations.TypedCollation {
	tc := typedCoercionCollation(typ, id)
	switch typ {
	case sqltypes.HexNum, sqltypes.HexVal, sqltypes.BitNum:
		// these types only describe literals
	default:
		if tc.Coercibility == collations.CoerceCoercible {
			tc.Coercibility = collations.CoerceImplicit
		}
	}
	return tc
}

func evalCollation(e eval) collations.TypedCollation {
	switch e := e.(type) {
	case nil:
		return collationNull
	case evalNumeric, *evalTemporal:
		return collationNumeric
	case *evalJSON:
		return collationJSON
	case *evalBytes:
		return e.col
	default:
		return collationBinary
	}
}

func mergeCollations(c1, c2 collations.TypedCollation, t1, t2 sqltypes.Type, env *collations.Environment) (collations.TypedCollation, colldata.Coercion, colldata.Coercion, error) {
	if c1.Collation == c2.Collation {
		return c1, nil, nil, nil
	}

	lt := sqltypes.IsTextOrBinary(t1)
	rt := sqltypes.IsTextOrBinary(t2)
	if !lt || !rt {
		if lt {
			return c1, nil, nil, nil
		}
		if rt {
			return c2, nil, nil, nil
		}
		return collationBinary, nil, nil, nil
	}

	return colldata.Merge(env, c1, c2, colldata.CoercionOptions{
		ConvertToSuperset:   true,
		ConvertWithCoercion: true,
	})
}

func mergeAndCoerceCollations(left, right eval, env *collations.Environment) (eval, eval, collations.TypedCollation, error) {
	lt := left.SQLType()
	rt := right.SQLType()

	mc, coerceLeft, coerceRight, err := mergeCollations(evalCollation(left), evalCollation(right), lt, rt, env)
	if err != nil {
		return nil, nil, collations.TypedCollation{}, err
	}
	if coerceLeft == nil && coerceRight == nil {
		return left, right, mc, nil
	}

	left1 := newEvalRaw(lt, left.(*evalBytes).bytes, mc)
	right1 := newEvalRaw(rt, right.(*evalBytes).bytes, mc)

	if coerceLeft != nil {
		left1.bytes, err = coerceLeft(nil, left1.bytes)
		if err != nil {
			return nil, nil, collations.TypedCollation{}, err
		}
	}
	if coerceRight != nil {
		right1.bytes, err = coerceRight(nil, right1.bytes)
		if err != nil {
			return nil, nil, collations.TypedCollation{}, err
		}
	}
	return left1, right1, mc, nil
}

type collationAggregation struct {
	cur collations.TypedCollation
	// unknown is set by addTyped when one of the strings has a collation
	// that is not known. See unknownCollation.
	unknown bool
	// binary is set by addTyped when one of the strings is a binary string
	// that is not a literal, such as a binary column.
	binary bool
}

// unknownCollation reports whether col is the collation of a string whose
// collation is not known, such as the string of a column that has no
// declared type. It is the binary collation with the coercibility of a
// number, where a binary string has the one of a column or of a literal.
func unknownCollation(col collations.TypedCollation) bool {
	return col.Collation == collations.CollationBinaryID && col.Coercibility == collations.CoerceNumeric
}

// addTyped is add for the collation tc of a value of type tt. It notes
// when the value is a string whose collation is not known, and when it is
// a binary string that is not a literal. See unknownResult.
func (ca *collationAggregation) addTyped(tt sqltypes.Type, tc collations.TypedCollation, env *collations.Environment) error {
	if tt == sqltypes.Enum || tt == sqltypes.Set || sqltypes.IsTextOrBinary(tt) {
		switch {
		case unknownCollation(tc):
			ca.unknown = true
		case tc.Collation == collations.CollationBinaryID && tc.Coercibility <= collations.CoerceImplicit:
			ca.binary = true
		}
	}
	return ca.add(tc, env)
}

// unknownResult reports whether the collation of the result is not known:
// one of the strings has a collation that is not known, and no collation
// won over it.
//
// Such a string comes from a column, so it wins over a binary literal as
// the collation of a column does, and loses to a binary column: with a
// column s that has no declared type, IFNULL(s, 0x41) has the unknown
// collation of s.
func (ca *collationAggregation) unknownResult() bool {
	return ca.unknown && !ca.binary && ca.cur.Collation == collations.CollationBinaryID
}

func (ca *collationAggregation) add(tc collations.TypedCollation, env *collations.Environment) error {
	if ca.cur.Collation == collations.Unknown {
		ca.cur = tc
	} else {
		var err error
		ca.cur, _, _, err = colldata.Merge(env, ca.cur, tc, colldata.CoercionOptions{ConvertToSuperset: true, ConvertWithCoercion: true})
		if err != nil {
			return err
		}
	}
	return nil
}

func (ca *collationAggregation) result() collations.TypedCollation {
	return ca.cur
}

// resultFor is result for a function that makes a string of its arguments,
// where conn is the connection collation.
//
// A string whose collation is not known comes from a column, and the
// collation of a column wins over the one of a literal. When such a string
// is combined with a literal that has the connection collation, the result
// keeps that collation and is as strong as a column: with a column s that
// has no declared type, CONCAT(s, 'a') wins over a hex literal as s does.
func (ca *collationAggregation) resultFor(conn collations.ID) collations.TypedCollation {
	cur := ca.cur
	if ca.unknown && conn != collations.Unknown && cur.Collation == conn && cur.Coercibility > collations.CoerceImplicit {
		cur.Coercibility = collations.CoerceImplicit
	}
	return cur
}

// concatResult is resultFor for CONCAT, CONCAT_WS and ELT, whose result
// has the type tt. Numbers have no collation: when there are only numbers,
// or strings whose collation is not known, the result has the connection
// collation.
func (ca *collationAggregation) concatResult(tt sqltypes.Type, conn collations.ID) collations.TypedCollation {
	tc := ca.resultFor(conn)
	if tc.Coercibility == collations.CoerceNumeric {
		tc = typedCoercionCollation(tt, conn)
		if ca.unknown && tc.Collation != collations.CollationBinaryID {
			tc.Coercibility = collations.CoerceImplicit
		}
	}
	return tc
}
