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
