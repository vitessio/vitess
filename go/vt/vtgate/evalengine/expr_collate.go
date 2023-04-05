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

var collationNull = collations.TypedCollation{
	Collation:    collations.CollationBinaryID,
	Coercibility: collations.CoerceIgnorable,
	Repertoire:   collations.RepertoireASCII,
}

var collationNumeric = collations.TypedCollation{
	Collation:    collations.CollationBinaryID,
	Coercibility: collations.CoerceNumeric,
	Repertoire:   collations.RepertoireASCII,
}

var collationBinary = collations.TypedCollation{
	Collation:    collations.CollationBinaryID,
	Coercibility: collations.CoerceCoercible,
	Repertoire:   collations.RepertoireASCII,
}

var collationJSON = collations.TypedCollation{
	Collation:    46, // utf8mb4_bin
	Coercibility: collations.CoerceCoercible,
	Repertoire:   collations.RepertoireUnicode,
}

var collationUtf8mb3 = collations.TypedCollation{
	Collation:    collations.CollationUtf8ID,
	Coercibility: collations.CoerceCoercible,
	Repertoire:   collations.RepertoireASCII,
}

type (
	CollateExpr struct {
		UnaryExpr
		TypedCollation collations.TypedCollation
	}
)

var _ Expr = (*CollateExpr)(nil)

func (c *CollateExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := c.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	switch e := e.(type) {
	case nil:
		return nil, nil
	case *evalBytes:
		if err := collations.Local().EnsureCollate(e.col.Collation, c.TypedCollation.Collation); err != nil {
			return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
		}
		return e.withCollation(c.TypedCollation), nil
	default:
		return evalToVarchar(e, c.TypedCollation.Collation, true)
	}
}

func (c *CollateExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	t, f := c.Inner.typeof(env, fields)
	return t, f | flagExplicitCollation
}

func evalCollation(e eval) collations.TypedCollation {
	switch e := e.(type) {
	case nil:
		return collationNull
	case evalNumeric:
		return collationNumeric
	case *evalJSON:
		return collationJSON
	case *evalBytes:
		return e.col
	default:
		return collationBinary
	}
}

func mergeCollations(c1, c2 collations.TypedCollation, t1, t2 sqltypes.Type) (collations.TypedCollation, collations.Coercion, collations.Coercion, error) {
	if c1.Collation == c2.Collation {
		return c1, nil, nil, nil
	}

	lt := sqltypes.IsText(t1) || sqltypes.IsBinary(t1)
	rt := sqltypes.IsText(t2) || sqltypes.IsBinary(t2)
	if !lt || !rt {
		if lt {
			return c1, nil, nil, nil
		}
		if rt {
			return c2, nil, nil, nil
		}
		return collationBinary, nil, nil, nil
	}

	env := collations.Local()
	return env.MergeCollations(c1, c2, collations.CoercionOptions{
		ConvertToSuperset:   true,
		ConvertWithCoercion: true,
	})
}

func mergeAndCoerceCollations(left, right eval) (eval, eval, collations.ID, error) {
	lt := left.SQLType()
	rt := right.SQLType()

	mc, coerceLeft, coerceRight, err := mergeCollations(evalCollation(left), evalCollation(right), lt, rt)
	if err != nil {
		return nil, nil, 0, err
	}
	if coerceLeft == nil && coerceRight == nil {
		return left, right, mc.Collation, nil
	}

	left1 := newEvalRaw(lt, left.(*evalBytes).bytes, mc)
	right1 := newEvalRaw(rt, right.(*evalBytes).bytes, mc)

	if coerceLeft != nil {
		left1.bytes, err = coerceLeft(nil, left1.bytes)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	if coerceRight != nil {
		right1.bytes, err = coerceRight(nil, right1.bytes)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	return left1, right1, mc.Collation, nil
}

type collationAggregation struct {
	cur collations.TypedCollation
}

func (ca *collationAggregation) add(env *collations.Environment, tc collations.TypedCollation) error {
	if ca.cur.Collation == collations.Unknown {
		ca.cur = tc
	} else {
		var err error
		ca.cur, _, _, err = env.MergeCollations(ca.cur, tc, collations.CoercionOptions{ConvertToSuperset: true, ConvertWithCoercion: true})
		if err != nil {
			return err
		}
	}
	return nil
}

func (ca *collationAggregation) result() collations.TypedCollation {
	return ca.cur
}
