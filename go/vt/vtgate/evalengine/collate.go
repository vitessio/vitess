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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
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

func (c *CollateExpr) eval(env *ExpressionEnv, out *EvalResult) {
	out.init(env, c.Inner)
	if err := collations.Local().EnsureCollate(out.collation().Collation, c.TypedCollation.Collation); err != nil {
		throwEvalError(vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error()))
	}
	out.replaceCollation(c.TypedCollation)
}

func (c *CollateExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	t, f := c.Inner.typeof(env)
	return t, f | flagExplicitCollation
}

type LookupDefaultCollation collations.ID

func (d LookupDefaultCollation) ColumnLookup(_ *sqlparser.ColName) (int, error) {
	return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "column access not supported here")
}

func (d LookupDefaultCollation) CollationForExpr(_ sqlparser.Expr) collations.ID {
	return collations.Unknown
}

func (d LookupDefaultCollation) DefaultCollation() collations.ID {
	return collations.ID(d)
}

func mergeCollations(left, right *EvalResult) (collations.ID, error) {
	lc := left.collation()
	rc := right.collation()
	if lc.Collation == rc.Collation {
		return lc.Collation, nil
	}

	lt := left.isTextual()
	rt := right.isTextual()
	if !lt || !rt {
		if lt {
			return lc.Collation, nil
		}
		if rt {
			return rc.Collation, nil
		}
		return collations.CollationBinaryID, nil
	}

	env := collations.Local()
	mc, coerceLeft, coerceRight, err := env.MergeCollations(lc, rc, collations.CoercionOptions{
		ConvertToSuperset:   true,
		ConvertWithCoercion: true,
	})
	if err != nil {
		return 0, err
	}

	if coerceLeft != nil {
		left.bytes_, err = coerceLeft(nil, left.bytes())
		if err != nil {
			throwEvalError(err)
		}
	}
	if coerceRight != nil {
		right.bytes_, err = coerceRight(nil, right.bytes())
		if err != nil {
			throwEvalError(err)
		}
	}

	left.replaceCollation(mc)
	right.replaceCollation(mc)
	return mc.Collation, nil
}
