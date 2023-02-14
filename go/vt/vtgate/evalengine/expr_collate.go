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
	"strconv"
	"strings"

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

var collationJSON = collations.TypedCollation{
	Collation:    46, // utf8mb4_bin
	Coercibility: collations.CoerceCoercible,
	Repertoire:   collations.RepertoireUnicode,
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

func (c *CollateExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
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

type LookupIntegrationTest struct {
	Collation collations.ID
}

func (*LookupIntegrationTest) ColumnLookup(name *sqlparser.ColName) (int, error) {
	n := name.CompliantName()
	if strings.HasPrefix(n, "column") {
		return strconv.Atoi(n[len("column"):])
	}
	return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown column: %q", n)
}

func (tl *LookupIntegrationTest) CollationForExpr(_ sqlparser.Expr) collations.ID {
	return tl.Collation
}

func (tl *LookupIntegrationTest) DefaultCollation() collations.ID {
	return tl.Collation
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

func mergeCollations(left, right eval) (eval, eval, collations.ID, error) {
	lc := evalCollation(left)
	rc := evalCollation(right)
	if lc.Collation == rc.Collation {
		return left, right, lc.Collation, nil
	}

	lt := evalResultIsTextual(left)
	rt := evalResultIsTextual(right)
	if !lt || !rt {
		if lt {
			return left, right, lc.Collation, nil
		}
		if rt {
			return left, right, rc.Collation, nil
		}
		return left, right, collations.CollationBinaryID, nil
	}

	env := collations.Local()
	mc, coerceLeft, coerceRight, err := env.MergeCollations(lc, rc, collations.CoercionOptions{
		ConvertToSuperset:   true,
		ConvertWithCoercion: true,
	})
	if err != nil {
		return nil, nil, 0, err
	}

	left1 := newEvalRaw(left.SQLType(), left.(*evalBytes).bytes, mc)
	right1 := newEvalRaw(right.SQLType(), right.(*evalBytes).bytes, mc)

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
	cur  collations.TypedCollation
	init bool
}

func (ca *collationAggregation) add(env *collations.Environment, tc collations.TypedCollation) error {
	if !ca.init {
		ca.cur = tc
		ca.init = true
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
