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
	Coercibility: collations.CoerceImplicit,
	Repertoire:   collations.RepertoireUnicode,
}

var collationUtf8mb3 = collations.TypedCollation{
	Collation:    collations.CollationUtf8mb3ID,
	Coercibility: collations.CoerceCoercible,
	Repertoire:   collations.RepertoireUnicode,
}

var collationRegexpFallback = collations.TypedCollation{
	Collation:    collations.CollationLatin1Swedish,
	Coercibility: collations.CoerceCoercible,
	Repertoire:   collations.RepertoireASCII,
}

type (
	CollateExpr struct {
		UnaryExpr
		TypedCollation collations.TypedCollation
		CollationEnv   *collations.Environment
	}

	IntroducerExpr struct {
		UnaryExpr
		TypedCollation collations.TypedCollation
		CollationEnv   *collations.Environment
	}
)

var _ IR = (*CollateExpr)(nil)

func (c *CollateExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := c.Inner.eval(env)
	if err != nil {
		return nil, err
	}

	var b *evalBytes
	switch e := e.(type) {
	case nil:
		return nil, nil
	case *evalBytes:
		if err := env.collationEnv.EnsureCollate(e.col.Collation, c.TypedCollation.Collation); err != nil {
			return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
		}
		b = e.withCollation(c.TypedCollation)
	default:
		b, err = evalToVarchar(e, c.TypedCollation.Collation, true)
		if err != nil {
			return nil, err
		}
	}

	b.flag |= flagExplicitCollation
	return b, nil
}

func (expr *CollateExpr) compile(c *compiler) (ctype, error) {
	ct, err := expr.Inner.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(ct)

	switch ct.Type {
	case sqltypes.VarChar:
		if err := c.env.CollationEnv().EnsureCollate(ct.Col.Collation, expr.TypedCollation.Collation); err != nil {
			return ctype{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
		}
		fallthrough
	case sqltypes.VarBinary:
		c.asm.Collate(expr.TypedCollation.Collation)
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, expr.TypedCollation.Collation, nil)
	}

	c.asm.jumpDestination(skip)

	ct.Type = sqltypes.VarChar
	ct.Col = expr.TypedCollation
	ct.Flag |= flagExplicitCollation | flagNullable
	return ct, nil
}

var _ IR = (*IntroducerExpr)(nil)

func (expr *IntroducerExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := expr.Inner.eval(env)
	if err != nil {
		return nil, err
	}

	var b *evalBytes
	if expr.TypedCollation.Collation == collations.CollationBinaryID {
		b = evalToBinary(e)
	} else {
		b, err = evalToVarchar(e, expr.TypedCollation.Collation, false)
		if err != nil {
			return nil, err
		}
	}
	b.flag |= flagExplicitCollation
	return b, nil
}

func (expr *IntroducerExpr) compile(c *compiler) (ctype, error) {
	_, err := expr.Inner.compile(c)
	if err != nil {
		return ctype{}, err
	}

	var ct ctype
	ct.Type = sqltypes.VarChar
	if expr.TypedCollation.Collation == collations.CollationBinaryID {
		ct.Type = sqltypes.VarBinary
	}
	c.asm.Introduce(1, ct.Type, expr.TypedCollation)
	ct.Col = expr.TypedCollation
	ct.Flag = flagExplicitCollation
	return ct, nil
}
