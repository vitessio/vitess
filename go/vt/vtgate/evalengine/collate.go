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
	Coercibility: collations.CoerceExplicit,
	Repertoire:   collations.RepertoireASCII,
}

func (c *CollateExpr) eval(env *ExpressionEnv, out *EvalResult) {
	out.init(env, c.Inner)
	if err := collations.Local().EnsureCollate(out.collation().Collation, c.TypedCollation.Collation); err != nil {
		throwEvalError(vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error()))
	}
	out.replaceCollation(c.TypedCollation)
}
