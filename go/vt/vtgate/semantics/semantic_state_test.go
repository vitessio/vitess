/*
Copyright 2022 The Vitess Authors.

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

package semantics

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestBindingAndExprEquality(t *testing.T) {
	tests := []struct {
		expressions string
		equal       bool
	}{{
		expressions: "t1_id+1, t1.t1_id+1",
		equal:       true,
	}, {
		expressions: "t2_id+1, t1_id+1",
		equal:       false,
	}, {
		expressions: "(t1_id+1)+1, t1.t1_id+1+1",
		equal:       true,
	}}

	for _, test := range tests {
		t.Run(test.expressions, func(t *testing.T) {
			parse, err := sqlparser.Parse(fmt.Sprintf("select %s from t1, t2", test.expressions))
			require.NoError(t, err)
			st, err := Analyze(parse, "db", fakeSchemaInfoTest())
			require.NoError(t, err)
			exprs := parse.(*sqlparser.Select).SelectExprs
			a := exprs[0].(*sqlparser.AliasedExpr).Expr
			b := exprs[1].(*sqlparser.AliasedExpr).Expr
			assert.Equal(t, st.EqualsExpr(a, b), test.equal)
		})
	}
}

func fakeSchemaInfoTest() *FakeSI {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("t1_id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("t2_id"),
		Type: querypb.Type_INT64,
	}}

	si := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t1": {Name: sqlparser.NewIdentifierCS("t1"), Columns: cols1, ColumnListAuthoritative: true, Keyspace: ks2},
			"t2": {Name: sqlparser.NewIdentifierCS("t2"), Columns: cols2, ColumnListAuthoritative: true, Keyspace: ks3},
		},
	}
	return si
}
