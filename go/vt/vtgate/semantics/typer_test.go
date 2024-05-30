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

package semantics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/colldata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestNormalizerAndSemanticAnalysisIntegration(t *testing.T) {
	// This test runs the normalizer which extracts literals and replaces them with arguments
	// It then tests that the semantic state contains the correct type
	tests := []struct {
		query, typ string
	}{
		{query: "select 1", typ: "INT64"},
		{query: "select 1.2", typ: "DECIMAL"},
		{query: "select 'text'", typ: "VARCHAR"},
		{query: "select 0x1234", typ: "HEXNUM"},
		{query: "select x'7b7d'", typ: "HEXVAL"},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			parse, err := sqlparser.NewTestParser().Parse(test.query)
			require.NoError(t, err)

			err = sqlparser.Normalize(parse, sqlparser.NewReservedVars("bv", sqlparser.BindVars{}), map[string]*querypb.BindVariable{})
			require.NoError(t, err)

			st, err := Analyze(parse, "d", fakeSchemaInfo())
			require.NoError(t, err)
			bv := parse.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.Argument)
			typ, found := st.ExprTypes[bv]
			require.True(t, found, "bindvar was not typed")
			require.Equal(t, test.typ, typ.Type().String())
		})
	}
}

// Tests that the types correctly picks up and sets the collation on columns
func TestColumnCollations(t *testing.T) {
	tests := []struct {
		query, collation string
	}{
		{query: "select textcol from t2"},
		{query: "select name from t2", collation: "utf8mb3_bin"},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			parse, err := sqlparser.NewTestParser().Parse(test.query)
			require.NoError(t, err)

			err = sqlparser.Normalize(parse, sqlparser.NewReservedVars("bv", sqlparser.BindVars{}), map[string]*querypb.BindVariable{})
			require.NoError(t, err)

			st, err := Analyze(parse, "d", fakeSchemaInfo())
			require.NoError(t, err)
			col := extract(parse.(*sqlparser.Select), 0)
			typ, found := st.TypeForExpr(col)
			require.True(t, found, "column was not typed")

			require.Equal(t, "VARCHAR", typ.Type().String())
			collation := colldata.Lookup(typ.Collation())
			if test.collation != "" {
				collation := colldata.Lookup(typ.Collation())
				require.NotNil(t, collation)
				require.Equal(t, test.collation, collation.Name())
			} else {
				require.Nil(t, collation)
			}
		})
	}
}
