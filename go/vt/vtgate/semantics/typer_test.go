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
			parse, known, err := sqlparser.NewTestParser().Parse2(test.query)
			require.NoError(t, err)

			rv := sqlparser.NewReservedVars("", known)
			out, err := sqlparser.Normalize(parse, rv, map[string]*querypb.BindVariable{}, true, "d", 0, "", map[string]string{}, nil, nil)
			require.NoError(t, err)

			st, err := Analyze(out.AST, "d", fakeSchemaInfo())
			require.NoError(t, err)

			bv := extract(out.AST.(*sqlparser.Select), 0).(*sqlparser.Argument)
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
			ast, err := sqlparser.NewTestParser().Parse(test.query)
			require.NoError(t, err)

			out, err := sqlparser.Normalize(ast, sqlparser.NewReservedVars("bv", sqlparser.BindVars{}), map[string]*querypb.BindVariable{}, true, "d", 0, "", map[string]string{}, nil, nil)
			require.NoError(t, err)

			st, err := Analyze(out.AST, "d", fakeSchemaInfo())
			require.NoError(t, err)
			col := extract(out.AST.(*sqlparser.Select), 0)
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

func TestWindowFunctionTypes(t *testing.T) {
	tests := []struct {
		query, typ string
	}{
		// Pure window functions
		{query: "select rank() over () from t1", typ: "INT64"},
		{query: "select row_number() over () from t1", typ: "INT64"},
		{query: "select dense_rank() over () from t1", typ: "INT64"},
		{query: "select ntile(1) over () from t1", typ: "INT64"},
		{query: "select percent_rank() over () from t1", typ: "FLOAT64"},
		{query: "select cume_dist() over () from t1", typ: "FLOAT64"},

		// Window functions that depend on input type
		{query: "select first_value(id) over () from t1", typ: "INT64"}, // id is INT64 in fakeSchemaInfo
		{query: "select last_value(id) over () from t1", typ: "INT64"},
		{query: "select nth_value(id, 1) over () from t1", typ: "INT64"},
		{query: "select lead(id, 1) over () from t1", typ: "INT64"},
		{query: "select lag(id, 1) over () from t1", typ: "INT64"},

		// Aggregates used as window functions
		{query: "select sum(id) over () from t1", typ: "DECIMAL"}, // SUM(INT) -> DECIMAL
		{query: "select count(*) over () from t1", typ: "INT64"},
		{query: "select min(id) over () from t1", typ: "INT64"},
		{query: "select max(id) over () from t1", typ: "INT64"},
		{query: "select avg(id) over () from t1", typ: "DECIMAL"}, // AVG(INT) -> DECIMAL
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			parse, _, err := sqlparser.NewTestParser().Parse2(test.query)
			require.NoError(t, err)

			st, err := Analyze(parse, "d", fakeSchemaInfo())
			require.NoError(t, err)

			sel := parse.(*sqlparser.Select)
			expr := extract(sel, 0)

			typ, found := st.TypeForExpr(expr)
			require.True(t, found, "expression was not typed")
			require.Equal(t, test.typ, typ.Type().String())
		})
	}
}
