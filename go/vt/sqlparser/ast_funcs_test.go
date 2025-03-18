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

package sqlparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestAddQueryHint(t *testing.T) {
	tcs := []struct {
		comments  Comments
		queryHint string
		expected  Comments
		err       string
	}{
		{
			comments:  Comments{},
			queryHint: "",
			expected:  nil,
		},
		{
			comments:  Comments{},
			queryHint: "SET_VAR(aa)",
			expected:  Comments{"/*+ SET_VAR(aa) */"},
		},
		{
			comments:  Comments{"/* toto */"},
			queryHint: "SET_VAR(aa)",
			expected:  Comments{"/*+ SET_VAR(aa) */", "/* toto */"},
		},
		{
			comments:  Comments{"/* toto */", "/*+ SET_VAR(bb) */"},
			queryHint: "SET_VAR(aa)",
			expected:  Comments{"/*+ SET_VAR(bb) SET_VAR(aa) */", "/* toto */"},
		},
		{
			comments:  Comments{"/* toto */", "/*+ SET_VAR(bb) "},
			queryHint: "SET_VAR(aa)",
			err:       "Query hint comment is malformed",
		},
		{
			comments:  Comments{"/* toto */", "/*+ SET_VAR(bb) */", "/*+ SET_VAR(cc) */"},
			queryHint: "SET_VAR(aa)",
			err:       "Must have only one query hint",
		},
		{
			comments:  Comments{"/*+ SET_VAR(bb) */"},
			queryHint: "SET_VAR(bb)",
			expected:  Comments{"/*+ SET_VAR(bb) */"},
		},
	}

	for i, tc := range tcs {
		comments := tc.comments.Parsed()
		t.Run(fmt.Sprintf("%d %s", i, String(comments)), func(t *testing.T) {
			got, err := comments.AddQueryHint(tc.queryHint)
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestSQLTypeToQueryType(t *testing.T) {
	tcs := []struct {
		input    string
		unsigned bool
		output   querypb.Type
	}{
		{
			input:    "tinyint",
			unsigned: true,
			output:   sqltypes.Uint8,
		},
		{
			input:    "tinyint",
			unsigned: false,
			output:   sqltypes.Int8,
		},
		{
			input:  "double",
			output: sqltypes.Float64,
		},
		{
			input:  "float8",
			output: sqltypes.Float64,
		},
		{
			input:  "float",
			output: sqltypes.Float32,
		},
		{
			input:  "float4",
			output: sqltypes.Float32,
		},
		{
			input:  "decimal",
			output: sqltypes.Decimal,
		},
	}

	for _, tc := range tcs {
		name := tc.input
		if tc.unsigned {
			name += " unsigned"
		}
		t.Run(name, func(t *testing.T) {
			got := SQLTypeToQueryType(tc.input, tc.unsigned)
			require.Equal(t, tc.output, got)
		})
	}
}

// TestColumns_Indexes verifies the functionality of Indexes method on Columns.
func TestColumns_Indexes(t *testing.T) {
	tests := []struct {
		name          string
		cols          Columns
		subSetCols    Columns
		indexesWanted []int
	}{
		{
			name:       "Not a subset",
			cols:       MakeColumns("col1", "col2", "col3"),
			subSetCols: MakeColumns("col2", "col4"),
		}, {
			name:          "Subset with 1 value",
			cols:          MakeColumns("col1", "col2", "col3"),
			subSetCols:    MakeColumns("col2"),
			indexesWanted: []int{1},
		}, {
			name:          "Subset with multiple values",
			cols:          MakeColumns("col1", "col2", "col3", "col4", "col5"),
			subSetCols:    MakeColumns("col3", "col5", "col1"),
			indexesWanted: []int{2, 4, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isSubset, indexes := tt.cols.Indexes(tt.subSetCols)
			if tt.indexesWanted == nil {
				require.False(t, isSubset)
				require.Nil(t, indexes)
				return
			}
			require.True(t, isSubset)
			require.EqualValues(t, tt.indexesWanted, indexes)
		})
	}
}

// TestExtractTables verifies the functionality of extracting all the tables from the SQLNode.
func TestExtractTables(t *testing.T) {
	tcases := []struct {
		sql      string
		expected []string
	}{{
		sql:      "select 1 from a",
		expected: []string{"a"},
	}, {
		sql:      "select 1 from a, b",
		expected: []string{"a", "b"},
	}, {
		sql:      "select 1 from a join b on a.id = b.id",
		expected: []string{"a", "b"},
	}, {
		sql:      "select 1 from a join b on a.id = b.id join c on b.id = c.id",
		expected: []string{"a", "b", "c"},
	}, {
		sql:      "select 1 from a join (select id from b) as c on a.id = c.id",
		expected: []string{"a", "b"},
	}, {
		sql:      "(select 1 from a) union (select 1 from b)",
		expected: []string{"a", "b"},
	}, {
		sql:      "select 1 from a where exists (select 1 from (select id from c) b where a.id = b.id)",
		expected: []string{"a", "c"},
	}, {
		sql:      "select 1 from k.a join k.b on a.id = b.id",
		expected: []string{"k.a", "k.b"},
	}, {
		sql:      "select 1 from k.a join l.a on k.a.id = l.a.id",
		expected: []string{"k.a", "l.a"},
	}, {
		sql:      "select 1 from a join (select id from a) as c on a.id = c.id",
		expected: []string{"a"},
	}}
	parser := NewTestParser()
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			stmt, err := parser.Parse(tcase.sql)
			require.NoError(t, err)
			tables := ExtractAllTables(stmt)
			require.Equal(t, tcase.expected, tables)
		})
	}
}

// TestRemoveKeyspace tests the RemoveKeyspaceIgnoreSysSchema function.
// It removes all the keyspace except system schema.
func TestRemoveKeyspaceIgnoreSysSchema(t *testing.T) {
	stmt, err := NewTestParser().Parse("select 1 from uks.unsharded join information_schema.tables")
	require.NoError(t, err)
	RemoveKeyspaceIgnoreSysSchema(stmt)

	require.Equal(t, "select 1 from unsharded join information_schema.`tables`", String(stmt))
}

// TestRemoveSpecificKeyspace tests the RemoveSpecificKeyspace function.
// It removes the specific keyspace from the database qualifier.
func TestRemoveSpecificKeyspace(t *testing.T) {
	stmt, err := NewTestParser().Parse("select 1 from uks.unsharded")
	require.NoError(t, err)

	// does not match
	RemoveSpecificKeyspace(stmt, "ks2")
	require.Equal(t, "select 1 from uks.unsharded", String(stmt))

	// match
	RemoveSpecificKeyspace(stmt, "uks")
	require.Equal(t, "select 1 from unsharded", String(stmt))
}

// TestAddKeyspace tests the AddKeyspace function which adds the keyspace to the non-qualified table.
func TestKeyspaceToNonQualifiedTable(t *testing.T) {
	stmt, err := NewTestParser().Parse("select col, col + (select 1 from t4) from ks.t join t2 join (select 1 from t3) as x where t.id = t2.id and x.id = t.id")
	require.NoError(t, err)

	// add keyspace to non qualified table
	AddKeyspace(stmt, "ks2")
	require.Equal(t, "select col, col + (select 1 from ks2.t4) from ks.t join ks2.t2 join (select 1 from ks2.t3) as x where t.id = t2.id and x.id = t.id", String(stmt))
}
