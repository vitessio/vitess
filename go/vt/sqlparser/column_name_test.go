/*
Copyright 2026 The Vitess Authors.

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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPreserveColumnNames tests that CREATE TABLE ... SELECT and views keep
// the names that MySQL gives the columns they make from expressions without
// an alias. The names in the expected output are the ones that MySQL 8.4
// gives the columns of each statement as written.
func TestPreserveColumnNames(t *testing.T) {
	testcases := []struct {
		name   string
		input  string
		output string
	}{{
		name:   "an expression is named as written",
		input:  "create table t2 as select a+b, concat( 'x' ), - 1, (a) from t1",
		output: "create table t2 as select a + b as `a+b`, concat('x') as `concat( 'x' )`, -1 as `- 1`, a from t1",
	}, {
		name:   "a literal is named as written, but a string after its value",
		input:  "create table t2 as select x'41', b'1', TRUE, 'abc', \"def\", N'nn', _utf8mb4'xyz', 1.50, null from t1",
		output: "create table t2 as select X'41' as `x'41'`, 0b1 as `b'1'`, true as `TRUE`, 'abc', 'def', N'nn', _utf8mb4 'xyz', 1.50, null from t1",
	}, {
		name:   "strings in a row are named after the first one",
		input:  "create table t2 as select 'a' 'b' from t1",
		output: "create table t2 as select 'ab' as a from t1",
	}, {
		name:   "an alias and a column keep their names",
		input:  "create table t2 as select a+b as s, t1.c from t1",
		output: "create table t2 as select a + b as s, t1.c from t1",
	}, {
		name:   "the first query block of a union names the columns",
		input:  "create table t2 as select a+b from t1 union select a+b from t1",
		output: "create table t2 as select a + b as `a+b` from t1 union select a + b from t1",
	}, {
		name:   "a derived table and a common table expression keep the names that the outer query refers to",
		input:  "create table t2 as with c as (select a+b from t1) select * from c, (select a-b from t1) as d",
		output: "create table t2 as with c as (select a + b as `a+b` from t1) select * from c, (select a - b as `a-b` from t1) as d",
	}, {
		name:   "a derived table with a list of names keeps it",
		input:  "create table t2 as select * from (select a+b from t1) as d (s)",
		output: "create table t2 as select * from (select a + b from t1) as d(s)",
	}, {
		name:   "a view names a duplicate generated name after it",
		input:  "create view v as select a+b, a+b, 1, 1 from t1",
		output: "create view v as select a + b as `a+b`, a + b as `Name_exp_a+b`, 1, 1 as Name_exp_1 from t1",
	}, {
		name:   "a view gives a name that is too long for a column a synthetic name",
		input:  "create view v as select concat('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') from t1",
		output: "create view v as select concat('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') as Name_exp_1 from t1",
	}, {
		name:   "a view renames a generated name that a column has",
		input:  "create view v as select 'a', a from t1",
		output: "create view v as select 'a' as Name_exp_a, a from t1",
	}, {
		name:   "a view with a list of names keeps it",
		input:  "alter view v (p) as select a+b from t1",
		output: "alter view v(p) as select a + b from t1",
	}, {
		// MySQL rejects two columns that are named the same, so the view is
		// left for MySQL to reject the same way.
		name:   "a view with two columns of the same name is left as it is",
		input:  "create view v as select a, a, a+b from t1",
		output: "create view v as select a, a, a + b from t1",
	}, {
		name:   "a query that is not part of DDL keeps its expressions as they are",
		input:  "select * from (select a+b from t1) as d",
		output: "select * from (select a + b from t1) as d",
	}}
	parser := NewTestParser()
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.output, String(stmt))
		})
	}
}

// TestMakeViewColumnNamesTruncates tests that a synthetic name stays within
// the length of a column name.
func TestMakeViewColumnNamesTruncates(t *testing.T) {
	long := strings.Repeat("x", maxColumnNameLength)
	columns := []viewColumn{{name: long}, {name: long, generated: true}}
	require.True(t, makeViewColumnNames(columns))
	assert.Equal(t, long, columns[0].name)
	assert.Equal(t, (syntheticColumnName + long)[:maxColumnNameLength], columns[1].name)
}
