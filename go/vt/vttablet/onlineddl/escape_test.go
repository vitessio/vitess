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

package onlineddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// maliciousTableName is a table identifier that closes its own back quotes and
// appends statements. A tenant supplies it by doubling the back quotes in the
// DDL it submits; the parser decodes them, so the value reaching the executor
// holds single back quotes.
const maliciousTableName = "t1`;INSERT INTO mydb.loot SELECT * FROM mysql.user;show create table `t1"

// maliciousQuotedName closes a string literal rather than a back quoted
// identifier. sqlShowTablesLike puts the name in a LIKE pattern, so that sink
// needs a single quote to break out, not a back quote.
const maliciousQuotedName = "t1';INSERT INTO mydb.loot SELECT * FROM mysql.user;SHOW TABLES LIKE 't1"

// TestGeneratedDDLIsOneStatement guards against a table name adding statements
// to the DDL the executor generates.
//
// The generated text is executed as-is, without a re-parse, and some of it runs
// on the DBA connection. Those connections negotiate CLIENT_MULTI_STATEMENTS and
// ExecuteFetch runs the whole batch before reporting an error, so an embedded
// ';' executes. Every identifier must therefore be escaped before the statement
// text is produced.
func TestGeneratedDDLIsOneStatement(t *testing.T) {
	parser := sqlparser.NewTestParser()

	testcases := []struct {
		name  string
		build func() string
	}{
		{
			name: "show create table",
			build: func() string {
				return buildIdentifierQuery(sqlShowCreateTable, maliciousTableName)
			},
		},
		{
			name: "rename table",
			build: func() string {
				return buildIdentifierQuery(sqlRenameTable, maliciousTableName, "t2")
			},
		},
		{
			name: "rename table, malicious target",
			build: func() string {
				return buildIdentifierQuery(sqlRenameTable, "t1", maliciousTableName)
			},
		},
		{
			name: "lock two tables write",
			build: func() string {
				return buildIdentifierQuery(sqlLockTwoTablesWrite, maliciousTableName, "t2")
			},
		},
		{
			name: "swap tables",
			build: func() string {
				return buildIdentifierQuery(sqlSwapTables,
					maliciousTableName, "t2", "t3",
					maliciousTableName, "t2", "t3",
				)
			},
		},
		{
			name: "drop table",
			build: func() string {
				return buildIdentifierQuery(sqlDropTable, maliciousTableName)
			},
		},
		{
			name: "drop table if exists",
			build: func() string {
				return buildIdentifierQuery(sqlDropTableIfExists, maliciousTableName)
			},
		},
		{
			name: "analyze table",
			build: func() string {
				return buildIdentifierQuery(sqlAnalyzeTable, maliciousTableName)
			},
		},
		{
			name: "analyze table local",
			build: func() string {
				return buildIdentifierQuery(sqlAnalyzeTableLocal, maliciousTableName)
			},
		},
		{
			name: "create sentry table",
			build: func() string {
				return buildIdentifierQuery(sqlCreateSentryTable, maliciousTableName)
			},
		},
		{
			name: "show tables like",
			build: func() string {
				return buildTableExistsQuery(maliciousQuotedName)
			},
		},
		{
			name: "show tables like, back quoted name",
			build: func() string {
				return buildTableExistsQuery(maliciousTableName)
			},
		},
		{
			name: "show table status like",
			build: func() string {
				return buildLiteralQuery(sqlShowTableStatus, maliciousQuotedName)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			query := tc.build()

			pieces, err := parser.SplitStatementToPieces(query)
			require.NoError(t, err)
			assert.Len(t, pieces, 1, "generated %q, which is %d statements", query, len(pieces))
		})
	}
}

// TestBuildTableExistsQuery pins the pattern the existence check sends: the '_'
// wildcard is escaped so that the pattern matches only the table asked about.
func TestBuildTableExistsQuery(t *testing.T) {
	assert.Equal(t,
		`SHOW TABLES LIKE '\_vt\_HOLD\_6ace8bcef73211ea87e9f875a4d24e90\_20200915120410'`,
		buildTableExistsQuery("_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410"))
}

// TestEscapeNameProducesOneIdentifier covers the same defect in the escaper used
// to build the vreplication filter query. escapeName wraps a column or table
// name in back quotes, and that name comes from the tenant's schema. MySQL
// allows a back quote inside a column name, so the name has to be escaped, not
// just wrapped, or it closes its own quoting.
func TestEscapeNameProducesOneIdentifier(t *testing.T) {
	parser := sqlparser.NewTestParser()

	for _, name := range []string{
		"col1",
		"my_col",
		"Weird Name",
		"a`b",                           // a back quote closes the wrapping
		"`col1`",                        // a name that is itself back quoted
		"a` , (select 1) as x from t #", // a back quote plus more expression
	} {
		t.Run(name, func(t *testing.T) {
			// escapeName is used to build a select list and a table name, so the
			// result has to be exactly one identifier in that position.
			query := "select " + escapeName(name) + " from t"

			stmt, err := parser.Parse(query)
			require.NoError(t, err, "escapeName(%q) produced %q", name, escapeName(name))
			sel, ok := stmt.(*sqlparser.Select)
			require.True(t, ok, "%q parsed as %T", query, stmt)
			require.Len(t, sel.SelectExprs.Exprs, 1, "escapeName(%q) yielded %q", name, escapeName(name))

			aliased, ok := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
			require.True(t, ok)
			col, ok := aliased.Expr.(*sqlparser.ColName)
			require.True(t, ok, "escapeName(%q) is not a column reference: %q", name, escapeName(name))
			assert.Equal(t, name, col.Name.String(), "escapeName(%q) changed the name", name)
		})
	}
}

// TestGeneratedDDLNamesRoundTrip pins that escaping preserves the name, so an
// ordinary table is still addressed by its own name.
func TestGeneratedDDLNamesRoundTrip(t *testing.T) {
	parser := sqlparser.NewTestParser()

	for _, name := range []string{"t1", "my_table", "Weird Name", maliciousTableName} {
		t.Run(name, func(t *testing.T) {
			query := buildIdentifierQuery(sqlRenameTable, name, "target")

			stmt, err := parser.Parse(query)
			require.NoError(t, err, "generated %q", query)
			rename, ok := stmt.(*sqlparser.RenameTable)
			require.True(t, ok, "generated %q, parsed as %T", query, stmt)
			require.Len(t, rename.TablePairs, 1)

			assert.Equal(t, name, rename.TablePairs[0].FromTable.Name.String())
			assert.Equal(t, "target", rename.TablePairs[0].ToTable.Name.String())
		})
	}
}
