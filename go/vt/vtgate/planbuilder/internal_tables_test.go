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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TestRejectInternalTableDDLAllowsInternalShapedProcedureNames verifies that
// procedure object names are not treated as table names by the internal table
// guard.
func TestRejectInternalTableDDLAllowsInternalShapedProcedureNames(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "create procedure",
			query: "create procedure _vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_() begin declare y int; set y = 1; end",
		},
		{
			name:  "drop procedure",
			query: "drop procedure _vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.query)
			require.NoError(t, err)

			ddl, ok := stmt.(sqlparser.DDLStatement)
			require.True(t, ok)

			require.NoError(t, rejectInternalTableDDL(ddl, tt.query, parser))
		})
	}
}

// TestRejectInternalTableDDLRejectsUnsafeProcedurePrepare verifies that prepared
// statements cannot hide internal-table modifications inside a procedure body.
func TestRejectInternalTableDDLRejectsUnsafeProcedurePrepare(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name string

		query string

		wantErr string
	}{
		{
			name: "literal delete",
			query: `
create procedure p()
begin
	prepare stmt from 'delete from _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_';
	execute stmt;
end`,
			wantErr: "VT09033: modification of internal table",
		},
		{
			name: "literal drop",
			query: `
create procedure p()
begin
	prepare stmt from 'drop table _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_';
	execute stmt;
end`,
			wantErr: "VT09033: modification of internal table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectInternalTableProcedureQuery(t, tt.query, parser)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestRejectInternalTableDDLAllowsSafeProcedurePrepare verifies that prepared
// statements remain available when their SQL is inspectable and safe, and that
// dynamic PREPARE, whose SQL is not inspectable, is passed through untouched.
func TestRejectInternalTableDDLAllowsSafeProcedurePrepare(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name  string
		query string
	}{
		{
			name: "literal select",
			query: `
create procedure p()
begin
	prepare stmt from 'select 1';
	execute stmt;
end`,
		},
		{
			name: "dynamic variable",
			query: `
create procedure p()
begin
	set @sql = concat('select * from t where id = ', 1);
	prepare stmt from @sql;
	execute stmt;
end`,
		},
		{
			name: "literal outside the vitess grammar",
			query: `
create procedure p()
begin
	prepare stmt from 'handler t open';
	execute stmt;
end`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, rejectInternalTableProcedureQuery(t, tt.query, parser))
		})
	}
}

// TestRejectInternalTableLoad verifies that LOAD DATA is blocked for every
// identifier quoting form that can name an internal operation table.
func TestRejectInternalTableLoad(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name string

		query string

		wantErr string
	}{
		{
			name:    "bare identifier",
			query:   "load data infile 'x.txt' into table _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			wantErr: "VT09033: modification of internal table",
		},
		{
			name:    "backtick quoted",
			query:   "load data infile 'x.txt' into table `_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_`",
			wantErr: "VT09033: modification of internal table",
		},
		{
			name:    "ansi quotes double quoted",
			query:   `load data infile 'x.txt' into table "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_"`,
			wantErr: "VT09033: modification of internal table",
		},
		{
			name:    "ansi quotes double quoted qualified",
			query:   `load data infile 'x.txt' into table "main"."_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_"`,
			wantErr: "VT09033: modification of internal table",
		},
		{
			name:  "regular table",
			query: "load data infile 'x.txt' into table t",
		},
		{
			name:  "regular table double quoted",
			query: `load data infile 'x.txt' into table "t"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectInternalTableLoad(tt.query, parser)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// rejectInternalTableProcedureQuery parses query as a DDL statement and runs it
// through the internal-table guard.
func rejectInternalTableProcedureQuery(t *testing.T, query string, parser *sqlparser.Parser) error {
	t.Helper()

	stmt, err := parser.Parse(query)
	require.NoError(t, err)

	ddl, ok := stmt.(sqlparser.DDLStatement)
	require.True(t, ok)

	return rejectInternalTableDDL(ddl, query, parser)
}
