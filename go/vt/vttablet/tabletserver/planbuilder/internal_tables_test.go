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
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const internalGCTable = "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_"

const internalTableErr = "modification of internal table '" + internalGCTable + "' is not allowed"

func internalTablesTestSchema() map[string]*schema.Table {
	newTable := func(name string, columns ...string) *schema.Table {
		fields := make([]*querypb.Field, 0, len(columns))
		for _, column := range columns {
			fields = append(fields, &querypb.Field{Name: column})
		}
		return &schema.Table{Name: sqlparser.NewIdentifierCS(name), Fields: fields}
	}
	return map[string]*schema.Table{
		"t1":            newTable("t1", "id", "col1"),
		"t2":            newTable("t2", "id", "col2"),
		internalGCTable: newTable(internalGCTable, "id", "col3"),
	}
}

func internalTableWriteCases() []struct {
	name    string
	query   string
	wantErr string
} {
	return []struct {
		name    string
		query   string
		wantErr string
	}{{
		name:    "insert into internal table is blocked",
		query:   "insert into " + internalGCTable + " (id) values (1)",
		wantErr: internalTableErr,
	}, {
		name:    "insert select into internal table is blocked",
		query:   "insert into " + internalGCTable + " (id) select id from t1",
		wantErr: internalTableErr,
	}, {
		name:    "replace into internal table is blocked",
		query:   "replace into " + internalGCTable + " (id) values (1)",
		wantErr: internalTableErr,
	}, {
		name:    "single-table update of internal table is blocked",
		query:   "update " + internalGCTable + " set col3 = 1",
		wantErr: internalTableErr,
	}, {
		name:    "update with SET qualified by internal table alias is blocked",
		query:   "update t1 join " + internalGCTable + " gc on t1.id = gc.id set gc.col3 = 1",
		wantErr: internalTableErr,
	}, {
		name:    "update with unqualified SET column owned by internal table is blocked",
		query:   "update t1 join " + internalGCTable + " gc on t1.id = gc.id set col3 = 1",
		wantErr: internalTableErr,
	}, {
		name:    "single-table delete from internal table is blocked",
		query:   "delete from " + internalGCTable + " where id = 1",
		wantErr: internalTableErr,
	}, {
		name:    "multi-table delete targeting internal table is blocked",
		query:   "delete gc from t1 join " + internalGCTable + " gc on t1.id = gc.id",
		wantErr: internalTableErr,
	}, {
		name:    "mixed-case internal table delete is blocked",
		query:   "delete from _VT_HLD_6ACE8BCEF73211EA87E9F875A4D24E90_20200915120410_",
		wantErr: "modification of internal table '_VT_HLD_6ACE8BCEF73211EA87E9F875A4D24E90_20200915120410_' is not allowed",
	}, {
		name:  "select from internal table is allowed",
		query: "select * from " + internalGCTable,
	}, {
		name:  "insert select reading internal table is allowed",
		query: "insert into t1 (id) select id from " + internalGCTable,
	}, {
		name:  "update with SET qualified by regular table alias is allowed",
		query: "update t1 join " + internalGCTable + " gc on t1.id = gc.id set t1.col1 = 1",
	}, {
		name:  "update with unqualified SET column owned by regular table is allowed",
		query: "update t1 join " + internalGCTable + " gc on t1.id = gc.id set col1 = 1",
	}, {
		name:  "update with ambiguous unqualified SET column is passed through",
		query: "update t1 join " + internalGCTable + " gc on t1.id = gc.id set id = 1",
	}, {
		name:  "update with unknown unqualified SET column is passed through",
		query: "update t1 join " + internalGCTable + " gc on t1.id = gc.id set colx = 1",
	}, {
		name:  "update with SET column owned by table missing from schema is allowed",
		query: "update t3 join " + internalGCTable + " gc on t3.id = gc.id set colz = 1",
	}, {
		name:  "multi-table delete joining internal table is allowed",
		query: "delete t1 from t1 join " + internalGCTable + " gc on t1.id = gc.id",
	}, {
		name:  "delete with internal table in subquery is allowed",
		query: "delete from t1 where id in (select id from " + internalGCTable + ")",
	}, {
		name:  "update with internal table in subquery is allowed",
		query: "update t1 set col1 = (select count(*) from " + internalGCTable + ")",
	}, {
		name:    "create internal table is blocked",
		query:   "create table " + internalGCTable + " (id bigint primary key)",
		wantErr: internalTableErr,
	}, {
		name:    "create mixed-case internal table is blocked",
		query:   "create table _VT_HLD_6ACE8BCEF73211EA87E9F875A4D24E90_20200915120410_ (id bigint primary key)",
		wantErr: "modification of internal table '_VT_HLD_6ACE8BCEF73211EA87E9F875A4D24E90_20200915120410_' is not allowed",
	}, {
		name:    "alter internal table is blocked",
		query:   "alter table " + internalGCTable + " add column foo int",
		wantErr: internalTableErr,
	}, {
		name:    "alter rename to internal table is blocked",
		query:   "alter table t1 rename " + internalGCTable,
		wantErr: internalTableErr,
	}, {
		name:    "drop internal table is blocked",
		query:   "drop table " + internalGCTable,
		wantErr: internalTableErr,
	}, {
		name:    "rename internal table is blocked",
		query:   "rename table " + internalGCTable + " to foo",
		wantErr: internalTableErr,
	}, {
		name:    "rename to internal table is blocked",
		query:   "rename table foo to " + internalGCTable,
		wantErr: internalTableErr,
	}, {
		name:    "truncate internal table is blocked",
		query:   "truncate table " + internalGCTable,
		wantErr: internalTableErr,
	}, {
		name:    "exchange partition with internal table is blocked",
		query:   "alter table t1 exchange partition p0 with table " + internalGCTable,
		wantErr: internalTableErr,
	}, {
		name:  "exchange partition with regular table is allowed",
		query: "alter table t1 exchange partition p0 with table t2",
	}, {
		name:  "alter regular table is allowed",
		query: "alter table t1 add column foo int",
	}, {
		name:    "procedure body inserting into internal table is blocked",
		query:   "create procedure p () begin insert into " + internalGCTable + " (id) values (1); end",
		wantErr: internalTableErr,
	}, {
		name:    "procedure body updating internal table is blocked",
		query:   "create procedure p () begin update " + internalGCTable + " set col3 = 1; end",
		wantErr: internalTableErr,
	}, {
		name:    "procedure body deleting from internal table is blocked",
		query:   "create procedure p () begin delete from " + internalGCTable + "; end",
		wantErr: internalTableErr,
	}, {
		name:    "procedure body dropping internal table is blocked",
		query:   "create procedure p () begin drop table " + internalGCTable + "; end",
		wantErr: internalTableErr,
	}, {
		name:  "procedure body reading internal table is allowed",
		query: "create procedure p () begin select * from " + internalGCTable + "; end",
	}, {
		name:  "procedure body writing regular table is allowed",
		query: "create procedure p () begin insert into t1 (id) values (1); end",
	}, {
		name:    "procedure body PREPARE literal modifying internal table is blocked",
		query:   "create procedure p () begin prepare stmt from 'delete from " + internalGCTable + "'; end",
		wantErr: internalTableErr,
	}, {
		name:  "procedure body PREPARE literal on regular table is allowed",
		query: "create procedure p () begin prepare stmt from 'delete from t1'; end",
	}, {
		name:  "procedure body dynamic PREPARE is passed through",
		query: "create procedure p () begin prepare stmt from @q; end",
	}, {
		name:  "create procedure with internal-shaped name is allowed",
		query: "create procedure " + internalGCTable + " () begin select 1; end",
	}, {
		name:  "drop procedure with internal-shaped name is allowed",
		query: "drop procedure " + internalGCTable,
	}}
}

func TestBuildRejectsInternalTableWrites(t *testing.T) {
	env := vtenv.NewTestEnv()
	parser := sqlparser.NewTestParser()
	tables := internalTablesTestSchema()

	for _, tc := range internalTableWriteCases() {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err)

			_, err = Build(env, stmt, tables, "dbName", false)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestBuildStreamingRejectsInternalTableWrites(t *testing.T) {
	parser := sqlparser.NewTestParser()
	tables := internalTablesTestSchema()

	cases := []struct {
		name    string
		query   string
		wantErr string
	}{{
		name:    "streaming update of internal table is blocked",
		query:   "update " + internalGCTable + " set col3 = 1",
		wantErr: internalTableErr,
	}, {
		name:    "streaming delete from internal table is blocked",
		query:   "delete from " + internalGCTable,
		wantErr: internalTableErr,
	}, {
		name:    "streaming insert into internal table is blocked",
		query:   "insert into " + internalGCTable + " (id) values (1)",
		wantErr: internalTableErr,
	}, {
		name:  "streaming select from internal table is allowed",
		query: "select * from " + internalGCTable,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err)

			_, err = BuildStreaming(stmt, tables)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
