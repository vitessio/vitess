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

package vexplain

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Mixed-version tests can emit different SQL escaping for the same literal bytes.
// Compare ASTs directly so older formatters cannot replace invalid UTF-8.
func vexplainRowsEqualsStr(wantStr string, got []sqltypes.Row) error {
	want, err := sqltypes.ParseRows(wantStr)
	if err != nil {
		return err
	}
	if len(want) != len(got) {
		return fmt.Errorf("expected %d VEXPLAIN rows, got %d", len(want), len(got))
	}

	parser := sqlparser.NewTestParser()
	matched := make([]bool, len(got))
	for _, wantRow := range want {
		if len(wantRow) != 3 {
			return fmt.Errorf("expected keyspace, shard and query in VEXPLAIN row: %v", wantRow)
		}
		wantStmt, err := parser.Parse(wantRow[2].ToString())
		if err != nil {
			return err
		}
		found := false
		for i, gotRow := range got {
			if matched[i] || len(gotRow) != 3 || !sqltypes.RowEqual(wantRow[:2], gotRow[:2]) || wantRow[2].Type() != gotRow[2].Type() {
				continue
			}
			gotStmt, err := parser.Parse(gotRow[2].ToString())
			if err != nil {
				return err
			}
			if sqlparser.Equals.Statement(wantStmt, gotStmt) {
				matched[i] = true
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("VEXPLAIN row %v is missing from result %v", wantRow, got)
		}
	}
	return nil
}

func TestVExplainQueryComparison(t *testing.T) {
	oldQuery := "insert into lookup(keyspace_id) values (_binary'\xd2\xfd\x88g\xd5\\r-\xfe')"
	newQuery := "insert into lookup(keyspace_id) values (_binary'\xd2\xfd\x88g\\\xd5\\r-\xfe')"
	changedQuery := "insert into lookup(keyspace_id) values (_binary'\xd2\xfd\x88g\\\xd4\\r-\xfe')"
	row := func(keyspace, shard, query string) sqltypes.Row {
		return sqltypes.Row{sqltypes.NewVarChar(keyspace), sqltypes.NewVarChar(shard), sqltypes.NewVarChar(query)}
	}
	oldRow := row("ks", "-40", oldQuery)
	newRow := row("ks", "-40", newQuery)
	beginRow := row("ks", "-40", "begin")

	for _, tc := range []struct {
		name    string
		want    []sqltypes.Row
		got     []sqltypes.Row
		wantErr bool
	}{
		{name: "old escaping", want: []sqltypes.Row{oldRow}, got: []sqltypes.Row{oldRow}},
		{name: "new escaping", want: []sqltypes.Row{oldRow}, got: []sqltypes.Row{newRow}},
		{name: "old escaping against new expectation", want: []sqltypes.Row{newRow}, got: []sqltypes.Row{oldRow}},
		{name: "unordered duplicate rows", want: []sqltypes.Row{oldRow, beginRow, oldRow}, got: []sqltypes.Row{newRow, newRow, beginRow}},
		{name: "changed literal byte", want: []sqltypes.Row{oldRow}, got: []sqltypes.Row{row("ks", "-40", changedQuery)}, wantErr: true},
		{name: "changed introducer", want: []sqltypes.Row{oldRow}, got: []sqltypes.Row{row("ks", "-40", strings.Replace(newQuery, "_binary", "_latin1", 1))}, wantErr: true},
		{name: "changed keyspace", want: []sqltypes.Row{oldRow}, got: []sqltypes.Row{row("other", "-40", newQuery)}, wantErr: true},
		{name: "changed shard", want: []sqltypes.Row{oldRow}, got: []sqltypes.Row{row("ks", "40-80", newQuery)}, wantErr: true},
		{name: "changed statement", want: []sqltypes.Row{beginRow}, got: []sqltypes.Row{row("ks", "-40", "commit")}, wantErr: true},
		{name: "missing row", want: []sqltypes.Row{oldRow, beginRow}, got: []sqltypes.Row{newRow}, wantErr: true},
		{name: "changed duplicate count", want: []sqltypes.Row{oldRow, beginRow, oldRow}, got: []sqltypes.Row{newRow, beginRow, beginRow}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := vexplainRowsEqualsStr(fmt.Sprint(tc.want), tc.got)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
