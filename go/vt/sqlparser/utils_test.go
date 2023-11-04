/*
Copyright 2019 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeAlphabetically(t *testing.T) {
	testcases := []struct {
		in  string
		out string
	}{{
		in:  "select * from tbl",
		out: "select * from tbl",
	}, {
		in:  "select * from tbl where a=3",
		out: "select * from tbl where a = 3",
	}, {
		in:  "select * from tbl where a=3 and b=4",
		out: "select * from tbl where a = 3 and b = 4",
	}, {
		in:  "select * from tbl where b=4 and a=3",
		out: "select * from tbl where a = 3 and b = 4",
	}, {
		in:  "select * from tbl where b=4 and c>5 and a=3",
		out: "select * from tbl where a = 3 and b = 4 and c > 5",
	}, {
		in:  "select * from tbl where b=4 or a=3",
		out: "select * from tbl where b = 4 or a = 3",
	}}

	for _, tc := range testcases {
		normalized, err := NormalizeAlphabetically(tc.in)
		assert.NoError(t, err)
		assert.Equal(t, tc.out, normalized)
	}
}

func TestQueryMatchesTemplates(t *testing.T) {
	testcases := []struct {
		name string
		q    string
		tmpl []string
		out  bool
	}{
		{
			name: "trivial, identical",
			q:    "select id from tbl",
			tmpl: []string{
				"select id from tbl",
			},
			out: true,
		}, {
			name: "trivial, canonical",
			q:    "select `id` from tbl",
			tmpl: []string{
				"select id FROM `tbl`",
			},
			out: true,
		}, {
			name: "trivial, identical from list",
			q:    "select id from tbl",
			tmpl: []string{
				"select name from tbl",
				"select id from tbl",
			},
			out: true,
		}, {
			name: "trivial no match",
			q:    "select id from tbl where a=3",
			tmpl: []string{
				"select id from tbl",
			},
			out: false,
		}, {
			name: "int value",
			q:    "select id from tbl where a=3",
			tmpl: []string{
				"select name from tbl where a=17",
				"select id from tbl where a=5",
			},
			out: true,
		}, {
			name: "string value",
			q:    "select id from tbl where a='abc'",
			tmpl: []string{
				"select name from tbl where a='x'",
				"select id from tbl where a='y'",
			},
			out: true,
		}, {
			name: "two params",
			q:    "select id from tbl where a='abc' and b='def'",
			tmpl: []string{
				"select name from tbl where a='x' and b = 'y'",
				"select id from tbl where a='x' and b = 'y'",
			},
			out: true,
		}, {
			name: "no match",
			q:    "select id from tbl where a='abc' and b='def'",
			tmpl: []string{
				"select name from tbl where a='x' and b = 'y'",
				"select id from tbl where a='x' and c = 'y'",
			},
			out: false,
		}, {
			name: "reorder AND params",
			q:    "select id from tbl where a='abc' and b='def'",
			tmpl: []string{
				"select id from tbl where b='x' and a = 'y'",
			},
			out: true,
		}, {
			name: "no reorder OR params",
			q:    "select id from tbl where a='abc' or b='def'",
			tmpl: []string{
				"select id from tbl where b='x' or a = 'y'",
			},
			out: false,
		}, {
			name: "strict reorder OR params",
			q:    "select id from tbl where a='abc' or b='def'",
			tmpl: []string{
				"select id from tbl where a='x' or b = 'y'",
			},
			out: true,
		}, {
			name: "identical 'x' annotation in template, identical query values",
			q:    "select id from tbl where a='abc' or b='abc'",
			tmpl: []string{
				"select id from tbl where a='x' or b = 'x'",
			},
			out: true,
		}, {
			name: "identical 'x' annotation in template, different query values",
			q:    "select id from tbl where a='abc' or b='def'",
			tmpl: []string{
				"select id from tbl where a='x' or b = 'x'",
			},
			out: false,
		}, {
			name: "reorder AND params, range test",
			q:    "select id from tbl where a >'abc' and b<3",
			tmpl: []string{
				"select id from tbl where b<17 and a > 'y'",
			},
			out: true,
		}, {
			name: "canonical, case",
			q:    "SHOW BINARY LOGS",
			tmpl: []string{
				"show binary logs",
			},
			out: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			match, err := QueryMatchesTemplates(tc.q, tc.tmpl)
			assert.NoError(t, err)
			assert.Equal(t, tc.out, match)
		})
	}
}

func TestReplaceTableQualifiers(t *testing.T) {
	origDB := "_vt"
	tests := []struct {
		name    string
		in      string
		newdb   string
		out     string
		wantErr bool
	}{
		{
			name:    "invalid select",
			in:      "select frog bar person",
			out:     "",
			wantErr: true,
		},
		{
			name: "simple select",
			in:   "select * from _vt.foo",
			out:  "select * from foo",
		},
		{
			name:  "simple select with new db",
			in:    "select * from _vt.foo",
			newdb: "_vt_test",
			out:   "select * from _vt_test.foo",
		},
		{
			name:  "simple select with new db same",
			in:    "select * from _vt.foo where id=1", // should be unchanged
			newdb: "_vt",
			out:   "select * from _vt.foo where id=1",
		},
		{
			name:  "simple select with new db needing escaping",
			in:    "select * from _vt.foo",
			newdb: "1_vt-test",
			out:   "select * from `1_vt-test`.foo",
		},
		{
			name: "complex select",
			in:   "select table_name, lastpk from _vt.copy_state where vrepl_id = 1 and id in (select max(id) from _vt.copy_state where vrepl_id = 1 group by vrepl_id, table_name)",
			out:  "select table_name, lastpk from copy_state where vrepl_id = 1 and id in (select max(id) from copy_state where vrepl_id = 1 group by vrepl_id, table_name)",
		},
		{
			name:  "complex mixed exists select",
			in:    "select workflow_name, db_name from _vt.vreplication where id = 1 and exists (select v1 from mydb.foo where fname = 'matt') and not exists (select v2 from _vt.newsidecartable where _vt.newsidecartable.id = _vt.vreplication.workflow_name)",
			newdb: "_vt_import",
			out:   "select workflow_name, db_name from _vt_import.vreplication where id = 1 and exists (select v1 from mydb.foo where fname = 'matt') and not exists (select v2 from _vt_import.newsidecartable where _vt_import.newsidecartable.id = _vt_import.vreplication.workflow_name)",
		},
		{
			name:  "derived table select",
			in:    "select myder.id from (select max(id) as id from _vt.copy_state where vrepl_id = 1 group by vrepl_id, table_name) as myder where id = 1",
			newdb: "__vt-metadata",
			out:   "select myder.id from (select max(id) as id from `__vt-metadata`.copy_state where vrepl_id = 1 group by vrepl_id, table_name) as myder where id = 1",
		},
		{
			name: "complex select",
			in:   "select t1.col1, t2.col2 from _vt.t1 as t1 join _vt.t2 as t2 on t1.id = t2.id",
			out:  "select t1.col1, t2.col2 from t1 as t1 join t2 as t2 on t1.id = t2.id",
		},
		{
			name: "simple insert",
			in:   "insert into _vt.foo(id) values (1)",
			out:  "insert into foo(id) values (1)",
		},
		{
			name: "simple update",
			in:   "update _vt.foo set id = 1",
			out:  "update foo set id = 1",
		},
		{
			name: "simple delete",
			in:   "delete from _vt.foo where id = 1",
			out:  "delete from foo where id = 1",
		},
		{
			name: "simple set",
			in:   "set names 'binary'",
			out:  "set names 'binary'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReplaceTableQualifiers(tt.in, origDB, tt.newdb)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.out, got, "RemoveTableQualifiers(); in: %s, out: %s", tt.in, got)
		})
	}
}
