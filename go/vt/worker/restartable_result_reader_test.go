// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

func TestGreaterThanTupleWhereClause(t *testing.T) {
	testcases := []struct {
		columns []string
		row     []sqltypes.Value
		want    []string
	}{
		{
			columns: []string{"a"},
			row:     []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Int64, []byte("1"))},
			want:    []string{"`a`>1"},
		},
		{
			columns: []string{"a", "b"},
			row: []sqltypes.Value{
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Float32, []byte("2.1")),
			},
			want: []string{"`a`>=1", "(`a`,`b`)>(1,2.1)"},
		},
		{
			columns: []string{"a", "b", "c"},
			row: []sqltypes.Value{
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Float32, []byte("2.1")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("Bär")),
			},
			want: []string{"`a`>=1", "(`a`,`b`,`c`)>(1,2.1,'Bär')"},
		},
	}

	for _, tc := range testcases {
		got := greaterThanTupleWhereClause(tc.columns, tc.row)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("greaterThanTupleWhereClause(%v, %v) = %v, want = %v", tc.columns, tc.row, got, tc.want)
		}
	}
}

func TestGenerateQuery(t *testing.T) {
	testcases := []struct {
		desc              string
		start             sqltypes.Value
		end               sqltypes.Value
		table             string
		columns           []string
		primaryKeyColumns []string
		lastRow           []sqltypes.Value
		want              string
	}{
		{
			desc:              "start and end defined",
			start:             sqltypes.MakeTrusted(sqltypes.Int64, []byte("11")),
			end:               sqltypes.MakeTrusted(sqltypes.Int64, []byte("26")),
			table:             "t1",
			columns:           []string{"a", "msg1", "msg2"},
			primaryKeyColumns: []string{"a"},
			want:              "SELECT `a`,`msg1`,`msg2` FROM `t1` WHERE `a`>=11 AND `a`<26 ORDER BY `a`",
		},
		{
			desc:              "only end defined",
			end:               sqltypes.MakeTrusted(sqltypes.Int64, []byte("26")),
			table:             "t1",
			columns:           []string{"a", "msg1", "msg2"},
			primaryKeyColumns: []string{"a"},
			want:              "SELECT `a`,`msg1`,`msg2` FROM `t1` WHERE `a`<26 ORDER BY `a`",
		},
		{
			desc:              "only start defined",
			start:             sqltypes.MakeTrusted(sqltypes.Int64, []byte("11")),
			table:             "t1",
			columns:           []string{"a", "msg1", "msg2"},
			primaryKeyColumns: []string{"a"},
			want:              "SELECT `a`,`msg1`,`msg2` FROM `t1` WHERE `a`>=11 ORDER BY `a`",
		},
		{
			desc:              "neither start nor end defined",
			table:             "t1",
			columns:           []string{"a", "msg1", "msg2"},
			primaryKeyColumns: []string{"a"},
			want:              "SELECT `a`,`msg1`,`msg2` FROM `t1` ORDER BY `a`",
		},
		{
			desc:              "neither start nor end defined and no primary key",
			table:             "t1",
			columns:           []string{"a", "msg1", "msg2"},
			primaryKeyColumns: []string{},
			want:              "SELECT `a`,`msg1`,`msg2` FROM `t1`",
		},
		{
			desc:              "start and end defined (multi-column primary key)",
			start:             sqltypes.MakeTrusted(sqltypes.Int64, []byte("11")),
			end:               sqltypes.MakeTrusted(sqltypes.Int64, []byte("26")),
			table:             "t1",
			columns:           []string{"a", "b", "msg1", "msg2"},
			primaryKeyColumns: []string{"a", "b"},
			want:              "SELECT `a`,`b`,`msg1`,`msg2` FROM `t1` WHERE `a`>=11 AND `a`<26 ORDER BY `a`,`b`",
		},
		{
			desc:              "start overriden by last row (multi-column primary key)",
			start:             sqltypes.MakeTrusted(sqltypes.Int64, []byte("11")),
			end:               sqltypes.MakeTrusted(sqltypes.Int64, []byte("26")),
			table:             "t1",
			columns:           []string{"a", "b", "msg1", "msg2"},
			primaryKeyColumns: []string{"a", "b"},
			lastRow: []sqltypes.Value{
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("2")),
			},
			want: "SELECT `a`,`b`,`msg1`,`msg2` FROM `t1` WHERE `a`>=1 AND (`a`,`b`)>(1,2) AND `a`<26 ORDER BY `a`,`b`",
		},
		{
			desc:              "no start or end defined but last row (multi-column primary key)",
			table:             "t1",
			columns:           []string{"a", "b", "msg1", "msg2"},
			primaryKeyColumns: []string{"a", "b"},
			lastRow: []sqltypes.Value{
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("2")),
			},
			want: "SELECT `a`,`b`,`msg1`,`msg2` FROM `t1` WHERE `a`>=1 AND (`a`,`b`)>(1,2) ORDER BY `a`,`b`",
		},
	}

	for _, tc := range testcases {
		r := RestartableResultReader{
			chunk: chunk{tc.start, tc.end},
			td: &tabletmanagerdatapb.TableDefinition{
				Name:              tc.table,
				Columns:           tc.columns,
				PrimaryKeyColumns: tc.primaryKeyColumns,
			},
			lastRow: tc.lastRow,
		}
		r.generateQuery()
		got := r.query
		if got != tc.want {
			t.Errorf("testcase = %v: generateQuery(chunk=%v, pk=%v, lastRow=%v) = %v, want = %v", tc.desc, r.chunk, r.td.PrimaryKeyColumns, r.lastRow, got, tc.want)
		}
	}
}
