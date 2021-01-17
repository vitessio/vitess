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

package worker

import (
	"errors"
	"flag"
	"reflect"
	"strings"
	"testing"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestGreaterThanTupleWhereClause(t *testing.T) {
	testcases := []struct {
		columns []string
		row     []sqltypes.Value
		want    []string
	}{
		{
			columns: []string{"a"},
			row:     []sqltypes.Value{sqltypes.NewInt64(1)},
			want:    []string{"`a`>1"},
		},
		{
			columns: []string{"a", "b"},
			row: []sqltypes.Value{
				sqltypes.NewInt64(1),
				sqltypes.TestValue(sqltypes.Float32, "2.1"),
			},
			want: []string{"`a`>=1", "(`a`,`b`)>(1,2.1)"},
		},
		{
			columns: []string{"a", "b", "c"},
			row: []sqltypes.Value{
				sqltypes.NewInt64(1),
				sqltypes.TestValue(sqltypes.Float32, "2.1"),
				sqltypes.NewVarChar("Bär"),
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
			start:             sqltypes.NewInt64(11),
			end:               sqltypes.NewInt64(26),
			table:             "t1",
			columns:           []string{"a", "msg1", "msg2"},
			primaryKeyColumns: []string{"a"},
			want:              "SELECT `a`,`msg1`,`msg2` FROM `t1` WHERE `a`>=11 AND `a`<26 ORDER BY `a`",
		},
		{
			desc:              "only end defined",
			end:               sqltypes.NewInt64(26),
			table:             "t1",
			columns:           []string{"a", "msg1", "msg2"},
			primaryKeyColumns: []string{"a"},
			want:              "SELECT `a`,`msg1`,`msg2` FROM `t1` WHERE `a`<26 ORDER BY `a`",
		},
		{
			desc:              "only start defined",
			start:             sqltypes.NewInt64(11),
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
			start:             sqltypes.NewInt64(11),
			end:               sqltypes.NewInt64(26),
			table:             "t1",
			columns:           []string{"a", "b", "msg1", "msg2"},
			primaryKeyColumns: []string{"a", "b"},
			want:              "SELECT `a`,`b`,`msg1`,`msg2` FROM `t1` WHERE `a`>=11 AND `a`<26 ORDER BY `a`,`b`",
		},
		{
			desc:              "start overridden by last row (multi-column primary key)",
			start:             sqltypes.NewInt64(11),
			end:               sqltypes.NewInt64(26),
			table:             "t1",
			columns:           []string{"a", "b", "msg1", "msg2"},
			primaryKeyColumns: []string{"a", "b"},
			lastRow: []sqltypes.Value{
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(2),
			},
			want: "SELECT `a`,`b`,`msg1`,`msg2` FROM `t1` WHERE `a`>=1 AND (`a`,`b`)>(1,2) AND `a`<26 ORDER BY `a`,`b`",
		},
		{
			desc:              "no start or end defined but last row (multi-column primary key)",
			table:             "t1",
			columns:           []string{"a", "b", "msg1", "msg2"},
			primaryKeyColumns: []string{"a", "b"},
			lastRow: []sqltypes.Value{
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(2),
			},
			want: "SELECT `a`,`b`,`msg1`,`msg2` FROM `t1` WHERE `a`>=1 AND (`a`,`b`)>(1,2) ORDER BY `a`,`b`",
		},
	}

	for _, tc := range testcases {
		r := RestartableResultReader{
			chunk: chunk{tc.start, tc.end, 1, 1},
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

// TestNewRestartableResultReader tests the correct error handling e.g.
// if the connection to a tablet fails due to a canceled context.
func TestNewRestartableResultReader(t *testing.T) {
	wantErr := errors.New("restartable_result_reader_test.go: context canceled")

	tabletconn.RegisterDialer("fake_dialer", func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		return nil, wantErr
	})
	protocol := flag.CommandLine.Lookup("tablet_protocol").Value.String()
	flag.Set("tablet_protocol", "fake_dialer")
	// Restore the previous flag value after the test.
	defer flag.Set("tablet_protocol", protocol)

	// Create dependencies e.g. a "singleTabletProvider" instance.
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	alias := &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  1,
	}
	tablet := &topodatapb.Tablet{
		Keyspace: "ks1",
		Shard:    "-80",
		Alias:    alias,
	}
	ctx := context.Background()
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}
	tp := newSingleTabletProvider(ctx, ts, alias)

	_, err := NewRestartableResultReader(ctx, wr.Logger(), tp, nil /* td */, chunk{}, false)
	if err == nil || !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("NewRestartableResultReader() should have failed because the context is canceled: %v", err)
	}
}
