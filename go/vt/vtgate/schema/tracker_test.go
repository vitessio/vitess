/*
Copyright 2021 The Vitess Authors.

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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

func TestTracking(t *testing.T) {
	target := &querypb.Target{
		Keyspace:   "ks",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_MASTER,
		Cell:       "aa",
	}
	tablet := &topodatapb.Tablet{}
	sbc := sandboxconn.NewSandboxConn(tablet)
	ch := make(chan *discovery.TabletHealth)
	tracker := NewTracker(ch)
	waiter := &testWaiter{}
	tracker.StartWithWaiter(waiter)
	defer tracker.Stop()
	testcases := []struct {
		tName  string
		result *sqltypes.Result
		updTbl []string
		exp    map[string][]vindexes.Column
	}{
		{
			tName: "new tables",
			result: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|col_name|col_type", "varchar|varchar|varchar"),
				"t1|id|int",
				"t1|name|varchar",
				"t2|id|varchar",
			),
			updTbl: []string{"t1", "t2"},
			exp: map[string][]vindexes.Column{
				"t1": {
					{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_INT32},
					{Name: sqlparser.NewColIdent("name"), Type: querypb.Type_VARCHAR},
				},
				"t2": {
					{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_VARCHAR},
				},
			},
		},
		{
			tName: "delete t1, updated t2 and new t3",
			result: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|col_name|col_type", "varchar|varchar|varchar"),
				"t2|id|varchar",
				"t2|name|varchar",
				"t3|id|datetime",
			),
			updTbl: []string{"t1", "t2", "t3"},
			exp: map[string][]vindexes.Column{
				"t2": {
					{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_VARCHAR},
					{Name: sqlparser.NewColIdent("name"), Type: querypb.Type_VARCHAR},
				},
				"t3": {
					{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_DATETIME},
				},
			},
		},
		{
			tName: "new t4",
			result: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|col_name|col_type", "varchar|varchar|varchar"),
				"t4|name|varchar",
			),
			updTbl: []string{"t4"},
			exp: map[string][]vindexes.Column{
				"t2": {
					{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_VARCHAR},
					{Name: sqlparser.NewColIdent("name"), Type: querypb.Type_VARCHAR},
				},
				"t3": {
					{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_DATETIME},
				},
				"t4": {
					{Name: sqlparser.NewColIdent("name"), Type: querypb.Type_VARCHAR},
				},
			},
		},
	}
	for _, tcase := range testcases {
		t.Run(tcase.tName, func(t *testing.T) {
			sbc.SetResults([]*sqltypes.Result{tcase.result})
			waiter.reset()
			ch <- &discovery.TabletHealth{
				Conn:          sbc,
				Tablet:        tablet,
				Target:        target,
				Serving:       true,
				TablesUpdated: tcase.updTbl,
			}
			waiter.wait()
			assert.Contains(t, sbc.StringQueries(), "le query")
			for k, v := range tcase.exp {
				utils.MustMatch(t, v, tracker.GetColumns("ks", k), "mismatch for table: ", k)
			}
		})
	}
}

// this struct helps us test without time.Sleep
type testWaiter struct {
	wg sync2.AtomicBool
}

func (i *testWaiter) done() {
	i.wg.Set(true)
}
func (i *testWaiter) reset() {
	i.wg.Set(false)
}
func (i *testWaiter) wait() {
	for !i.wg.Get() {
		// busy loop until done
	}
}
