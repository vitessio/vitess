/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    `http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schema

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
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
	tablet := &topodatapb.Tablet{
		Keyspace: target.Keyspace,
		Shard:    target.Shard,
		Type:     target.TabletType,
	}
	fields := sqltypes.MakeTestFields("table_name|col_name|col_type", "varchar|varchar|varchar")

	type delta struct {
		result *sqltypes.Result
		updTbl []string
	}
	var (
		d0 = delta{
			result: sqltypes.MakeTestResult(
				fields,
				"prior|id|int",
			),
			updTbl: []string{"prior"},
		}

		d1 = delta{
			result: sqltypes.MakeTestResult(
				fields,
				"t1|id|int",
				"t1|name|varchar",
				"t2|id|varchar",
			),
			updTbl: []string{"t1", "t2"},
		}

		d2 = delta{
			result: sqltypes.MakeTestResult(
				fields,
				"t2|id|varchar",
				"t2|name|varchar",
				"t3|id|datetime",
			),
			updTbl: []string{"prior", "t1", "t2", "t3"},
		}

		d3 = delta{
			result: sqltypes.MakeTestResult(
				fields,
				"t4|name|varchar",
			),
			updTbl: []string{"t4"},
		}
	)

	testcases := []struct {
		tName  string
		deltas []delta
		exp    map[string][]vindexes.Column
	}{{
		tName:  "new tables",
		deltas: []delta{d0, d1},
		exp: map[string][]vindexes.Column{
			"t1": {
				{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_INT32},
				{Name: sqlparser.NewColIdent("name"), Type: querypb.Type_VARCHAR}},
			"t2": {
				{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_VARCHAR}},
			"prior": {
				{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_INT32}},
		},
	}, {
		tName:  "delete t1 and prior, updated t2 and new t3",
		deltas: []delta{d0, d1, d2},
		exp: map[string][]vindexes.Column{
			"t2": {
				{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_VARCHAR},
				{Name: sqlparser.NewColIdent("name"), Type: querypb.Type_VARCHAR}},
			"t3": {
				{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_DATETIME}},
		},
	}, {
		tName:  "new t4",
		deltas: []delta{d0, d1, d2, d3},
		exp: map[string][]vindexes.Column{
			"t2": {
				{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_VARCHAR},
				{Name: sqlparser.NewColIdent("name"), Type: querypb.Type_VARCHAR}},
			"t3": {
				{Name: sqlparser.NewColIdent("id"), Type: querypb.Type_DATETIME}},
			"t4": {
				{Name: sqlparser.NewColIdent("name"), Type: querypb.Type_VARCHAR}},
		},
	},
	}
	for i, tcase := range testcases {
		t.Run(fmt.Sprintf("%d - %s", i, tcase.tName), func(t *testing.T) {
			sbc := sandboxconn.NewSandboxConn(tablet)
			ch := make(chan *discovery.TabletHealth)
			tracker := NewTracker(ch)
			tracker.consumeDelay = 1 * time.Millisecond
			tracker.Start()
			defer tracker.Stop()

			results := []*sqltypes.Result{{}}
			for _, d := range tcase.deltas {
				for _, deltaRow := range d.result.Rows {
					same := false
					for _, row := range results[0].Rows {
						if row[0].String() == deltaRow[0].String() && row[1].String() == deltaRow[1].String() {
							same = true
							break
						}
					}
					if same == false {
						results[0].Rows = append(results[0].Rows, deltaRow)
					}
				}
			}

			sbc.SetResults(results)
			sbc.Queries = nil

			wg := sync.WaitGroup{}
			wg.Add(1)
			tracker.RegisterSignalReceiver(func() {
				wg.Done()
			})

			for _, d := range tcase.deltas {
				ch <- &discovery.TabletHealth{
					Conn:          sbc,
					Tablet:        tablet,
					Target:        target,
					Serving:       true,
					TablesUpdated: d.updTbl,
				}
			}

			require.False(t, waitTimeout(&wg, time.Second), "schema was updated but received no signal")

			require.Equal(t, 1, len(sbc.StringQueries()))

			_, keyspacePresent := tracker.tracked[target.Keyspace]
			require.Equal(t, true, keyspacePresent)

			for k, v := range tcase.exp {
				utils.MustMatch(t, v, tracker.GetColumns("ks", k), "mismatch for table: ", k)
			}
		})
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func TestTrackerGetKeyspaceUpdateController(t *testing.T) {
	ks3 := &updateController{}
	tracker := Tracker{
		tracked: map[keyspace]*updateController{
			"ks3": ks3,
		},
	}

	th1 := &discovery.TabletHealth{
		Target: &querypb.Target{Keyspace: "ks1"},
	}
	ks1 := tracker.getKeyspaceUpdateController(th1)

	th2 := &discovery.TabletHealth{
		Target: &querypb.Target{Keyspace: "ks2"},
	}
	ks2 := tracker.getKeyspaceUpdateController(th2)

	th3 := &discovery.TabletHealth{
		Target: &querypb.Target{Keyspace: "ks3"},
	}

	assert.NotEqual(t, ks1, ks2, "ks1 and ks2 should not be equal, belongs to different keyspace")
	assert.Equal(t, ks1, tracker.getKeyspaceUpdateController(th1), "received different updateController")
	assert.Equal(t, ks2, tracker.getKeyspaceUpdateController(th2), "received different updateController")
	assert.Equal(t, ks3, tracker.getKeyspaceUpdateController(th3), "received different updateController")

	assert.NotNil(t, ks1.init, "ks1 needs to be initialized")
	assert.NotNil(t, ks2.init, "ks2 needs to be initialized")
	assert.Nil(t, ks3.init, "ks3 already initialized")
}
