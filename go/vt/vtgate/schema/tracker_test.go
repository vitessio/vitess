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

	"vitess.io/vitess/go/mysql"

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
		TabletType: topodatapb.TabletType_PRIMARY,
		Cell:       "aa",
	}
	tablet := &topodatapb.Tablet{
		Keyspace: target.Keyspace,
		Shard:    target.Shard,
		Type:     target.TabletType,
	}
	fields := sqltypes.MakeTestFields(
		"table_name|col_name|col_type|collation_name",
		"varchar|varchar|varchar|varchar",
	)

	type delta struct {
		result *sqltypes.Result
		updTbl []string
	}
	var (
		d0 = delta{
			result: sqltypes.MakeTestResult(
				fields,
				"prior|id|int|",
			),
			updTbl: []string{"prior"},
		}

		d1 = delta{
			result: sqltypes.MakeTestResult(
				fields,
				"t1|id|int|",
				"t1|name|varchar|utf8_bin",
				"t2|id|varchar|utf8_bin",
			),
			updTbl: []string{"t1", "t2"},
		}

		d2 = delta{
			result: sqltypes.MakeTestResult(
				fields,
				"t2|id|varchar|utf8_bin",
				"t2|name|varchar|utf8_bin",
				"t3|id|datetime|",
			),
			updTbl: []string{"prior", "t1", "t2", "t3"},
		}

		d3 = delta{
			result: sqltypes.MakeTestResult(
				fields,
				"t4|name|varchar|utf8_bin",
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
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT32},
				{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, CollationName: "utf8_bin"}},
			"t2": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_VARCHAR, CollationName: "utf8_bin"}},
			"prior": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT32}},
		},
	}, {
		tName:  "delete t1 and prior, updated t2 and new t3",
		deltas: []delta{d0, d1, d2},
		exp: map[string][]vindexes.Column{
			"t2": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_VARCHAR, CollationName: "utf8_bin"},
				{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, CollationName: "utf8_bin"}},
			"t3": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_DATETIME}},
		},
	}, {
		tName:  "new t4",
		deltas: []delta{d0, d1, d2, d3},
		exp: map[string][]vindexes.Column{
			"t2": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_VARCHAR, CollationName: "utf8_bin"},
				{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, CollationName: "utf8_bin"}},
			"t3": {
				{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_DATETIME}},
			"t4": {
				{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR, CollationName: "utf8_bin"}},
		},
	},
	}
	for i, tcase := range testcases {
		t.Run(fmt.Sprintf("%d - %s", i, tcase.tName), func(t *testing.T) {
			sbc := sandboxconn.NewSandboxConn(tablet)
			ch := make(chan *discovery.TabletHealth)
			tracker := NewTracker(ch, "", false)
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
					Conn:    sbc,
					Tablet:  tablet,
					Target:  target,
					Serving: true,
					Stats:   &querypb.RealtimeStats{TableSchemaChanged: d.updTbl},
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

func TestTrackingUnHealthyTablet(t *testing.T) {
	target := &querypb.Target{
		Keyspace:   "ks",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_PRIMARY,
		Cell:       "aa",
	}
	tablet := &topodatapb.Tablet{
		Keyspace: target.Keyspace,
		Shard:    target.Shard,
		Type:     target.TabletType,
	}

	sbc := sandboxconn.NewSandboxConn(tablet)
	ch := make(chan *discovery.TabletHealth)
	tracker := NewTracker(ch, "", false)
	tracker.consumeDelay = 1 * time.Millisecond
	tracker.Start()
	defer tracker.Stop()

	// the test are written in a way that it expects 3 signals to be sent from the tracker to the subscriber.
	wg := sync.WaitGroup{}
	wg.Add(3)
	tracker.RegisterSignalReceiver(func() {
		wg.Done()
	})

	tcases := []struct {
		name          string
		serving       bool
		expectedQuery string
		updatedTbls   []string
	}{
		{
			name:    "initial load",
			serving: true,
		},
		{
			name:        "initial load",
			serving:     true,
			updatedTbls: []string{"a"},
		},
		{
			name:    "non serving tablet",
			serving: false,
		},
		{
			name:    "now serving tablet",
			serving: true,
		},
	}

	sbc.SetResults([]*sqltypes.Result{{}, {}, {}})
	for _, tcase := range tcases {
		ch <- &discovery.TabletHealth{
			Conn:    sbc,
			Tablet:  tablet,
			Target:  target,
			Serving: tcase.serving,
			Stats:   &querypb.RealtimeStats{TableSchemaChanged: tcase.updatedTbls},
		}
		time.Sleep(5 * time.Millisecond)
	}

	require.False(t, waitTimeout(&wg, 5*time.Second), "schema was updated but received no signal")
	require.Equal(t, []string{mysql.FetchTables, mysql.FetchUpdatedTables, mysql.FetchTables}, sbc.StringQueries())
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
		tracked: map[keyspaceStr]*updateController{
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

	assert.NotNil(t, ks1.reloadKeyspace, "ks1 needs to be initialized")
	assert.NotNil(t, ks2.reloadKeyspace, "ks2 needs to be initialized")
	assert.Nil(t, ks3.reloadKeyspace, "ks3 already initialized")
}

// TestViewsTracking tests that the tracker is able to track views.
func TestViewsTracking(t *testing.T) {
	target := &querypb.Target{Cell: "aa", Keyspace: "ks", Shard: "-80", TabletType: topodatapb.TabletType_PRIMARY}
	tablet := &topodatapb.Tablet{Keyspace: target.Keyspace, Shard: target.Shard, Type: target.TabletType}

	schemaDefResult := []map[string]string{{
		"prior": "create view prior as select 1 from tbl",
		"t1":    "create view t1 as select 1 from tbl1",
		"t2":    "create view t2 as select 1 from tbl2",
	}, {
		"t2": "create view t2 as select 1,2 from tbl2",
		"t3": "create view t3 as select 1 from tbl3",
	}, {
		"t4": "create view t4 as select 1 from tbl4",
	}}

	testcases := []struct {
		testName string
		updView  []string
		exp      map[string]string
	}{{
		testName: "new views",
		updView:  []string{"prior", "t1", "t2"},
		exp: map[string]string{
			"t1":    "select 1 from tbl1",
			"t2":    "select 1 from tbl2",
			"prior": "select 1 from tbl"},
	}, {
		testName: "delete prior, updated t2 and new t3",
		updView:  []string{"prior", "t2", "t3"},
		exp: map[string]string{
			"t1": "select 1 from tbl1",
			"t2": "select 1, 2 from tbl2",
			"t3": "select 1 from tbl3"},
	}, {
		testName: "new t4",
		updView:  []string{"t4"},
		exp: map[string]string{
			"t1": "select 1 from tbl1",
			"t2": "select 1, 2 from tbl2",
			"t3": "select 1 from tbl3",
			"t4": "select 1 from tbl4"},
	}}

	ch := make(chan *discovery.TabletHealth)
	tracker := NewTracker(ch, "", true)
	tracker.tables = nil // making tables map nil - so load keyspace does not try to load the tables information.
	tracker.consumeDelay = 1 * time.Millisecond
	tracker.Start()
	defer tracker.Stop()

	wg := sync.WaitGroup{}
	tracker.RegisterSignalReceiver(func() {
		wg.Done()
	})

	sbc := sandboxconn.NewSandboxConn(tablet)
	sbc.SetSchemaResult(schemaDefResult)

	for count, tcase := range testcases {
		t.Run(tcase.testName, func(t *testing.T) {
			wg.Add(1)
			ch <- &discovery.TabletHealth{
				Conn:    sbc,
				Tablet:  tablet,
				Target:  target,
				Serving: true,
				Stats:   &querypb.RealtimeStats{ViewSchemaChanged: tcase.updView},
			}

			require.False(t, waitTimeout(&wg, time.Second), "schema was updated but received no signal")
			require.EqualValues(t, count+1, sbc.GetSchemaCount.Get())

			_, keyspacePresent := tracker.tracked[target.Keyspace]
			require.Equal(t, true, keyspacePresent)

			for k, v := range tcase.exp {
				utils.MustMatch(t, v, sqlparser.String(tracker.GetViews("ks", k)), "mismatch for table: ", k)
			}
		})
	}
}
