/*
Copyright 2020 The Vitess Authors.

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

/*
TestVStreamCopyCompleteFlow tests a complete happy VStream Copy flow: copy/catchup/fastforward/replicate
Three tables t1, t2, t3 are copied. Initially 10 (numInitialRows) rows are inserted into each.
To avoid races in testing we send additional events when *uvstreamerTestMode* is set to true. These are used in
conjunction with callbacks to do additional crud at precise points of the flow to test the different paths
We intercept the vstreamer send callback to look for specific events and invoke these test callbacks.
Fast forward requires tables to be locked briefly to get a snapshot: the test uses this knowledge to hold a lock
on the table in order to insert rows for fastforward to find.

The flow is as follows:
	t1: copy phase, 10 rows.
		The lastpk event is intercepted
		A row is inserted into t1 to be found in catchup
		A row is inserted into t2 which will be an empty transaction during t1 catchup but will be found in t2 copy
	t1/t2: catchup phase finds inserted row in t1
	t2: copy phase to start. Test event is sent, intercepted, we lock t2 to block t2's copy, and a row is inserted into t1 and then unlock
	t2: fastforward finds t1 event
	t2: copy starts
	t2: copy complete
	t3: copy phase to start. Test event is sent, intercepted, we lock t3 to block t3's copy, and two rows are, one each into t1 and t2 and then unlock
	t3: fastforward finds t1 and t2 events
	t3: copy starts
	t3: copy complete, all tables copied, Copy Complete test event sent, insert 3 rows, one each into t1/t2/t3
	replicate: finds the 3 inserts, context is cancelled after last expected row callback and its commit, vstream context is cancelled
*/

package vstreamer

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/dbconfigs"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

const (
	createTableQuery = "create table %s(id%d1 int, id%d2 int, primary key(id%d1))"
	bulkInsertQuery  = "insert into %s (id%d1, id%d2) values "
	insertQuery      = "insert into %s (id%d1, id%d2) values (%d, %d)"
	numInitialRows   = 10
)

type state struct {
	tables []string
}

var testState = &state{}

var positions map[string]string
var allEvents []*binlogdatapb.VEvent
var muAllEvents sync.Mutex
var callbacks map[string]func()

func TestVStreamCopyFilterValidations(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer execStatements(t, []string{
		"drop table t1",
		"drop table t2a",
		"drop table t2b",
	})

	execStatements(t, []string{
		"create table t1(id11 int, id12 int, primary key(id11))",
		"create table t2a(id21 int, id22 int, primary key(id21))",
		"create table t2b(id21 int, id22 int, primary key(id21))",
	})
	engine.se.Reload(context.Background())

	var getUVStreamer = func(filter *binlogdatapb.Filter, tablePKs []*binlogdatapb.TableLastPK) *uvstreamer {
		uvs := &uvstreamer{
			ctx:        ctx,
			cancel:     cancel,
			vse:        nil,
			send:       nil,
			cp:         dbconfigs.Connector{},
			se:         engine.se,
			startPos:   "",
			filter:     filter,
			vschema:    nil,
			config:     nil,
			inTablePKs: tablePKs,
		}
		return uvs
	}
	var testFilter = func(rules []*binlogdatapb.Rule, tablePKs []*binlogdatapb.TableLastPK, expected []string, expectedError string) {
		uvs := getUVStreamer(&binlogdatapb.Filter{Rules: rules}, tablePKs)
		if expectedError == "" {
			require.NoError(t, uvs.init())
		} else {
			require.Error(t, uvs.init(), expectedError)
			return
		}
		require.Equal(t, len(expected), len(uvs.plans))
		for _, tableName := range expected {
			require.True(t, uvs.plans[tableName].tablePK.TableName == tableName)
			if tablePKs == nil {
				require.Nil(t, uvs.plans[tableName].tablePK.Lastpk)
			}
		}
		for _, pk := range tablePKs {
			require.Equal(t, uvs.plans[pk.TableName].tablePK, pk)
		}
	}

	type TestCase struct {
		rules         []*binlogdatapb.Rule
		tablePKs      []*binlogdatapb.TableLastPK
		expected      []string
		expectedError string
	}

	var testCases []*TestCase

	testCases = append(testCases, &TestCase{[]*binlogdatapb.Rule{{Match: "t1"}}, nil, []string{"t1"}, ""})
	testCases = append(testCases, &TestCase{[]*binlogdatapb.Rule{{Match: "t2a"}, {Match: "t1"}}, nil, []string{"t1", "t2a"}, ""})
	testCases = append(testCases, &TestCase{[]*binlogdatapb.Rule{{Match: "/.*"}}, nil, []string{"t1", "t2a", "t2b"}, ""})
	testCases = append(testCases, &TestCase{[]*binlogdatapb.Rule{{Match: "/t2.*"}}, nil, []string{"t2a", "t2b"}, ""})

	tablePKs := []*binlogdatapb.TableLastPK{{
		TableName: "t1",
		Lastpk:    getQRFromLastPK([]*query.Field{{Name: "id11", Type: query.Type_INT32}}, []sqltypes.Value{sqltypes.NewInt32(10)}),
	}}
	testCases = append(testCases, &TestCase{[]*binlogdatapb.Rule{{Match: "t1"}}, tablePKs, []string{"t1"}, ""})

	testCases = append(testCases, &TestCase{[]*binlogdatapb.Rule{{Match: "/.*"}, {Match: "xyz"}}, nil, []string{""}, "table xyz is not present in the database"})
	testCases = append(testCases, &TestCase{[]*binlogdatapb.Rule{{Match: "/x.*"}}, nil, []string{""}, "stream needs a position or a table to copy"})

	for _, tc := range testCases {
		log.Infof("Running %v", tc.rules)
		testFilter(tc.rules, tc.tablePKs, tc.expected, tc.expectedError)
	}
}

func TestVStreamCopyCompleteFlow(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer execStatements(t, []string{
		"drop table t1",
		"drop table t2",
		"drop table t3",
	})

	uvstreamerTestMode = true
	defer func() { uvstreamerTestMode = false }()
	initialize(t)
	if err := engine.se.Reload(context.Background()); err != nil {
		t.Fatal("Error reloading schema")
	}

	var rules []*binlogdatapb.Rule
	var tablePKs []*binlogdatapb.TableLastPK
	for i, table := range testState.tables {
		rules = append(rules, getRule(table))
		tablePKs = append(tablePKs, getTablePK(table, i+1))
	}
	filter := &binlogdatapb.Filter{
		Rules: rules,
	}

	// Test event called after t1 copy is complete
	callbacks["OTHER.*Copy Start t2"] = func() {
		conn, err := env.Mysqld.GetDbaConnection(ctx)
		require.NoError(t, err)
		defer conn.Close()

		log.Info("Inserting row for fast forward to find, locking t2")
		conn.ExecuteFetch("lock tables t2 write", 1, false)
		insertRow(t, "t1", 1, numInitialRows+2)
		log.Infof("Position after second insert into t1: %s", masterPosition(t))
		conn.ExecuteFetch("unlock tables", 1, false)
		log.Info("Inserted row for fast forward to find, unlocked tables")

	}

	callbacks["OTHER.*Copy Start t3"] = func() {
		conn, err := env.Mysqld.GetDbaConnection(ctx)
		require.NoError(t, err)
		defer conn.Close()

		log.Info("Inserting row for fast forward to find, locking t3")
		conn.ExecuteFetch("lock tables t3 write", 1, false)
		insertRow(t, "t1", 1, numInitialRows+3)
		insertRow(t, "t2", 2, numInitialRows+2)
		log.Infof("Position after third insert into t1: %s", masterPosition(t))
		conn.ExecuteFetch("unlock tables", 1, false)
		log.Info("Inserted rows for fast forward to find, unlocked tables")

	}

	callbacks["OTHER.*Copy Done"] = func() {
		log.Info("Copy done, inserting events to stream")
		insertRow(t, "t1", 1, numInitialRows+4)
		insertRow(t, "t2", 2, numInitialRows+3)
		// savepoints should not be sent in the event stream
		execStatement(t, `
begin;
insert into t3 (id31, id32) values (12, 360);
savepoint a;
insert into t3 (id31, id32) values (13, 390);
rollback work to savepoint a;
savepoint b;
insert into t3 (id31, id32) values (13, 390);
release savepoint b;
commit;"
`)
	}

	numCopyEvents := 3 /*t1,t2,t3*/ * (numInitialRows + 1 /*FieldEvent*/ + 1 /*LastPKEvent*/ + 1 /*TestEvent: Copy Start*/ + 2 /*begin,commit*/ + 3 /* LastPK Completed*/)
	numCopyEvents += 2                                    /* GTID + Test event after all copy is done */
	numCatchupEvents := 3 * 5                             /*2 t1, 1 t2 : BEGIN+FIELD+ROW+GTID+COMMIT*/
	numFastForwardEvents := 5                             /*t1:FIELD+ROW*/
	numMisc := 1                                          /* t2 insert during t1 catchup that comes in t2 copy */
	numReplicateEvents := 2*5 /* insert into t1/t2 */ + 8 /* begin/field/2 inserts/gtid/commit + 2 savepoints */
	numExpectedEvents := numCopyEvents + numCatchupEvents + numFastForwardEvents + numMisc + numReplicateEvents

	var lastRowEventSeen bool

	callbacks["ROW.*t3.*13390"] = func() {
		log.Infof("Saw last row event")
		lastRowEventSeen = true
	}

	callbacks["COMMIT"] = func() {
		log.Infof("Got commit, lastRowSeen is %t", lastRowEventSeen)
		if lastRowEventSeen {
			log.Infof("Found last row event, canceling context")
			cancel()
		}
	}
	resetMetrics(t)
	startVStreamCopy(ctx, t, filter, tablePKs)

	select {
	case <-time.After(5 * time.Second):
		printAllEvents("Timed out")
		t.Fatal("Timed out waiting for events")
	case <-ctx.Done():
		log.Infof("Received context.Done, ending test")
	}
	muAllEvents.Lock()
	defer muAllEvents.Unlock()
	if len(allEvents) != numExpectedEvents {
		printAllEvents(fmt.Sprintf("Received %d events, expected %d", len(allEvents), numExpectedEvents))
		t.Fatalf("Received %d events, expected %d", len(allEvents), numExpectedEvents)
	} else {
		log.Infof("Successfully received %d events", numExpectedEvents)
	}
	validateReceivedEvents(t)
	validateMetrics(t)
}

func validateReceivedEvents(t *testing.T) {
	for i, ev := range allEvents {
		ev.Timestamp = 0
		if ev.Type == binlogdatapb.VEventType_FIELD {
			for j := range ev.FieldEvent.Fields {
				ev.FieldEvent.Fields[j].Flags = 0
			}
		}
		got := ev.String()
		want := expectedEvents[i]
		if !strings.HasPrefix(got, want) {
			printAllEvents("Events not received in the right order")
			t.Fatalf("Event %d did not match, want %s, got %s", i, want, got)
		}
	}
}

func resetMetrics(t *testing.T) {
	engine.vstreamerEventsStreamed.Reset()
	engine.resultStreamerNumRows.Reset()
	engine.rowStreamerNumRows.Reset()
	engine.vstreamerPhaseTimings.Reset()
}

func validateMetrics(t *testing.T) {
	require.Equal(t, engine.vstreamerEventsStreamed.Get(), int64(len(allEvents)))
	require.Equal(t, engine.resultStreamerNumRows.Get(), int64(0))
	require.Equal(t, engine.rowStreamerNumRows.Get(), int64(31))
	require.Equal(t, engine.vstreamerPhaseTimings.Counts()["VStreamerTest.copy"], int64(3))
	require.Equal(t, engine.vstreamerPhaseTimings.Counts()["VStreamerTest.catchup"], int64(2))
	require.Equal(t, engine.vstreamerPhaseTimings.Counts()["VStreamerTest.fastforward"], int64(2))
}

func insertMultipleRows(t *testing.T, table string, idx int, numRows int) {
	query1 := fmt.Sprintf(bulkInsertQuery, table, idx, idx)
	s := ""
	for i := 1; i <= numRows; i++ {
		if s != "" {
			s += ","
		}
		s += fmt.Sprintf("(%d,%d)", i, i*idx*10)
	}
	query1 += s
	execStatement(t, query1)
}

func initTables(t *testing.T, tables []string) {
	var idx int
	positions["start"] = masterPosition(t)
	for i, table := range tables {
		idx = i + 1
		execStatement(t, fmt.Sprintf(createTableQuery, table, idx, idx, idx))
	}
	for i, table := range tables {
		tableName := table
		idx = i + 1
		insertMultipleRows(t, table, idx, numInitialRows)
		positions[fmt.Sprintf("%sBulkInsert", table)] = masterPosition(t)

		callbacks[fmt.Sprintf("LASTPK.*%s.*%d", table, numInitialRows)] = func() {
			ctx := context.Background()
			if tableName == "t1" {
				idx := 1
				id := numInitialRows + 1
				table := "t1"
				query1 := fmt.Sprintf(insertQuery, table, idx, idx, id, id*idx*10)
				idx = 2
				table = "t2"
				query2 := fmt.Sprintf(insertQuery, table, idx, idx, id, id*idx*10)

				queries := []string{
					"begin",
					query1,
					query2,
					"commit",
				}
				env.Mysqld.ExecuteSuperQueryList(ctx, queries)
				log.Infof("Position after first insert into t1 and t2: %s", masterPosition(t))
			}
		}
	}
	positions["afterInitialInsert"] = masterPosition(t)
}

func initialize(t *testing.T) {
	callbacks = make(map[string]func())
	testState.tables = []string{"t1", "t2", "t3"}
	positions = make(map[string]string)
	initTables(t, testState.tables)
	callbacks["gtid.*"+positions["afterInitialInsert"]] = func() {
		log.Infof("Callback: afterInitialInsert")
	}
}

func getRule(table string) *binlogdatapb.Rule {
	return &binlogdatapb.Rule{
		Match:  table,
		Filter: fmt.Sprintf("select * from %s", table),
	}
}

func getTablePK(table string, idx int) *binlogdatapb.TableLastPK {
	fields := []*query.Field{{Name: fmt.Sprintf("id%d1", idx), Type: query.Type_INT32}}

	lastPK := []sqltypes.Value{sqltypes.NewInt32(0)}
	return &binlogdatapb.TableLastPK{
		TableName: table,
		Lastpk:    getQRFromLastPK(fields, lastPK),
	}
}

func insertRow(t *testing.T, table string, idx int, id int) {
	execStatement(t, fmt.Sprintf(insertQuery, table, idx, idx, id, id*idx*10))
}

func printAllEvents(msg string) {
	log.Errorf("%s: Received %d events", msg, len(allEvents))
	for i, ev := range allEvents {
		log.Errorf("%d:\t%s", i, ev)
	}
}

func getEventCallback(event *binlogdatapb.VEvent) func() {
	s := fmt.Sprintf("%v", event)
	for key, cb := range callbacks {
		match := regexp.MustCompile(".*" + key + ".*")
		if key == s || match.MatchString(s) {
			return cb
		}
	}
	return nil
}

func startVStreamCopy(ctx context.Context, t *testing.T, filter *binlogdatapb.Filter, tablePKs []*binlogdatapb.TableLastPK) {
	pos := ""
	go func() {
		err := engine.Stream(ctx, pos, tablePKs, filter, func(evs []*binlogdatapb.VEvent) error {
			//t.Logf("Received events: %v", evs)
			muAllEvents.Lock()
			defer muAllEvents.Unlock()
			for _, ev := range evs {
				if ev.Type == binlogdatapb.VEventType_HEARTBEAT {
					continue
				}
				cb := getEventCallback(ev)
				if cb != nil {
					cb()
				}
				allEvents = append(allEvents, ev)
			}
			return nil
		})
		require.Nil(t, err)
	}()
}

var expectedEvents = []string{
	"type:OTHER gtid:\"Copy Start t1\"",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t1\" fields:<name:\"id11\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id11\" column_length:11 charset:63 > fields:<name:\"id12\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id12\" column_length:11 charset:63 > > ",
	"type:GTID",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:1 lengths:2 values:\"110\" > > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:1 lengths:2 values:\"220\" > > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:1 lengths:2 values:\"330\" > > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:1 lengths:2 values:\"440\" > > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:1 lengths:2 values:\"550\" > > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:1 lengths:2 values:\"660\" > > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:1 lengths:2 values:\"770\" > > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:1 lengths:2 values:\"880\" > > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:1 lengths:2 values:\"990\" > > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:2 lengths:3 values:\"10100\" > > > ",
	"type:LASTPK last_p_k_event:<table_last_p_k:<table_name:\"t1\" lastpk:<rows:<lengths:2 values:\"10\" > > > > ",
	"type:COMMIT",
	"type:BEGIN",
	"type:LASTPK last_p_k_event:<table_last_p_k:<table_name:\"t1\" > completed:true > ",
	"type:COMMIT",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t1\" fields:<name:\"id11\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id11\" column_length:11 charset:63 > fields:<name:\"id12\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id12\" column_length:11 charset:63 > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:2 lengths:3 values:\"11110\" > > > ",
	"type:GTID",
	"type:COMMIT", //insert for t2 done along with t1 does not generate an event since t2 is not yet copied
	"type:OTHER gtid:\"Copy Start t2\"",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t1\" fields:<name:\"id11\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id11\" column_length:11 charset:63 > fields:<name:\"id12\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id12\" column_length:11 charset:63 > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:2 lengths:3 values:\"12120\" > > > ",
	"type:GTID",
	"type:COMMIT",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t2\" fields:<name:\"id21\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id21\" column_length:11 charset:63 > fields:<name:\"id22\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id22\" column_length:11 charset:63 > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:1 lengths:2 values:\"120\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:1 lengths:2 values:\"240\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:1 lengths:2 values:\"360\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:1 lengths:2 values:\"480\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:1 lengths:3 values:\"5100\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:1 lengths:3 values:\"6120\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:1 lengths:3 values:\"7140\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:1 lengths:3 values:\"8160\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:1 lengths:3 values:\"9180\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:2 lengths:3 values:\"10200\" > > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:2 lengths:3 values:\"11220\" > > > ",
	"type:LASTPK last_p_k_event:<table_last_p_k:<table_name:\"t2\" lastpk:<rows:<lengths:2 values:\"11\" > > > > ",
	"type:COMMIT",
	"type:BEGIN",
	"type:LASTPK last_p_k_event:<table_last_p_k:<table_name:\"t2\" > completed:true > ",
	"type:COMMIT",
	"type:OTHER gtid:\"Copy Start t3\"",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t1\" fields:<name:\"id11\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id11\" column_length:11 charset:63 > fields:<name:\"id12\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id12\" column_length:11 charset:63 > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:2 lengths:3 values:\"13130\" > > > ",
	"type:GTID",
	"type:COMMIT",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t2\" fields:<name:\"id21\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id21\" column_length:11 charset:63 > fields:<name:\"id22\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id22\" column_length:11 charset:63 > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:2 lengths:3 values:\"12240\" > > > ",
	"type:GTID",
	"type:COMMIT",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t3\" fields:<name:\"id31\" type:INT32 table:\"t3\" org_table:\"t3\" database:\"vttest\" org_name:\"id31\" column_length:11 charset:63 > fields:<name:\"id32\" type:INT32 table:\"t3\" org_table:\"t3\" database:\"vttest\" org_name:\"id32\" column_length:11 charset:63 > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:1 lengths:2 values:\"130\" > > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:1 lengths:2 values:\"260\" > > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:1 lengths:2 values:\"390\" > > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:1 lengths:3 values:\"4120\" > > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:1 lengths:3 values:\"5150\" > > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:1 lengths:3 values:\"6180\" > > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:1 lengths:3 values:\"7210\" > > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:1 lengths:3 values:\"8240\" > > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:1 lengths:3 values:\"9270\" > > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:2 lengths:3 values:\"10300\" > > > ",
	"type:LASTPK last_p_k_event:<table_last_p_k:<table_name:\"t3\" lastpk:<rows:<lengths:2 values:\"10\" > > > > ",
	"type:COMMIT",
	"type:BEGIN",
	"type:LASTPK last_p_k_event:<table_last_p_k:<table_name:\"t3\" > completed:true > ",
	"type:COMMIT",
	"type:OTHER gtid:\"Copy Done\"",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t1\" fields:<name:\"id11\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id11\" column_length:11 charset:63 > fields:<name:\"id12\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id12\" column_length:11 charset:63 > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:2 lengths:3 values:\"14140\" > > > ",
	"type:GTID",
	"type:COMMIT",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t2\" fields:<name:\"id21\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id21\" column_length:11 charset:63 > fields:<name:\"id22\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id22\" column_length:11 charset:63 > > ",
	"type:ROW row_event:<table_name:\"t2\" row_changes:<after:<lengths:2 lengths:3 values:\"13260\" > > > ",
	"type:GTID",
	"type:COMMIT",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t3\" fields:<name:\"id31\" type:INT32 table:\"t3\" org_table:\"t3\" database:\"vttest\" org_name:\"id31\" column_length:11 charset:63 > fields:<name:\"id32\" type:INT32 table:\"t3\" org_table:\"t3\" database:\"vttest\" org_name:\"id32\" column_length:11 charset:63 > > ",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:2 lengths:3 values:\"12360\" > > > ",
	"type:SAVEPOINT statement:\"SAVEPOINT `a`\"",
	"type:SAVEPOINT statement:\"SAVEPOINT `b`\"",
	"type:ROW row_event:<table_name:\"t3\" row_changes:<after:<lengths:2 lengths:3 values:\"13390\" > > > ",
	"type:GTID",
	"type:COMMIT",
}
