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
	t1: copy phase, 10 rows. The lastpk event is intercepted and a row is inserted into t1 to be found in catchup
	t1/t2: catchup phase finds inserted row in t1
	t2: copy phase to start. Test event is sent, intercepted, we lock t2 to block t2's copy, and a row is inserted into t1 and then unlock
	t2: fastforward finds t1 event
	t2: copy starts
	t2: copy complete
	all tables copied, Copy Complete test event sent, vstream context is cancelled
*/

package vstreamer

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

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

var callbacks map[string]func()

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
	engine.se.Reload(context.Background())

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
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		log.Info("Inserting row for fast forward to find, locking t2")
		conn.ExecuteFetch("lock tables t2 write", 1, false)
		insertRow(t, "t1", 1, numInitialRows+2)
		log.Infof("Position after second insert into t1: %s", masterPosition(t))
		conn.ExecuteFetch("unlock tables", 1, false)
		log.Info("Inserted row for fast forward to find, unlocked tables")

	}

	callbacks["OTHER.*Copy Done"] = func() {
		log.Info("Copy done, canceling context")
		cancel()
	}
	startVStreamCopy(ctx, t, filter, tablePKs)

	/*
		//vstream catchup t1 and copy t2
		insertRow(t, "t1", 1, numInitialRows+2)
		insertRow(t, "t2", 2, numInitialRows+1)

		//vstream catchup t1,t2 and copy t3
		insertRow(t, "t1", 1, numInitialRows+2)
		insertRow(t, "t2", 2, numInitialRows+1)
	*/

	select {
	case <-time.After(10 * time.Second):
		printAllEvents("Timed out")
		t.Fatal("Timed out waiting for events")
	case <-ctx.Done():
		log.Infof("Received context.Done, ending test")
	}
	numCopyEvents := 3 /*t1,t2,t3*/ * (numInitialRows + 1 /*FieldEvent*/ + 1 /*LastPKEvent*/ + 1 /*TestEvent: Copy Start*/ + 2 /*begin,commit*/ + 3 /* LastPK Completed*/)
	numCopyEvents += 2        /* GTID + Test event after all copy is done */
	numCatchupEvents := 5     /*t1:FIELD+ROW*/
	numFastForwardEvents := 5 /*t1:FIELD+ROW*/
	numExpectedEvents := numCopyEvents + numCatchupEvents + numFastForwardEvents
	printAllEvents("End of test")
	if len(allEvents) != numExpectedEvents {
		log.Errorf("Received %d events, expected %d", len(allEvents), numExpectedEvents)
		for _, ev := range allEvents {
			log.Errorf("\t%s", ev)
		}
		t.Fatalf("Received %d events, expected %d", len(allEvents), numExpectedEvents)
	} else {
		log.Infof("Successfully received %d events", numExpectedEvents)
	}
	validateReceivedEvents(t)

}

func validateReceivedEvents(t *testing.T) {
	if len(allEvents) != len(expectedEvents) {
		t.Fatalf("Received events not equal to expected events, wanted %d, got %d", len(expectedEvents), len(allEvents))
	}
	for i, ev := range allEvents {
		ev.Timestamp = 0
		got := ev.String()
		want := expectedEvents[i]
		if !strings.HasPrefix(got, want) {
			t.Fatalf("Event %d did not match, want %s, got %s", i, want, got)
		}
	}
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
			if tableName == "t1" {
				insertRow(t, "t1", 1, numInitialRows+1)
				log.Infof("Position after first insert into t1: %s", masterPosition(t))
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
	query := fmt.Sprintf(insertQuery, table, idx, idx, id, id*idx*10)
	execStatement(t, query)
}

func printAllEvents(msg string) {
	log.Infof("%s: Received %d events", msg, len(allEvents))
	for _, ev := range allEvents {
		log.Infof("\t%s", ev)
	}
}

func getEventCallback(ctx context.Context, t *testing.T, event *binlogdatapb.VEvent) func() {
	s := fmt.Sprintf("%v", event)
	for key, cb := range callbacks {
		match := regexp.MustCompile(".*" + key + ".*")
		if match.MatchString(s) {
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
			for _, ev := range evs {
				if ev.Type == binlogdatapb.VEventType_HEARTBEAT {
					continue
				}
				cb := getEventCallback(ctx, t, ev)
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
	"type:FIELD field_event:<table_name:\"t1\" fields:<name:\"id11\" type:INT32 > fields:<name:\"id12\" type:INT32 > > ",
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
	"type:FIELD field_event:<table_name:\"t1\" fields:<name:\"id11\" type:INT32 > fields:<name:\"id12\" type:INT32 > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:2 lengths:3 values:\"11110\" > > > ",
	"type:GTID",
	"type:COMMIT",
	"type:OTHER gtid:\"Copy Start t2\"",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t1\" fields:<name:\"id11\" type:INT32 > fields:<name:\"id12\" type:INT32 > > ",
	"type:ROW row_event:<table_name:\"t1\" row_changes:<after:<lengths:2 lengths:3 values:\"12120\" > > > ",
	"type:GTID",
	"type:COMMIT",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t2\" fields:<name:\"id21\" type:INT32 > fields:<name:\"id22\" type:INT32 > > ",
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
	"type:LASTPK last_p_k_event:<table_last_p_k:<table_name:\"t2\" lastpk:<rows:<lengths:2 values:\"10\" > > > > ",
	"type:COMMIT",
	"type:BEGIN",
	"type:LASTPK last_p_k_event:<table_last_p_k:<table_name:\"t2\" > completed:true > ",
	"type:COMMIT",
	"type:OTHER gtid:\"Copy Start t3\"",
	"type:BEGIN",
	"type:FIELD field_event:<table_name:\"t3\" fields:<name:\"id31\" type:INT32 > fields:<name:\"id32\" type:INT32 > > ",
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
}
