package vstreamer

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
)

const (
	createTableQuery = "create table %s(id%d1 int, id%d2 int, primary key(id%d1))"
	bulkInsertQuery  = "insert into %s (id%d1, id%d2) values "
	insertQuery      = "insert into %s (id%d1, id%d2) values (%d, %d)"
	fieldEventTpl    = "type:FIELD field_event:<table_name:\"%s\" fields:<name:\"id%d1\" type:INT32 > fields:<name:\"id%d2\" type:INT32 > > "
	insertEventTpl   = "type:ROW row_event:<row_changes:<after:<lengths:%d lengths:%d values:\"%d%d\" > > > "
	numInitialRows   = 10
)

type tableInfo struct {
	name              string
	fieldEvent        string
	insertEvents      []string
	catchupEvents     []string
	fastForwardEvents []string
}

type state struct {
	tables          []string
	tableInfos      map[string]*tableInfo
	replicateEvents []string
}

var testState = &state{}

var positions map[string]string
var allEvents []*binlogdatapb.VEvent

var callbacks map[string]func()
var cancelTest func()

func TestVStreamCopyCompleteFlow(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancelTest = cancel
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

	var rules []*binlogdata.Rule
	var tablePKs []*TableLastPK
	for i, table := range testState.tables {
		rules = append(rules, getRule(table))
		tablePKs = append(tablePKs, getTablePK(table, i+1))
	}
	filter := &binlogdatapb.Filter{
		Rules: rules,
	}

	callbacks["OTHER.*Copy Start t2"] = func() {
		conn, err := env.Mysqld.GetDbaConnection()
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
	callbacks["ROW.*t1.*12120"] = func() {
		log.Info("Got fast forward row, unlocking t2")
		execStatement(t, "unlock tables")
	}

	callbacks["OTHER.*Copy Done"] = func() {
		log.Info("Got copy done, canceling context")
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
	numCopyEvents := 3 /*t1,t2,t3*/ * (numInitialRows + 1 /*FieldEvent*/ + 1 /*LastPKEvent*/ + 1 /*Copy Start Other TestEvent*/)
	numCopyEvents += 2        /* GTID + Test event after all copy is done */
	numCatchupEvents := 5     /*t1:FIELD+ROW*/
	numFastForwardEvents := 5 /*t1:FIELD+ROW*/
	numExpectedEvents := numCopyEvents + numCatchupEvents + numFastForwardEvents
	printAllEvents("End of test")
	if len(allEvents) != numExpectedEvents {
		t.Fatalf("Received %d events, expected %d", len(allEvents), numExpectedEvents)
	} else {
		log.Infof("Successfully received %d events", numExpectedEvents)
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

func getFieldEvent(table string, idx int) string {
	return fmt.Sprintf(fieldEventTpl, table, idx, idx)
}

func getInsertEvents(table string, idx int, numRows int) []string {
	var events []string
	for i := 1; i <= numRows; i++ {
		events = append(events,
			fmt.Sprintf(insertEventTpl, len(strconv.Itoa(i)), len(strconv.Itoa(i*idx*10)), i, i*idx*10))
	}
	events = append(events, "gtid")
	return events
}

func initTables(t *testing.T, tables []string) {
	var idx int
	tableInfos := make(map[string]*tableInfo)
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
		tableInfos[table] = &tableInfo{
			name:         table,
			fieldEvent:   getFieldEvent(table, idx),
			insertEvents: getInsertEvents(table, idx, numInitialRows),
		}

		callbacks[fmt.Sprintf("LASTPK.*%s.*%d", table, numInitialRows)] = func() {
			if tableName == "t1" {
				insertRow(t, "t1", 1, numInitialRows+1)
				log.Infof("Position after first insert into t1: %s", masterPosition(t))
			}
		}
	}
	positions["afterInitialInsert"] = masterPosition(t)
	testState.tableInfos = tableInfos
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

func getRule(table string) *binlogdata.Rule {
	return &binlogdata.Rule{
		Match:  table,
		Filter: fmt.Sprintf("select * from %s", table),
	}
}

func getTablePK(table string, idx int) *TableLastPK {
	return &TableLastPK{
		name: table,
		lastPK: &sqltypes.Result{
			Fields: []*query.Field{{Name: fmt.Sprintf("id%d1", idx), Type: query.Type_INT32}},
			Rows:   [][]sqltypes.Value{{sqltypes.NewInt32(0)}},
		},
	}
}

func insertRow(t *testing.T, table string, idx int, id int) {
	query := fmt.Sprintf(insertQuery, table, idx, idx, id, id*idx*10)
	execStatement(t, query)
	insertEvent := fmt.Sprintf(insertEventTpl, len(strconv.Itoa(id)), len(strconv.Itoa(id*idx*10)), id, id*idx*10)
	testState.tableInfos[table].catchupEvents = append(testState.tableInfos[table].catchupEvents, insertEvent)
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

func startVStreamCopy(ctx context.Context, t *testing.T, filter *binlogdatapb.Filter, tablePKs []*TableLastPK) {
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
