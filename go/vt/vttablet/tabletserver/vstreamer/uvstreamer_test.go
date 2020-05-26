package vstreamer

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
)

/*

	We want to test:

	Copy Phase: insert at start (3 tables)
	Catchup:
		after 1st, before 2nd starts insert into 1st   //validate copy of 1st, catchup of 1st
		after 2nd, before 3rd starts insert into 1st and 2nd  //validate copy of 2nd, catchup of 1st and 2nd, copy of 3rd

*/

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
		mutexName := fmt.Sprintf("%sCopy", table)
		mutexes[mutexName] = &sync.Mutex{}
		mutexes[mutexName].Lock()

		callbacks[fmt.Sprintf("LASTPK.*%s.*%d", table, numInitialRows)] = func() {
			log.Infof("Unlocking mutex %s, table %s", mutexName, tableName)
			if tableName == "t1" {
				insertRow(t, "t1", 1, numInitialRows+1)
				positions["EOF"] = masterPosition(t)

				callbacks["gtid.*"+positions["EOF"]] = func() {
					log.Infof("Got EOF, canceling test")
					cancelTest()
				}

			}
			mutexes[mutexName].Unlock()
		}
	}
	positions["afterInitialInsert"] = masterPosition(t)
	testState.tableInfos = tableInfos
}

func initialize(t *testing.T) {
	callbacks = make(map[string]func())
	mutexes = make(map[string]*sync.Mutex)
	testState.tables = []string{"t1", "t2", "t3"}
	positions = make(map[string]string)
	initTables(t, testState.tables)
	log.Infof("Positions are %v", positions)
	callbacks["gtid.*"+positions["afterInitialInsert"]] = func() {
		log.Infof("Callback: afterInitialInsert")
	}
}

func getRule(table string) *binlogdata.Rule {
	return &binlogdata.Rule{
		Match:  fmt.Sprintf(table),
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

var callbacks map[string]func()

var cancelTest func()
var mutexes map[string]*sync.Mutex

func TestVStreamCopyCompleteFlow(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancelTest = cancel
	defer cancel()

	initialize(t)

	var rules []*binlogdata.Rule
	var tablePKs []*TableLastPK
	for i, table := range testState.tables {
		rules = append(rules, getRule(table))
		tablePKs = append(tablePKs, getTablePK(table, i+1))
	}
	filter := &binlogdatapb.Filter{
		Rules: rules,
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
	numCopyEvents := 3 /*t1,t2,t3*/ *(numInitialRows+1 /*FieldEvent*/ +1 /*LastPKEvent*/) + 1 /* gtid after all copy is done */
	numCatchupEvents := 2                                                                     /*t1:FIELD+ROW*/
	numFastForwardEvents := 0
	numExpectedEvents := numCopyEvents + numCatchupEvents + numFastForwardEvents
	if len(allEvents) != numExpectedEvents {
		t.Fatalf("Received %d events, expected %d", len(allEvents), numExpectedEvents)
	} else {
		log.Infof("Successfully received %d events", numExpectedEvents)
	}
	//printAllEvents("End of test")
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
