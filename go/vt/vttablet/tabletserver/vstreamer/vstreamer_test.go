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

package vstreamer

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type testcase struct {
	input  any
	output [][]string
}

func checkIfOptionIsSupported(t *testing.T, variable string) bool {
	qr, err := env.Mysqld.FetchSuperQuery(context.Background(), fmt.Sprintf("show variables like '%s'", variable))
	require.NoError(t, err)
	require.NotNil(t, qr)
	if qr.Rows != nil && len(qr.Rows) == 1 {
		return true
	}
	return false
}

// TestPlayerNoBlob sets up a new environment with mysql running with
// binlog_row_image as noblob. It confirms that the VEvents created are
// correct: that they don't contain the missing columns and that the
// DataColumns bitmap is sent.
func TestNoBlob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldEngine := engine
	engine = nil
	oldEnv := env
	env = nil
	newEngine(t, ctx, "noblob")
	defer func() {
		if engine != nil {
			engine.Close()
		}
		if env != nil {
			env.Close()
		}
		engine = oldEngine
		env = oldEnv
	}()

	ts := &TestSpec{
		t: t,
		ddls: []string{
			// t1 has a blob column and a primary key. The blob column will not be in update row events.
			"create table t1(id int, blb blob, val varchar(4), primary key(id))",
			// t2 has a text column and no primary key. The text column will be in update row events.
			"create table t2(id int, txt text, val varchar(4), unique key(id, val))",
			// t3 has a text column and a primary key. The text column will not be in update row events.
			"create table t3(id int, txt text, val varchar(4), primary key(id))",
		},
		options: &TestSpecOptions{
			noblob: true,
		},
	}
	defer ts.Close()
	ts.Init()
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 'blob1', 'aaa')", nil},
		{"update t1 set val = 'bbb'", nil},
		{"commit", nil},
	}, {{"begin", nil},
		{"insert into t2 values (1, 'text1', 'aaa')", nil},
		{"update t2 set val = 'bbb'", nil},
		{"commit", nil},
	}, {{"begin", nil},
		{"insert into t3 values (1, 'text1', 'aaa')", nil},
		{"update t3 set val = 'bbb'", nil},
		{"commit", nil},
	}}
	ts.Run()
}

// TestSetAndEnum confirms that the events for set and enum columns are correct.
func TestSetAndEnum(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id int, val binary(4), color set('red','green','blue','black','white','pink','purple','yellow','brown'), size enum('S','M','L'), primary key(id))",
			"create table t2(id int, val binary(4), color set('red','green','blue','black','white','pink','purple','yellow','brown','eggshell','mint','tan','fuschia','teal','babyblue','grey','bulletgrey') collate utf8mb4_bin, size enum('S','M','L') collate utf8mb4_bin, primary key(id)) charset=utf8mb4",
		},
	}
	defer ts.Close()
	ts.Init()
	ts.tests = [][]*TestQuery{
		{
			{"begin", nil},
			{"insert into t1 values (1, 'aaa', 'red,blue', 'S')", nil},
			{"insert into t1 values (2, 'bbb', 'green,pink,purple,yellow,brown', 'M')", nil},
			{"insert into t1 values (3, 'ccc', 'red,green,blue', 'L')", nil},
			{"commit", nil},
		},
		{
			{"begin", nil},
			{"insert into t2 values (1, 'xxx', 'red,blue,black,grey', 'S')", nil},
			{"insert into t2 values (2, 'yyy', 'green,black,pink,purple,yellow,brown,mint,tan,bulletgrey', 'M')", nil},
			{"insert into t2 values (3, 'zzz', 'red,green,blue', 'L')", nil},
			{"commit", nil},
		},
		{
			{"begin", nil},
			// This query fails with the following error when SQL mode includes STRICT:
			// failed: Data truncated for column 'size' at row 1 (errno 1265) (sqlstate 01000) during query: insert into t2 values (4, 'lll', '', '')
			{"set @@session.sql_mode = ''", nil},
			{"insert into t2 values (4, 'lll', '', '')", nil},
			{"insert into t2 values (5, 'mmm', 'invalid', 'invalid,invalid,mint,invalid')", []TestRowEvent{
				{spec: &TestRowEventSpec{table: "t2", changes: []TestRowChange{{after: []string{"5", "mmm", "\x00", ""}}}}},
			}},
			{"insert into t2 values (6, 'nnn', NULL, NULL)", nil},
			{"commit", nil},
		},
	}
	ts.Run()
}

// TestCellValuePadding tests that the events are correctly padded for binary columns.
func TestCellValuePadding(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id int, val binary(4), primary key(val))",
			"create table t2(id int, val char(4), primary key(val))",
			"create table t3(id int, val char(4) collate utf8mb4_bin, primary key(val))"},
	}
	defer ts.Close()
	ts.Init()
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 'aaa\000')", nil},
		{"insert into t1 values (2, 'bbb\000')", nil},
		{"update t1 set id = 11 where val = 'aaa\000'", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"1", "aaa\x00"}, after: []string{"11", "aaa\x00"}}}}},
		}},
		{"insert into t2 values (1, 'aaa')", nil},
		{"insert into t2 values (2, 'bbb')", nil},
		{"update t2 set id = 11 where val = 'aaa'", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t2", changes: []TestRowChange{{before: []string{"1", "aaa"}, after: []string{"11", "aaa"}}}}},
		}},
		{"insert into t3 values (1, 'aaa')", nil},
		{"insert into t3 values (2, 'bb')", nil},
		{"update t3 set id = 11 where val = 'aaa'", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t3", changes: []TestRowChange{{before: []string{"1", "aaa"}, after: []string{"11", "aaa"}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()
}

// TestColumnCollationHandling confirms that we handle column collations
// properly in vstreams now that we parse any optional collation ID values
// in binlog_row_metadata AND we query mysqld for the collation when possible.
func TestColumnCollationHandling(t *testing.T) {
	extraCollation := "utf8mb4_ja_0900_as_cs"           // Test 2 byte collation ID handling
	if strings.HasPrefix(testenv.MySQLVersion, "5.7") { // 5.7 does not support 2 byte collation IDs
		extraCollation = "utf8mb4_croatian_ci"
	}
	ts := &TestSpec{
		t: t,
		ddls: []string{
			fmt.Sprintf("create table t1(id int, txt text, val char(4) collate utf8mb4_bin, id2 int, val2 varchar(64) collate utf8mb4_general_ci, valvb varbinary(128), val3 varchar(255) collate %s, primary key(val))", extraCollation),
		},
	}
	defer ts.Close()
	ts.Init()
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 'aaa', 'aaa', 1, 'aaa', 'aaa', 'aaa')", nil},
		{"insert into t1 values (2, 'bb', 'bb', 1, 'bb', 'bb', 'bb')", nil},
		{"update t1 set id = 11 where val = 'aaa'", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"1", "aaa", "aaa", "1", "aaa", "aaa", "aaa"}, after: []string{"11", "aaa", "aaa", "1", "aaa", "aaa", "aaa"}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()
}

// This test is not ported to the new test framework because it only runs on old deprecated versions of MySQL.
// We leave the test for older flavors until we EOL them.
func TestSetStatement(t *testing.T) {
	if !checkIfOptionIsSupported(t, "log_builtin_as_identified_by_password") {
		// the combination of setting this option and support for "set password" only works on a few flavors
		log.Info("Cannot test SetStatement on this flavor")
		return
	}

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	queries := []string{
		"begin",
		"insert into t1 values (1, 'aaa')",
		"commit",
		"set global log_builtin_as_identified_by_password=1",
		"SET PASSWORD FOR 'vt_appdebug'@'localhost'='*AA17DA66C7C714557F5485E84BCAFF2C209F2F53'", // select password('vtappdebug_password');
	}
	testcases := []testcase{{
		input: queries,
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"t1" fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:3 values:"1aaa"}}}`,
			`gtid`,
			`commit`,
		}, {
			`gtid`,
			`other`,
		}},
	}}
	runCases(t, nil, testcases, "current", nil)
}

// TestSetForeignKeyCheck confirms that the binlog RowEvent flags are set correctly when foreign_key_checks are on and off.
func TestSetForeignKeyCheck(t *testing.T) {
	testRowEventFlags = true
	defer func() { testRowEventFlags = false }()

	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id int, val binary(4), primary key(id))",
		},
	}
	defer ts.Close()
	ts.Init()
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 'aaa')", []TestRowEvent{{flags: 1}}},
		{"set @@session.foreign_key_checks=1", noEvents},
		{"insert into t1 values (2, 'bbb')", []TestRowEvent{{flags: 1}}},
		{"set @@session.foreign_key_checks=0", noEvents},
		{"insert into t1 values (3, 'ccc')", []TestRowEvent{{flags: 3}}},
		{"commit", nil},
	}}
	ts.Run()

}

func TestStmtComment(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id int, val varbinary(128), primary key(id))",
		},
		options: nil,
	}
	defer ts.Close()

	ts.Init()

	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 'aaa')", nil},
		{"commit", nil},
		{"/*!40000 ALTER TABLE `t1` DISABLE KEYS */", []TestRowEvent{
			{restart: true, event: "gtid"},
			{event: "other"}},
		},
	}}
	ts.Run()
}

func TestVersion(t *testing.T) {
	oldEngine := engine
	defer func() {
		engine = oldEngine
	}()

	ctx := context.Background()
	err := env.SchemaEngine.EnableHistorian(true)
	require.NoError(t, err)
	defer env.SchemaEngine.EnableHistorian(false)

	engine = NewEngine(engine.env, env.SrvTopo, env.SchemaEngine, nil, env.Cells[0])
	engine.InitDBConfig(env.KeyspaceName, env.ShardName)
	engine.Open()
	defer engine.Close()

	execStatements(t, []string{
		"create database if not exists _vt",
		"create table if not exists _vt.schema_version(id int, pos varbinary(10000), time_updated bigint(20), ddl varchar(10000), schemax blob, primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table _vt.schema_version",
	})
	dbSchema := &binlogdatapb.MinimalSchema{
		Tables: []*binlogdatapb.MinimalTable{{
			Name: "t1",
		}},
	}
	blob, _ := dbSchema.MarshalVT()
	gtid := "MariaDB/0-41983-20"
	testcases := []testcase{{
		input: []string{
			fmt.Sprintf("insert into _vt.schema_version values(1, '%s', 123, 'create table t1', %v)", gtid, encodeString(string(blob))),
		},
		// External table events don't get sent.
		output: [][]string{{
			`begin`,
			`type:VERSION`}, {
			`gtid`,
			`commit`}},
	}}
	runCases(t, nil, testcases, "", nil)
	mt, err := env.SchemaEngine.GetTableForPos(ctx, sqlparser.NewIdentifierCS("t1"), gtid)
	require.NoError(t, err)
	assert.True(t, proto.Equal(mt, dbSchema.Tables[0]))
}

func TestMissingTables(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id11 int, id12 int, primary key(id11))",
		},
	}
	ts.Init()
	defer ts.Close()
	execStatements(t, []string{
		"create table shortlived(id31 int, id32 int, primary key(id31))",
	})
	defer execStatements(t, []string{
		"drop table _shortlived",
	})
	startPos := primaryPosition(t)
	execStatements(t, []string{
		"insert into shortlived values (1,1), (2,2)",
		"alter table shortlived rename to _shortlived",
	})
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}},
	}
	fe := ts.fieldEvents["t1"]
	insert := "insert into t1 values (101, 1010)"
	rowEvent := getRowEvent(ts, fe, insert)
	testcases := []testcase{
		{
			input:  []string{},
			output: [][]string{},
		},

		{
			input: []string{insert},
			output: [][]string{
				{"begin", "gtid", "commit"},
				{"gtid", "type:OTHER"},
				{"begin", fe.String(), rowEvent, "gtid", "commit"},
			},
		},
	}
	runCases(t, filter, testcases, startPos, nil)
}

func TestVStreamCopySimpleFlow(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id11 int, id12 int, primary key(id11))",
			"create table t2(id21 int, id22 int, primary key(id21))",
		},
	}
	ts.Init()
	defer ts.Close()

	log.Infof("Pos before bulk insert: %s", primaryPosition(t))
	insertSomeRows(t, 10)
	log.Infof("Pos after bulk insert: %s", primaryPosition(t))

	ctx := context.Background()
	qr, err := env.Mysqld.FetchSuperQuery(ctx, "SELECT count(*) as cnt from t1, t2 where t1.id11 = t2.id21")
	if err != nil {
		t.Fatal("Query failed")
	}
	require.Equal(t, "[[INT64(10)]]", fmt.Sprintf("%v", qr.Rows))

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}, {
			Match:  "t2",
			Filter: "select * from t2",
		}},
	}

	var tablePKs []*binlogdatapb.TableLastPK
	tablePKs = append(tablePKs, getTablePK("t1", 1))
	tablePKs = append(tablePKs, getTablePK("t2", 2))
	t1FieldEvent := &TestFieldEvent{
		table: "t1",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id11", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "id12", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
		},
		enumSetStrings: true,
	}
	t2FieldEvent := &TestFieldEvent{
		table: "t2",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id21", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "id22", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
		},
		enumSetStrings: true,
	}

	t1Events := []string{}
	t2Events := []string{}
	for i := 1; i <= 10; i++ {
		t1Events = append(t1Events,
			fmt.Sprintf("type:ROW row_event:{table_name:\"t1\" row_changes:{after:{lengths:%d lengths:%d values:\"%d%d\"}}}", len(strconv.Itoa(i)), len(strconv.Itoa(i*10)), i, i*10))
		t2Events = append(t2Events,
			fmt.Sprintf("type:ROW row_event:{table_name:\"t2\" row_changes:{after:{lengths:%d lengths:%d values:\"%d%d\"}}}", len(strconv.Itoa(i)), len(strconv.Itoa(i*20)), i, i*20))
	}
	t1Events = append(t1Events, "lastpk", "commit")
	t2Events = append(t2Events, "lastpk", "commit")

	// Now we're past the copy phase and have no ENUM or SET columns.
	t1FieldEvent.enumSetStrings = false
	t2FieldEvent.enumSetStrings = false
	insertEvents1 := []string{
		"begin",
		t1FieldEvent.String(),
		getRowEvent(ts, t1FieldEvent, "insert into t1 values (101, 1010)"),
		"gtid",
		"commit",
	}
	insertEvents2 := []string{
		"begin",
		t2FieldEvent.String(),
		getRowEvent(ts, t2FieldEvent, "insert into t2 values (202, 2020)"),
		"gtid",
		"commit",
	}

	testcases := []testcase{
		{
			input:  []string{},
			output: [][]string{{"begin", t1FieldEvent.String()}, {"gtid"}, t1Events, {"begin", "lastpk", "commit"}, {"begin", t2FieldEvent.String()}, t2Events, {"begin", "lastpk", "commit"}, {"copy_completed"}},
		},

		{
			input: []string{
				"insert into t1 values (101, 1010)",
			},
			output: [][]string{insertEvents1},
		},
		{
			input: []string{
				"insert into t2 values (202, 2020)",
			},
			output: [][]string{insertEvents2},
		},
	}

	runCases(t, filter, testcases, "vscopy", tablePKs)
	log.Infof("Pos at end of test: %s", primaryPosition(t))
}

func TestVStreamCopyWithDifferentFilters(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id1 int, id2 int, id3 int, primary key(id1)) charset=utf8mb4",
			"create table t2a(id1 int, id2 int, primary key(id1)) charset=utf8mb4",
			"create table t2b(id1 varchar(20), id2 int, primary key(id1)) charset=utf8mb4",
		},
	}
	ts.Init()
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/t2.*",
		}, {
			Match:  "t1",
			Filter: "select id1, id2 from t1",
		}},
	}

	t1FieldEvent := &TestFieldEvent{
		table: "t1",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id1", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "id2", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
		},
		enumSetStrings: true,
	}

	execStatements(t, []string{
		"insert into t1(id1, id2, id3) values (1, 2, 3)",
		"insert into t2a(id1, id2) values (1, 4)",
		"insert into t2b(id1, id2) values ('b', 6)",
		"insert into t2b(id1, id2) values ('a', 5)",
	})

	// All field events in this test are in the copy phase so they should all
	// have the enum_set_string_values field set.
	for _, fe := range ts.fieldEvents {
		fe.enumSetStrings = true
	}

	var expectedEvents = []string{
		"begin",
		t1FieldEvent.String(),
		"gtid",
		getRowEvent(ts, t1FieldEvent, "insert into t1 values (1, 2)"),
		getLastPKEvent("t1", "id1", sqltypes.Int32, []sqltypes.Value{sqltypes.NewInt32(1)}, collations.CollationBinaryID, uint32(53251)),
		"commit",
		"begin",
		getCopyCompletedEvent("t1"),
		"commit",
		"begin",
		ts.fieldEvents["t2a"].String(),
		getRowEvent(ts, ts.fieldEvents["t2a"], "insert into t2a values (1, 4)"),
		getLastPKEvent("t2a", "id1", sqltypes.Int32, []sqltypes.Value{sqltypes.NewInt32(1)}, collations.CollationBinaryID, uint32(53251)),
		"commit",
		"begin",
		getCopyCompletedEvent("t2a"),
		"commit",
		"begin",
		ts.fieldEvents["t2b"].String(),
		getRowEvent(ts, ts.fieldEvents["t2b"], "insert into t2b values ('a', 5)"),
		getRowEvent(ts, ts.fieldEvents["t2b"], "insert into t2b values ('b', 6)"),
		getLastPKEvent("t2b", "id1", sqltypes.VarChar, []sqltypes.Value{sqltypes.NewVarChar("b")}, uint32(testenv.DefaultCollationID), uint32(20483)),
		"commit",
		"begin",
		getCopyCompletedEvent("t2b"),
		"commit",
	}

	var allEvents []*binlogdatapb.VEvent
	var wg sync.WaitGroup
	wg.Add(1)
	ctx2, cancel2 := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancel2()

	var errGoroutine error
	go func() {
		defer wg.Done()
		engine.Stream(ctx2, "", nil, filter, throttlerapp.VStreamerName, func(evs []*binlogdatapb.VEvent) error {
			for _, ev := range evs {
				if ev.Type == binlogdatapb.VEventType_HEARTBEAT {
					continue
				}
				if ev.Throttled {
					continue
				}
				allEvents = append(allEvents, ev)
			}
			if len(allEvents) == len(expectedEvents) {
				log.Infof("Got %d events as expected", len(allEvents))
				for i, ev := range allEvents {
					ev.Timestamp = 0
					switch ev.Type {
					case binlogdatapb.VEventType_FIELD:
						for j := range ev.FieldEvent.Fields {
							ev.FieldEvent.Fields[j].Flags = 0
						}
						ev.FieldEvent.Keyspace = ""
						ev.FieldEvent.Shard = ""
						// All events in this test are in the copy phase so they should
						// all have the enum_set_string_values field set.
						ev.FieldEvent.EnumSetStringValues = true
					case binlogdatapb.VEventType_ROW:
						ev.RowEvent.Keyspace = ""
						ev.RowEvent.Shard = ""
					}
					ev.Keyspace = ""
					ev.Shard = ""
					got := ev.String()
					want := expectedEvents[i]
					switch want {
					case "begin", "commit", "gtid":
						want = fmt.Sprintf("type:%s", strings.ToUpper(want))
					default:
						want = env.RemoveAnyDeprecatedDisplayWidths(want)
					}
					if !strings.HasPrefix(got, want) {
						errGoroutine = fmt.Errorf("event %d did not match, want %s, got %s", i, want, got)
						return errGoroutine
					}
				}

				return io.EOF
			}
			return nil
		})
	}()
	wg.Wait()
	if errGoroutine != nil {
		t.Fatalf(errGoroutine.Error())
	}
}

// TestFilteredVarBinary confirms that adding a filter using a varbinary column results in the correct set of events.
func TestFilteredVarBinary(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id1 int, val varbinary(128), primary key(id1))",
		},
		options: &TestSpecOptions{
			filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select id1, val from t1 where val = 'newton'",
				}},
			},
		},
	}
	defer ts.Close()
	ts.Init()
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 'kepler')", noEvents},
		{"insert into t1 values (2, 'newton')", nil},
		{"insert into t1 values (3, 'newton')", nil},
		{"insert into t1 values (4, 'kepler')", noEvents},
		{"insert into t1 values (5, 'newton')", nil},
		{"update t1 set val = 'newton' where id1 = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"1", "newton"}}}}},
		}},
		{"update t1 set val = 'kepler' where id1 = 2", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"2", "newton"}}}}},
		}},
		{"update t1 set val = 'newton' where id1 = 2", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"2", "newton"}}}}},
		}},
		{"update t1 set val = 'kepler' where id1 = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"1", "newton"}}}}},
		}},
		{"delete from t1 where id1 in (2,3)", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"2", "newton"}}, {before: []string{"3", "newton"}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()
}

// TestFilteredInt confirms that adding a filter using an int column results in the correct set of events.
func TestFilteredInt(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
		},
		options: &TestSpecOptions{
			filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select id1, val from t1 where id2 = 200",
				}},
			},
		},
	}
	defer ts.Close()
	ts.Init()
	ts.fieldEvents["t1"].cols[1].skip = true
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 100, 'aaa')", noEvents},
		{"insert into t1 values (2, 200, 'bbb')", nil},
		{"insert into t1 values (3, 100, 'ccc')", noEvents},
		{"insert into t1 values (4, 200, 'ddd')", nil},
		{"insert into t1 values (5, 200, 'eee')", nil},
		{"update t1 set val = 'newddd' where id1 = 4", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"4", "ddd"}, after: []string{"4", "newddd"}}}}},
		}},
		{"update t1 set id2 = 200 where id1 = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"1", "aaa"}}}}},
		}},
		{"update t1 set id2 = 100 where id1 = 2", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"2", "bbb"}}}}},
		}},
		{"update t1 set id2 = 100 where id1 = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"1", "aaa"}}}}},
		}},
		{"update t1 set id2 = 200 where id1 = 2", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"2", "bbb"}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()
}

// TestSavepoint confirms that rolling back to a savepoint drops the dmls that were executed during the savepoint.
func TestSavepoint(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table stream1(id int, val varbinary(128), primary key(id))",
		},
	}
	defer ts.Close()
	ts.Init()
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into stream1 values (1, 'aaa')", nil},
		{"savepoint a", noEvents},
		{"insert into stream1 values (2, 'aaa')", noEvents},
		{"rollback work to savepoint a", noEvents},
		{"savepoint b", noEvents},
		{"update stream1 set val='bbb' where id = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "stream1", changes: []TestRowChange{{before: []string{"1", "aaa"}, after: []string{"1", "bbb"}}}}},
		}},
		{"release savepoint b", noEvents},
		{"commit", nil},
	}}
	ts.Run()
}

// TestSavepointWithFilter tests that using savepoints with both filtered and unfiltered tables works as expected.
func TestSavepointWithFilter(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table stream1(id int, val varbinary(128), primary key(id))",
			"create table stream2(id int, val varbinary(128), primary key(id))",
		},
		options: &TestSpecOptions{
			filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "stream2",
					Filter: "select * from stream2",
				}},
			},
		},
	}
	defer ts.Close()
	ts.Init()
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into stream1 values (1, 'aaa')", noEvents},
		{"savepoint a", noEvents},
		{"insert into stream1 values (2, 'aaa')", noEvents},
		{"savepoint b", noEvents},
		{"insert into stream1 values (3, 'aaa')", noEvents},
		{"savepoint c", noEvents},
		{"insert into stream1 values (4, 'aaa')", noEvents},
		{"savepoint d", noEvents},
		{"commit", nil},
	}, {
		{"begin", nil},
		{"insert into stream1 values (5, 'aaa')", noEvents},
		{"savepoint d", noEvents},
		{"insert into stream1 values (6, 'aaa')", noEvents},
		{"savepoint c", noEvents},
		{"insert into stream1 values (7, 'aaa')", noEvents},
		{"savepoint b", noEvents},
		{"insert into stream1 values (8, 'aaa')", noEvents},
		{"savepoint a", noEvents},
		{"commit", nil},
	}, {
		{"begin", nil},
		{"insert into stream1 values (9, 'aaa')", noEvents},
		{"savepoint a", noEvents},
		{"insert into stream2 values (1, 'aaa')", nil},
		{"savepoint b", noEvents},
		{"insert into stream1 values (10, 'aaa')", noEvents},
		{"savepoint c", noEvents},
		{"insert into stream2 values (2, 'aaa')", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "stream2", changes: []TestRowChange{{after: []string{"2", "aaa"}}}}},
		}},
		{"savepoint d", noEvents},
		{"commit", nil},
	}}
	ts.Run()
}

func TestStatements(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table stream1(id int, val varbinary(128), primary key(id))",
			"create table stream2(id int, val varbinary(128), primary key(id))",
		},
	}
	defer ts.Close()
	ts.Init()
	fe := &TestFieldEvent{
		table: "stream1",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "val", dataType: "VARBINARY", colType: "varbinary(256)", len: 256, collationID: 63},
		},
	}
	ddlAlterWithPrefixAndSuffix := "/* prefix */ alter table stream1 change column val val varbinary(256) /* suffix */"
	ddlTruncate := "truncate table stream2"
	ddlReverseAlter := "/* prefix */ alter table stream1 change column val val varbinary(128) /* suffix */"
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into stream1 values (1, 'aaa')", nil},
		{"update stream1 set val='bbb' where id = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "stream1", changes: []TestRowChange{{before: []string{"1", "aaa"}, after: []string{"1", "bbb"}}}}},
		}},
		{"commit", nil},
	}, { // Normal DDL.
		{"alter table stream1 change column val val varbinary(128)", nil},
	}, { // DDL padded with comments.
		{ddlAlterWithPrefixAndSuffix, []TestRowEvent{
			{event: "gtid"},
			{event: ts.getDDLEvent(ddlAlterWithPrefixAndSuffix)},
		}},
	}, { // Multiple tables, and multiple rows changed per statement.
		{"begin", nil},
		{"insert into stream1 values (2, 'bbb')", []TestRowEvent{
			{event: fe.String()},
			{spec: &TestRowEventSpec{table: "stream1", changes: []TestRowChange{{after: []string{"2", "bbb"}}}}},
		}},
		{"insert into stream2 values (1, 'aaa')", nil},
		{"update stream1 set val='ccc'", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "stream1", changes: []TestRowChange{{before: []string{"1", "bbb"}, after: []string{"1", "ccc"}}, {before: []string{"2", "bbb"}, after: []string{"2", "ccc"}}}}},
		}},
		{"delete from stream1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "stream1", changes: []TestRowChange{{before: []string{"1", "ccc"}}, {before: []string{"2", "ccc"}}}}},
		}},
		{"commit", nil},
	}, {
		{ddlTruncate, []TestRowEvent{
			{event: "gtid"},
			{event: ts.getDDLEvent(ddlTruncate)},
		}},
	}, {
		{ddlReverseAlter, []TestRowEvent{
			{event: "gtid"},
			{event: ts.getDDLEvent(ddlReverseAlter)},
		}},
	}}
	ts.Run()

	ts.Reset()
	// Test FilePos flavor
	savedEngine := engine
	defer func() { engine = savedEngine }()
	engine = customEngine(t, func(in mysql.ConnParams) mysql.ConnParams {
		in.Flavor = "FilePos"
		return in
	})
	defer engine.Close()
	ts.Run()
}

// TestOther tests "other" and "priv" statements. These statements can
// produce very different events depending on the version of mysql or
// mariadb. So, we just show that vreplication transmits "OTHER" events
// if the binlog is affected by the statement.
func TestOther(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table stream1(id int, val varbinary(128), primary key(id))",
			"create table stream2(id int, val varbinary(128), primary key(id))",
		},
	}
	ts.Init()
	defer ts.Close()

	testcases := []string{
		"repair table stream2",
		"optimize table stream2",
		"analyze table stream2",
		"select * from stream1",
		"set @val=1",
		"show tables",
		"describe stream1",
		"grant select on stream1 to current_user()",
		"revoke select on stream1 from current_user()",
	}

	// customRun is a modified version of runCases.
	customRun := func(mode string) {
		t.Logf("Run mode: %v", mode)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg, ch := startStream(ctx, t, nil, "", nil)
		defer wg.Wait()
		want := [][]string{{
			`gtid`,
			`type:OTHER`,
		}}

		for _, stmt := range testcases {
			startPosition := primaryPosition(t)
			execStatement(t, stmt)
			endPosition := primaryPosition(t)
			if startPosition == endPosition {
				t.Logf("statement %s did not affect binlog", stmt)
				continue
			}
			expectLog(ctx, t, stmt, ch, want)
		}
		cancel()
		if evs, ok := <-ch; ok {
			t.Fatalf("unexpected evs: %v", evs)
		}
	}
	customRun("gtid")

	// Test FilePos flavor
	savedEngine := engine
	defer func() { engine = savedEngine }()
	engine = customEngine(t, func(in mysql.ConnParams) mysql.ConnParams {
		in.Flavor = "FilePos"
		return in
	})
	defer engine.Close()
	customRun("filePos")
}

// TestRegexp tests a filter which has a regexp suffix.
func TestRegexp(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table yes_stream(id int, val varbinary(128), primary key(id))",
			"create table no_stream(id int, val varbinary(128), primary key(id))",
		},
		options: &TestSpecOptions{
			filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match: "/yes.*/",
				}},
			},
		},
	}
	defer ts.Close()

	ts.Init()

	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into yes_stream values (1, 'aaa')", nil},
		{"insert into no_stream values (2, 'bbb')", noEvents},
		{"update yes_stream set val='bbb' where id = 1", nil},
		{"update no_stream set val='bbb' where id = 2", noEvents},
		{"commit", nil},
	}}
	ts.Run()
}

func TestREKeyRange(t *testing.T) {
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "/.*/",
			Filter: "-80",
		}},
	}
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
		},
		options: &TestSpecOptions{
			filter: filter,
		},
	}
	ignoreKeyspaceShardInFieldAndRowEvents = false
	defer func() {
		ignoreKeyspaceShardInFieldAndRowEvents = true
	}()
	ts.Init()
	defer ts.Close()

	setVSchema(t, shardedVSchema)
	defer env.SetVSchema("{}")

	// 1, 2, 3 and 5 are in shard -80.
	// 4 and 6 are in shard 80-.
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 1, 'aaa')", nil},
		{"insert into t1 values (4, 1, 'bbb')", noEvents},
		{"update t1 set id1 = 2 where id1 = 1", []TestRowEvent{ // Stays in shard.
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"1", "1", "aaa"}, after: []string{"2", "1", "aaa"}}}}},
		}},
		{"update t1 set id1 = 6 where id1 = 2", []TestRowEvent{ // Moves from -80 to 80-.
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"2", "1", "aaa"}}}}},
		}},
		{"update t1 set id1 = 3 where id1 = 4", []TestRowEvent{ // Moves from 80- back to -80.
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"3", "1", "bbb"}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()

	// Switch the vschema to make id2 the primary vindex.
	altVSchema := `{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "hash"
        }
      ]
    }
  }
}`
	setVSchema(t, altVSchema)
	ts.Reset()
	// Only the first insert should be sent.
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (4, 1, 'aaa')", nil},
		{"insert into t1 values (1, 4, 'aaa')", noEvents},
		{"commit", nil},
	}}
	ts.Init()
	ts.Run()
}

func TestInKeyRangeMultiColumn(t *testing.T) {
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id, region, val, keyspace_id() from t1 where in_keyrange('-80')",
		}},
	}
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(region int, id int, val varbinary(128), primary key(id))",
		},
		options: &TestSpecOptions{
			filter: filter,
		},
	}
	ts.Init()
	defer ts.Close()

	setVSchema(t, multicolumnVSchema)
	defer env.SetVSchema("{}")

	fe := &TestFieldEvent{
		table: "t1",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "region", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "val", dataType: "VARBINARY", colType: "varbinary(128)", len: 128, collationID: 63},
			{name: "keyspace_id", dataType: "VARBINARY", colType: "varbinary(256)", len: 256, collationID: 63},
		},
	}

	// 1 and 2 are in shard -80.
	// 128 is in shard 80-.
	keyspaceId1 := "\x01\x16k@\xb4J\xbaK\xd6"
	keyspaceId2 := "\x02\x16k@\xb4J\xbaK\xd6"
	keyspaceId3 := "\x01\x06\xe7\xea\"Βp\x8f"
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 1, 'aaa')", []TestRowEvent{
			{event: fe.String()},
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"1", "1", "aaa", keyspaceId1}}}}},
		}},
		{"insert into t1 values (128, 2, 'bbb')", noEvents},
		{"update t1 set region = 2 where id = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"1", "1", "aaa", keyspaceId1}, after: []string{"1", "2", "aaa", keyspaceId2}}}}},
		}},
		{"update t1 set region = 128 where id = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"1", "2", "aaa", keyspaceId2}}}}},
		}},
		{"update t1 set region = 1 where id = 2", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"2", "1", "bbb", keyspaceId3}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()
}

func TestREMultiColumnVindex(t *testing.T) {
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "/.*/",
			Filter: "-80",
		}},
	}
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(region int, id int, val varbinary(128), primary key(id))",
		},
		options: &TestSpecOptions{
			filter: filter,
		},
	}
	ts.Init()
	defer ts.Close()

	setVSchema(t, multicolumnVSchema)
	defer env.SetVSchema("{}")
	// (region, id) is the primary vindex.
	// (1,1), (1, 2)  are in shard -80.
	// (128, 1) (128, 2) are in shard 80-.
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 1, 'aaa')", nil},
		{"insert into t1 values (128, 2, 'bbb')", noEvents},
		{"update t1 set region = 2 where id = 1", nil},
		{"update t1 set region = 128 where id = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"2", "1", "aaa"}}}}},
		}},
		{"update t1 set region = 1 where id = 2", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"1", "2", "bbb"}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()
}

// TestSelectFilter tests a filter with an in_keyrange function, used in a sharded keyspace.
func TestSelectFilter(t *testing.T) {
	fe := &TestFieldEvent{
		table: "t1",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id2", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "val", dataType: "VARBINARY", colType: "varbinary(128)", len: 128, collationID: 63},
		},
	}
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
		},
		options: &TestSpecOptions{
			filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select id2, val from t1 where in_keyrange(id2, 'hash', '-80')",
				}},
			},
		},
	}
	defer ts.Close()

	ts.Init()

	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (4, 1, 'aaa')", []TestRowEvent{
			{event: fe.String()},
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"1", "aaa"}}}}},
		}},
		{"insert into t1 values (2, 4, 'aaa')", noEvents}, // not in keyrange
		{"commit", nil},
	}}
	ts.Run()
}

func TestDDLAddColumn(t *testing.T) {
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "ddl_test2",
			Filter: "select * from ddl_test2",
		}, {
			Match: "/.*/",
		}},
	}

	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table ddl_test1(id int, val1 varbinary(128), primary key(id))",
			"create table ddl_test2(id int, val1 varbinary(128), primary key(id))",
		},
		options: &TestSpecOptions{
			// Test RE as well as select-based filters.
			filter: filter,
		},
	}
	defer ts.Close()
	// Record position before the next few statements.
	ts.Init()
	pos := primaryPosition(t)
	ts.SetStartPosition(pos)
	alterTest1 := "alter table ddl_test1 add column val2 varbinary(128)"
	alterTest2 := "alter table ddl_test2 add column val2 varbinary(128)"
	fe1 := &TestFieldEvent{
		table: "ddl_test1",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "val1", dataType: "VARBINARY", colType: "varbinary(128)", len: 128, collationID: 63},
			{name: "val2", dataType: "VARBINARY", colType: "varbinary(128)", len: 128, collationID: 63},
		},
	}
	fe2 := &TestFieldEvent{
		table: "ddl_test2",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "val1", dataType: "VARBINARY", colType: "varbinary(128)", len: 128, collationID: 63},
			{name: "val2", dataType: "VARBINARY", colType: "varbinary(128)", len: 128, collationID: 63},
		},
	}
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into ddl_test1 values(1, 'aaa')", nil},
		{"insert into ddl_test2 values(1, 'aaa')", nil},
		{"commit", nil},
	}, {
		// Adding columns is allowed.
		{alterTest1, []TestRowEvent{
			{event: "gtid"},
			{event: ts.getDDLEvent(alterTest1)},
		}},
	}, {
		{alterTest2, []TestRowEvent{
			{event: "gtid"},
			{event: ts.getDDLEvent(alterTest2)},
		}},
	}, {
		{"begin", nil},
		{"insert into ddl_test1 values(2, 'bbb', 'ccc')", []TestRowEvent{
			{event: fe1.String()},
			{spec: &TestRowEventSpec{table: "ddl_test1", changes: []TestRowChange{{after: []string{"2", "bbb", "ccc"}}}}},
		}},
		{"insert into ddl_test2 values(2, 'bbb', 'ccc')", []TestRowEvent{
			{event: fe2.String()},
			{spec: &TestRowEventSpec{table: "ddl_test2", changes: []TestRowChange{{after: []string{"2", "bbb", "ccc"}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()
}

func TestDDLDropColumn(t *testing.T) {
	execStatement(t, "create table ddl_test2(id int, val1 varbinary(128), val2 varbinary(128), primary key(id))")
	defer execStatement(t, "drop table ddl_test2")

	// Record position before the next few statements.
	pos := primaryPosition(t)
	execStatements(t, []string{
		"insert into ddl_test2 values(1, 'aaa', 'ccc')",
		// Adding columns is allowed.
		"alter table ddl_test2 drop column val2",
		"insert into ddl_test2 values(2, 'bbb')",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		for range ch {
		}
	}()
	defer close(ch)
	err := vstream(ctx, t, pos, nil, nil, ch)
	want := "cannot determine table columns"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}
}

func TestUnsentDDL(t *testing.T) {
	execStatement(t, "create table unsent(id int, val varbinary(128), primary key(id))")

	testcases := []testcase{{
		input: []string{
			"drop table unsent",
		},
		// An unsent DDL is sent as an empty transaction.
		output: [][]string{{
			`gtid`,
			`type:OTHER`,
		}},
	}}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/none/",
		}},
	}
	runCases(t, filter, testcases, "", nil)
}

func TestBuffering(t *testing.T) {
	reset := AdjustPacketSize(10)
	defer reset()

	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table packet_test(id int, val varbinary(128), primary key(id))",
		},
	}
	defer ts.Close()
	ts.Init()
	ddl := "alter table packet_test change val val varchar(128)"
	ts.tests = [][]*TestQuery{{
		// All rows in one packet.
		{"begin", nil},
		{"insert into packet_test values (1, '123')", nil},
		{"insert into packet_test values (2, '456')", nil},
		{"commit", nil},
	}, {
		// A new row causes packet size to be exceeded.
		// Also test deletes
		{"begin", nil},
		{"insert into packet_test values (3, '123456')", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "packet_test", changes: []TestRowChange{{after: []string{"3", "123456"}}}}},
		}},
		{"insert into packet_test values (4, '789012')", []TestRowEvent{
			{restart: true, spec: &TestRowEventSpec{table: "packet_test", changes: []TestRowChange{{after: []string{"4", "789012"}}}}},
		}},
		{"delete from packet_test where id=3", []TestRowEvent{
			{restart: true, spec: &TestRowEventSpec{table: "packet_test", changes: []TestRowChange{{before: []string{"3", "123456"}}}}},
		}},
		{"delete from packet_test where id=4", []TestRowEvent{
			{restart: true, spec: &TestRowEventSpec{table: "packet_test", changes: []TestRowChange{{before: []string{"4", "789012"}}}}},
		}},
		{"commit", nil},
	}, {
		// A single row is itself bigger than the packet size.
		{"begin", nil},
		{"insert into packet_test values (5, '123456')", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "packet_test", changes: []TestRowChange{{after: []string{"5", "123456"}}}}},
		}},
		{"insert into packet_test values (6, '12345678901')", []TestRowEvent{
			{restart: true, spec: &TestRowEventSpec{table: "packet_test", changes: []TestRowChange{{after: []string{"6", "12345678901"}}}}},
		}},
		{"insert into packet_test values (7, '23456')", []TestRowEvent{
			{restart: true, spec: &TestRowEventSpec{table: "packet_test", changes: []TestRowChange{{after: []string{"7", "23456"}}}}},
		}},
		{"commit", nil},
	}, {
		// An update packet is bigger because it has a before and after image.
		{"begin", nil},
		{"insert into packet_test values (8, '123')", nil},
		{"update packet_test set val='456' where id=8", []TestRowEvent{
			{restart: true, spec: &TestRowEventSpec{table: "packet_test", changes: []TestRowChange{{before: []string{"8", "123"}, after: []string{"8", "456"}}}}},
		}},
		{"commit", nil},
	}, {
		// DDL is in its own packet.
		{ddl, []TestRowEvent{
			{event: "gtid"},
			{event: ts.getDDLEvent(ddl)},
		}},
	}}
	ts.Run()
}

// TestBestEffortNameInFieldEvent tests that we make a valid best effort
// attempt to deduce the type and collation in the event of table renames.
// In both cases the varbinary becomes a varchar. We get the correct
// collation information, however, in the binlog_row_metadata in 8.0 but
// not in 5.7. So in 5.7 our best effort uses varchar with its default
// collation for text fields.
func TestBestEffortNameInFieldEvent(t *testing.T) {
	bestEffortCollation := collations.ID(collations.CollationBinaryID)
	if strings.HasPrefix(testenv.MySQLVersion, "5.7") {
		bestEffortCollation = testenv.DefaultCollationID
	}
	filter := &binlogdatapb.Filter{
		FieldEventMode: binlogdatapb.Filter_BEST_EFFORT,
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	// Modeled after vttablet endtoend compatibility tests.
	execStatements(t, []string{
		"create table vitess_test(id int, val varbinary(128), primary key(id)) ENGINE=InnoDB CHARSET=utf8mb4",
	})
	position := primaryPosition(t)
	execStatements(t, []string{
		"insert into vitess_test values(1, 'abc')",
		"rename table vitess_test to vitess_test_new",
	})

	defer execStatements(t, []string{
		"drop table vitess_test_new",
	})
	testcases := []testcase{{
		input: []string{
			"insert into vitess_test_new values(2, 'abc')",
		},
		// In this case, we don't have information about vitess_test since it was renamed to vitess_test_test.
		// information returned by binlog for val column == varchar (rather than varbinary).
		output: [][]string{{
			`begin`,
			fmt.Sprintf(`type:FIELD field_event:{table_name:"vitess_test" fields:{name:"@1" type:INT32 charset:63} fields:{name:"@2" type:VARCHAR charset:%d}}`, bestEffortCollation),
			`type:ROW row_event:{table_name:"vitess_test" row_changes:{after:{lengths:1 lengths:3 values:"1abc"}}}`,
			`gtid`,
			`commit`,
		}, {
			`gtid`,
			`type:DDL statement:"rename table vitess_test to vitess_test_new"`,
		}, {
			`begin`,
			`type:FIELD field_event:{table_name:"vitess_test_new" fields:{name:"id" type:INT32 table:"vitess_test_new" org_table:"vitess_test_new" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"vitess_test_new" org_table:"vitess_test_new" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"vitess_test_new" row_changes:{after:{lengths:1 lengths:3 values:"2abc"}}}`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, position, nil)
}

// todo: migrate to new framework
// test that vstreamer ignores tables created by OnlineDDL
func TestInternalTables(t *testing.T) {
	if version.GoOS == "darwin" {
		t.Skip("internal online ddl table matching doesn't work on Mac because it is case insensitive")
	}
	filter := &binlogdatapb.Filter{
		FieldEventMode: binlogdatapb.Filter_BEST_EFFORT,
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	// Modeled after vttablet endtoend compatibility tests.
	execStatements(t, []string{
		"create table vitess_test(id int, val varbinary(128), primary key(id))",
		"create table _1e275eef_3b20_11eb_a38f_04ed332e05c2_20201210204529_gho(id int, val varbinary(128), primary key(id))",
		"create table _vt_PURGE_1f9194b43b2011eb8a0104ed332e05c2_20201210194431(id int, val varbinary(128), primary key(id))",
		"create table _product_old(id int, val varbinary(128), primary key(id))",
	})
	position := primaryPosition(t)
	execStatements(t, []string{
		"insert into vitess_test values(1, 'abc')",
		"insert into _1e275eef_3b20_11eb_a38f_04ed332e05c2_20201210204529_gho values(1, 'abc')",
		"insert into _vt_PURGE_1f9194b43b2011eb8a0104ed332e05c2_20201210194431 values(1, 'abc')",
		"insert into _product_old values(1, 'abc')",
	})

	defer execStatements(t, []string{
		"drop table vitess_test",
		"drop table _1e275eef_3b20_11eb_a38f_04ed332e05c2_20201210204529_gho",
		"drop table _vt_PURGE_1f9194b43b2011eb8a0104ed332e05c2_20201210194431",
		"drop table _product_old",
	})
	testcases := []testcase{{
		input: []string{
			"insert into vitess_test values(2, 'abc')",
		},
		// In this case, we don't have information about vitess_test since it was renamed to vitess_test_test.
		// information returned by binlog for val column == varchar (rather than varbinary).
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"vitess_test" fields:{name:"id" type:INT32 table:"vitess_test" org_table:"vitess_test" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"vitess_test" org_table:"vitess_test" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"vitess_test" row_changes:{after:{lengths:1 lengths:3 values:"1abc"}}}`,
			`gtid`,
			`commit`,
		}, {`begin`, `gtid`, `commit`}, {`begin`, `gtid`, `commit`}, {`begin`, `gtid`, `commit`}, // => inserts into the three internal comments
			{
				`begin`,
				`type:ROW row_event:{table_name:"vitess_test" row_changes:{after:{lengths:1 lengths:3 values:"2abc"}}}`,
				`gtid`,
				`commit`,
			}},
	}}
	runCases(t, filter, testcases, position, nil)
}

func TestTypes(t *testing.T) {
	// Modeled after vttablet endtoend compatibility tests.
	execStatements(t, []string{
		"create table vitess_ints(tiny tinyint, tinyu tinyint unsigned, small smallint, smallu smallint unsigned, medium mediumint, mediumu mediumint unsigned, normal int, normalu int unsigned, big bigint, bigu bigint unsigned, y year, primary key(tiny)) ENGINE=InnoDB CHARSET=utf8mb4",
		"create table vitess_fracts(id int, deci decimal(5,2), num numeric(5,2), f float, d double, primary key(id)) ENGINE=InnoDB CHARSET=utf8mb4",
		"create table vitess_strings(vb varbinary(16), c char(16), vc varchar(16), b binary(4), tb tinyblob, bl blob, ttx tinytext, tx text, en enum('a','b'), s set('a','b'), primary key(vb)) ENGINE=InnoDB CHARSET=utf8mb4",
		"create table vitess_misc(id int, b bit(8), d date, dt datetime, t time, g geometry, primary key(id)) ENGINE=InnoDB CHARSET=utf8mb4",
		"create table vitess_null(id int, val varbinary(128), primary key(id)) ENGINE=InnoDB CHARSET=utf8mb4",
		"create table vitess_decimal(id int, dec1 decimal(12,4), dec2 decimal(13,4), primary key(id)) ENGINE=InnoDB CHARSET=utf8mb4",
	})
	defer execStatements(t, []string{
		"drop table vitess_ints",
		"drop table vitess_fracts",
		"drop table vitess_strings",
		"drop table vitess_misc",
		"drop table vitess_null",
		"drop table vitess_decimal",
	})

	testcases := []testcase{{
		input: []string{
			"insert into vitess_ints values(-128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 2012)",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"vitess_ints" fields:{name:"tiny" type:INT8 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"tiny" column_length:4 charset:63 column_type:"tinyint(4)"} fields:{name:"tinyu" type:UINT8 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"tinyu" column_length:3 charset:63 column_type:"tinyint(3) unsigned"} fields:{name:"small" type:INT16 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"small" column_length:6 charset:63 column_type:"smallint(6)"} fields:{name:"smallu" type:UINT16 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"smallu" column_length:5 charset:63 column_type:"smallint(5) unsigned"} fields:{name:"medium" type:INT24 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"medium" column_length:9 charset:63 column_type:"mediumint(9)"} fields:{name:"mediumu" type:UINT24 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"mediumu" column_length:8 charset:63 column_type:"mediumint(8) unsigned"} fields:{name:"normal" type:INT32 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"normal" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"normalu" type:UINT32 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"normalu" column_length:10 charset:63 column_type:"int(10) unsigned"} fields:{name:"big" type:INT64 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"big" column_length:20 charset:63 column_type:"bigint(20)"} fields:{name:"bigu" type:UINT64 table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"bigu" column_length:20 charset:63 column_type:"bigint(20) unsigned"} fields:{name:"y" type:YEAR table:"vitess_ints" org_table:"vitess_ints" database:"vttest" org_name:"y" column_length:4 charset:63 column_type:"year(4)"}}`,
			`type:ROW row_event:{table_name:"vitess_ints" row_changes:{after:{lengths:4 lengths:3 lengths:6 lengths:5 lengths:8 lengths:8 lengths:11 lengths:10 lengths:20 lengths:20 lengths:4 values:"` +
				`-128` +
				`255` +
				`-32768` +
				`65535` +
				`-8388608` +
				`16777215` +
				`-2147483648` +
				`4294967295` +
				`-9223372036854775808` +
				`18446744073709551615` +
				`2012` +
				`"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		input: []string{
			"insert into vitess_fracts values(1, 1.99, 2.99, 3.99, 4.99)",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"vitess_fracts" fields:{name:"id" type:INT32 table:"vitess_fracts" org_table:"vitess_fracts" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"deci" type:DECIMAL table:"vitess_fracts" org_table:"vitess_fracts" database:"vttest" org_name:"deci" column_length:7 charset:63 decimals:2 column_type:"decimal(5,2)"} fields:{name:"num" type:DECIMAL table:"vitess_fracts" org_table:"vitess_fracts" database:"vttest" org_name:"num" column_length:7 charset:63 decimals:2 column_type:"decimal(5,2)"} fields:{name:"f" type:FLOAT32 table:"vitess_fracts" org_table:"vitess_fracts" database:"vttest" org_name:"f" column_length:12 charset:63 decimals:31 column_type:"float"} fields:{name:"d" type:FLOAT64 table:"vitess_fracts" org_table:"vitess_fracts" database:"vttest" org_name:"d" column_length:22 charset:63 decimals:31 column_type:"double"}}`,
			`type:ROW row_event:{table_name:"vitess_fracts" row_changes:{after:{lengths:1 lengths:4 lengths:4 lengths:8 lengths:8 values:"11.992.993.99E+004.99E+00"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		// TODO(sougou): validate that binary and char data generate correct DMLs on the other end.
		input: []string{
			"insert into vitess_strings values('a', 'b', 'c', 'd\000\000\000', 'e', 'f', 'g', 'h', 'a', 'a,b')",
		},
		output: [][]string{{
			`begin`,
			fmt.Sprintf(`type:FIELD field_event:{table_name:"vitess_strings" fields:{name:"vb" type:VARBINARY table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"vb" column_length:16 charset:63 column_type:"varbinary(16)"} fields:{name:"c" type:CHAR table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"c" column_length:64 charset:%d column_type:"char(16)"} fields:{name:"vc" type:VARCHAR table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"vc" column_length:64 charset:%d column_type:"varchar(16)"} fields:{name:"b" type:BINARY table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"b" column_length:4 charset:63 column_type:"binary(4)"} fields:{name:"tb" type:BLOB table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"tb" column_length:255 charset:63 column_type:"tinyblob"} fields:{name:"bl" type:BLOB table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"bl" column_length:65535 charset:63 column_type:"blob"} fields:{name:"ttx" type:TEXT table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"ttx" column_length:1020 charset:%d column_type:"tinytext"} fields:{name:"tx" type:TEXT table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"tx" column_length:262140 charset:%d column_type:"text"} fields:{name:"en" type:ENUM table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"en" column_length:4 charset:%d column_type:"enum('a','b')"} fields:{name:"s" type:SET table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"s" column_length:12 charset:%d column_type:"set('a','b')"} enum_set_string_values:true}`, testenv.DefaultCollationID, testenv.DefaultCollationID, testenv.DefaultCollationID, testenv.DefaultCollationID, testenv.DefaultCollationID, testenv.DefaultCollationID),
			`type:ROW row_event:{table_name:"vitess_strings" row_changes:{after:{lengths:1 lengths:1 lengths:1 lengths:4 lengths:1 lengths:1 lengths:1 lengths:1 lengths:1 lengths:3 ` +
				`values:"abcd\x00\x00\x00efghaa,b"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		// TODO(sougou): validate that the geometry value generates the correct DMLs on the other end.
		input: []string{
			"insert into vitess_misc values(1, '\x01', '2012-01-01', '2012-01-01 15:45:45', '15:45:45', point(1, 2))",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"vitess_misc" fields:{name:"id" type:INT32 table:"vitess_misc" org_table:"vitess_misc" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"b" type:BIT table:"vitess_misc" org_table:"vitess_misc" database:"vttest" org_name:"b" column_length:8 charset:63 column_type:"bit(8)"} fields:{name:"d" type:DATE table:"vitess_misc" org_table:"vitess_misc" database:"vttest" org_name:"d" column_length:10 charset:63 column_type:"date"} fields:{name:"dt" type:DATETIME table:"vitess_misc" org_table:"vitess_misc" database:"vttest" org_name:"dt" column_length:19 charset:63 column_type:"datetime"} fields:{name:"t" type:TIME table:"vitess_misc" org_table:"vitess_misc" database:"vttest" org_name:"t" column_length:10 charset:63 column_type:"time"} fields:{name:"g" type:GEOMETRY table:"vitess_misc" org_table:"vitess_misc" database:"vttest" org_name:"g" column_length:4294967295 charset:63 column_type:"geometry"}}`,
			`type:ROW row_event:{table_name:"vitess_misc" row_changes:{after:{lengths:1 lengths:1 lengths:10 lengths:19 lengths:8 lengths:25 values:"1\x012012-01-012012-01-01 15:45:4515:45:45\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		input: []string{
			"insert into vitess_null values(1, null)",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"vitess_null" fields:{name:"id" type:INT32 table:"vitess_null" org_table:"vitess_null" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"vitess_null" org_table:"vitess_null" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"vitess_null" row_changes:{after:{lengths:1 lengths:-1 values:"1"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		input: []string{
			"insert into vitess_decimal values(1, 1.23, 1.23)",
			"insert into vitess_decimal values(2, -1.23, -1.23)",
			"insert into vitess_decimal values(3, 0000000001.23, 0000000001.23)",
			"insert into vitess_decimal values(4, -0000000001.23, -0000000001.23)",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"vitess_decimal" fields:{name:"id" type:INT32 table:"vitess_decimal" org_table:"vitess_decimal" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"dec1" type:DECIMAL table:"vitess_decimal" org_table:"vitess_decimal" database:"vttest" org_name:"dec1" column_length:14 charset:63 decimals:4 column_type:"decimal(12,4)"} fields:{name:"dec2" type:DECIMAL table:"vitess_decimal" org_table:"vitess_decimal" database:"vttest" org_name:"dec2" column_length:15 charset:63 decimals:4 column_type:"decimal(13,4)"}}`,
			`type:ROW row_event:{table_name:"vitess_decimal" row_changes:{after:{lengths:1 lengths:6 lengths:6 values:"11.23001.2300"}}}`,
			`gtid`,
			`commit`,
		}, {
			`begin`,
			`type:ROW row_event:{table_name:"vitess_decimal" row_changes:{after:{lengths:1 lengths:7 lengths:7 values:"2-1.2300-1.2300"}}}`,
			`gtid`,
			`commit`,
		}, {
			`begin`,
			`type:ROW row_event:{table_name:"vitess_decimal" row_changes:{after:{lengths:1 lengths:6 lengths:6 values:"31.23001.2300"}}}`,
			`gtid`,
			`commit`,
		}, {
			`begin`,
			`type:ROW row_event:{table_name:"vitess_decimal" row_changes:{after:{lengths:1 lengths:7 lengths:7 values:"4-1.2300-1.2300"}}}`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "", nil)
}

func TestJSON(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table vitess_json(id int default 1, val json, primary key(id))",
		},
	}
	ts.Init()
	defer ts.Close()
	ts.tests = [][]*TestQuery{}
	queries := []*TestQuery{}
	jsonValues := []string{"{}", "123456", `"vtTablet"`, `{"foo": "bar"}`, `["abc", 3.14, true]`}
	queries = append(queries, &TestQuery{"begin", nil})
	for i, val := range jsonValues {
		queries = append(queries, &TestQuery{fmt.Sprintf("insert into vitess_json values(%d, %s)", i+1, encodeString(val)), nil})
	}
	queries = append(queries, &TestQuery{"commit", nil})

	ts.tests = append(ts.tests, queries)
	ts.Run()
}

func TestExternalTable(t *testing.T) {
	execStatements(t, []string{
		"create database external",
		"create table external.ext(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop database external",
	})

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into external.ext values (1, 'aaa')",
			"commit",
		},
		// External table events don't get sent.
		output: [][]string{{
			`begin`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "", nil)
}

func TestJournal(t *testing.T) {
	execStatements(t, []string{
		"create table if not exists _vt.resharding_journal(id int, db_name varchar(128), val blob, primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table _vt.resharding_journal",
	})

	journal1 := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_SHARDS,
	}
	journal2 := &binlogdatapb.Journal{
		Id:            2,
		MigrationType: binlogdatapb.MigrationType_SHARDS,
	}
	testcases := []testcase{{
		input: []string{
			"begin",
			fmt.Sprintf("insert into _vt.resharding_journal values(1, 'vttest', '%v')", journal1.String()),
			fmt.Sprintf("insert into _vt.resharding_journal values(2, 'nosend', '%v')", journal2.String()),
			"commit",
		},
		// External table events don't get sent.
		output: [][]string{{
			`begin`,
			`type:JOURNAL journal:{id:1 migration_type:SHARDS}`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "", nil)
}

// TestMinimalMode confirms that we don't support minimal binlog_row_image mode.
func TestMinimalMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldEngine := engine
	engine = nil
	oldEnv := env
	env = nil
	newEngine(t, ctx, "minimal")
	defer func() {
		if engine != nil {
			engine.Close()
		}
		if env != nil {
			env.Close()
		}
		engine = oldEngine
		env = oldEnv
	}()
	err := engine.Stream(context.Background(), "current", nil, nil, throttlerapp.VStreamerName, func(evs []*binlogdatapb.VEvent) error { return nil })
	require.Error(t, err, "minimal binlog_row_image is not supported by Vitess VReplication")
}

func TestStatementMode(t *testing.T) {
	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
		"create table stream2(id int, val varbinary(128), primary key(id))",
	})

	defer execStatements(t, []string{
		"drop table stream1",
		"drop table stream2",
	})

	testcases := []testcase{{
		input: []string{
			"set @@session.binlog_format='STATEMENT'",
			"begin",
			"insert into stream1 values (1, 'aaa')",
			"update stream1 set val='bbb' where id = 1",
			"delete from stream1 where id = 1",
			"commit",
			"set @@session.binlog_format='ROW'",
		},
		output: [][]string{{
			`begin`,
			`type:INSERT dml:"insert into stream1 values (1, 'aaa')"`,
			`type:UPDATE dml:"update stream1 set val='bbb' where id = 1"`,
			`type:DELETE dml:"delete from stream1 where id = 1"`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "", nil)
}

func TestHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg, ch := startStream(ctx, t, nil, "", nil)
	defer wg.Wait()
	evs := <-ch
	require.Equal(t, 1, len(evs))
	assert.Equal(t, binlogdatapb.VEventType_HEARTBEAT, evs[0].Type)
	cancel()
}

func TestNoFutureGTID(t *testing.T) {
	// Execute something to make sure we have ranges in GTIDs.
	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table stream1",
	})

	pos := primaryPosition(t)
	t.Logf("current position: %v", pos)
	// Both mysql and mariadb have '-' in their gtids.
	// Invent a GTID in the future.
	index := strings.LastIndexByte(pos, '-')
	num, err := strconv.Atoi(pos[index+1:])
	require.NoError(t, err)
	future := pos[:index+1] + fmt.Sprintf("%d", num+1)
	t.Logf("future position: %v", future)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		for range ch {
		}
	}()
	defer close(ch)
	err = vstream(ctx, t, future, nil, nil, ch)
	want := "GTIDSet Mismatch"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}
}

func TestFilteredMultipleWhere(t *testing.T) {
	fe := &TestFieldEvent{
		table: "t1",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id1", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "val", dataType: "VARBINARY", colType: "varbinary(128)", len: 128, collationID: 63},
		},
	}
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id1 int, id2 int, id3 int, val varbinary(128), primary key(id1))",
		},
		options: &TestSpecOptions{
			filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select id1, val from t1 where in_keyrange('-80') and id2 = 200 and id3 = 1000 and val = 'newton'",
				}},
			},
			customFieldEvents: true,
		},
	}
	_ = fe
	defer ts.Close() // Ensure clean-up

	ts.Init()

	setVSchema(t, shardedVSchema)
	defer env.SetVSchema("{}")

	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 100, 1000, 'kepler')", noEvents},
		{"insert into t1 values (2, 200, 1000, 'newton')", []TestRowEvent{
			{event: fe.String()},
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"2", "newton"}}}}},
		}},
		{"insert into t1 values (3, 100, 2000, 'kepler')", noEvents},
		{"insert into t1 values (128, 200, 1000, 'newton')", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"128", "newton"}}}}},
		}},
		{"insert into t1 values (5, 200, 2000, 'kepler')", noEvents},
		{"insert into t1 values (129, 200, 1000, 'kepler')", noEvents},
		{"commit", nil},
	}}
	ts.Run()
}

// TestGeneratedColumns just confirms that generated columns are sent in a vstream as expected
func TestGeneratedColumns(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id int, val varbinary(6), val2 varbinary(6) as (concat(id, val)), val3 varbinary(6) as (concat(val, id)), id2 int, primary key(id))",
		},
		options: &TestSpecOptions{
			customFieldEvents: true,
		},
	}
	defer ts.Close()

	ts.Init()

	fe := &TestFieldEvent{
		table: "t1",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "id", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "val", dataType: "VARBINARY", colType: "varbinary(6)", len: 6, collationID: 63},
			{name: "val2", dataType: "VARBINARY", colType: "varbinary(6)", len: 6, collationID: 63},
			{name: "val3", dataType: "VARBINARY", colType: "varbinary(6)", len: 6, collationID: 63},
			{name: "id2", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
		},
	}

	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1(id, val, id2) values (1, 'aaa', 10)", []TestRowEvent{
			{event: fe.String()},
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"1", "aaa", "1aaa", "aaa1", "10"}}}}},
		}},
		{"insert into t1(id, val, id2) values (2, 'bbb', 20)", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"2", "bbb", "2bbb", "bbb2", "20"}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()
}

// TestGeneratedInvisiblePrimaryKey validates that generated invisible primary keys are sent in row events.
func TestGeneratedInvisiblePrimaryKey(t *testing.T) {
	if !env.HasCapability(testenv.ServerCapabilityGeneratedInvisiblePrimaryKey) {
		t.Skip("skipping test as server does not support generated invisible primary keys")
	}

	execStatement(t, "SET @@session.sql_generate_invisible_primary_key=ON")
	defer execStatement(t, "SET @@session.sql_generate_invisible_primary_key=OFF")
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(val varbinary(6))",
		},
		options: nil,
	}
	defer ts.Close()

	ts.Init()

	fe := &TestFieldEvent{
		table: "t1",
		db:    testenv.DBName,
		cols: []*TestColumn{
			{name: "my_row_id", dataType: "UINT64", colType: "bigint unsigned", len: 20, collationID: 63},
			{name: "val", dataType: "VARBINARY", colType: "varbinary(6)", len: 6, collationID: 63},
		},
	}

	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values ('aaa')", []TestRowEvent{
			{event: fe.String()},
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{after: []string{"1", "aaa"}}}}},
		}},
		{"update t1 set val = 'bbb' where my_row_id = 1", []TestRowEvent{
			{spec: &TestRowEventSpec{table: "t1", changes: []TestRowChange{{before: []string{"1", "aaa"}, after: []string{"1", "bbb"}}}}},
		}},
		{"commit", nil},
	}}
	ts.Run()
}
