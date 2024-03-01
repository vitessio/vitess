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
	"vitess.io/vitess/go/mysql/replication"
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

func (tfe *TestFieldEvent) String() string {
	s := fmt.Sprintf("type:FIELD field_event:{table_name:\"%s\"", tfe.table)
	fld := ""
	for _, col := range tfe.cols {
		if col.skip {
			continue
		}
		fld += fmt.Sprintf(" fields:{name:\"%s\" type:%s table:\"%s\" org_table:\"%s\" database:\"%s\" org_name:\"%s\" column_length:%d charset:%d",
			col.name, col.dataType, tfe.table, tfe.table, tfe.db, col.name, col.len, col.collationID)
		if col.colType != "" {
			fld += fmt.Sprintf(" column_type:\"%s\"", col.colType)
		}
		fld += "}"
	}
	s += fld
	s += "}"
	return s
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
	require.NoError(t, ts.Init())
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
			"create table t1(id int, val binary(4), color set('red','green','blue'), size enum('S','M','L'), primary key(id))",
		},
	}
	defer ts.Close()
	require.NoError(t, ts.Init())
	ts.tests = [][]*TestQuery{{
		{"begin", nil},
		{"insert into t1 values (1, 'aaa', 'red,blue', 'S')", nil},
		{"insert into t1 values (2, 'bbb', 'green', 'M')", nil},
		{"insert into t1 values (3, 'ccc', 'red,blue,green', 'L')", nil},
		{"commit", nil},
	}}
	ts.Run()
}

// TestCellValuePadding tests that the events are correctly padded for binary columns.
func TestCellValuePadding(t *testing.T) {
	ts := &TestSpec{
		t: t,
		ddls: []string{
			"create table t1(id int, val binary(4), primary key(val))",
			"create table t2(id int, val char(4), primary key(val))",
			"create table t3(id int, val char(4) collate utf8mb4_bin, primary key(val))",
		},
	}
	defer ts.Close()
	require.NoError(t, ts.Init())
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
	require.NoError(t, ts.Init())
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

func TestSetStatement(t *testing.T) {
	if !checkIfOptionIsSupported(t, "log_builtin_as_identified_by_password") {
		// the combination of setting this option and support for "set password" only works on a few flavors
		log.Info("Cannot test SetStatement on this flavor")
		return
	}
	engine.se.Reload(context.Background())

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())
	queries := []string{
		"begin",
		"insert into t1 values (1, 'aaa')",
		"commit",
		"set global log_builtin_as_identified_by_password=1",
		"SET PASSWORD FOR 'vt_appdebug'@'localhost'='*AA17DA66C7C714557F5485E84BCAFF2C209F2F53'", //select password('vtappdebug_password');
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
	require.NoError(t, ts.Init())
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

	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())
	queries := []string{
		"begin",
		"insert into t1 values (1, 'aaa')",
		"commit",
		"/*!40000 ALTER TABLE `t1` DISABLE KEYS */",
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

func TestVersion(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	oldEngine := engine
	defer func() {
		engine = oldEngine
	}()

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
	engine.se.Reload(context.Background())
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
	mt, err := env.SchemaEngine.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid)
	require.NoError(t, err)
	assert.True(t, proto.Equal(mt, dbSchema.Tables[0]))
}

func insertLotsOfData(t *testing.T, numRows int) {
	query1 := "insert into t1 (id11, id12) values"
	s := ""
	for i := 1; i <= numRows; i++ {
		if s != "" {
			s += ","
		}
		s += fmt.Sprintf("(%d,%d)", i, i*10)
	}
	query1 += s
	query2 := "insert into t2 (id21, id22) values"
	s = ""
	for i := 1; i <= numRows; i++ {
		if s != "" {
			s += ","
		}
		s += fmt.Sprintf("(%d,%d)", i, i*20)
	}
	query2 += s
	execStatements(t, []string{
		query1,
		query2,
	})
}

func TestMissingTables(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	engine.se.Reload(context.Background())
	execStatements(t, []string{
		"create table t1(id11 int, id12 int, primary key(id11))",
		"create table shortlived(id31 int, id32 int, primary key(id31))",
	})
	defer execStatements(t, []string{
		"drop table t1",
		"drop table _shortlived",
	})
	startPos := primaryPosition(t)
	execStatements(t, []string{
		"insert into shortlived values (1,1), (2,2)",
		"alter table shortlived rename to _shortlived",
	})
	engine.se.Reload(context.Background())
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}},
	}
	testcases := []testcase{
		{
			input:  []string{},
			output: [][]string{},
		},

		{
			input: []string{
				"insert into t1 values (101, 1010)",
			},
			output: [][]string{
				{
					"begin",
					"gtid",
					"commit",
				},
				{
					"gtid",
					"type:OTHER",
				},
				{
					"begin",
					"type:FIELD field_event:{table_name:\"t1\" fields:{name:\"id11\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id11\" column_length:11 charset:63 column_type:\"int(11)\"} fields:{name:\"id12\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id12\" column_length:11 charset:63 column_type:\"int(11)\"}}",
					"type:ROW row_event:{table_name:\"t1\" row_changes:{after:{lengths:3 lengths:4 values:\"1011010\"}}}",
					"gtid",
					"commit",
				},
			},
		},
	}
	runCases(t, filter, testcases, startPos, nil)
}

func TestVStreamCopySimpleFlow(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	execStatements(t, []string{
		"create table t1(id11 int, id12 int, primary key(id11))",
		"create table t2(id21 int, id22 int, primary key(id21))",
	})
	log.Infof("Pos before bulk insert: %s", primaryPosition(t))
	insertLotsOfData(t, 10)
	log.Infof("Pos after bulk insert: %s", primaryPosition(t))
	defer execStatements(t, []string{
		"drop table t1",
		"drop table t2",
	})
	engine.se.Reload(context.Background())
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

	t1FieldEvent := []string{"begin", "type:FIELD field_event:{table_name:\"t1\" fields:{name:\"id11\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id11\" column_length:11 charset:63 column_type:\"int(11)\"} fields:{name:\"id12\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id12\" column_length:11 charset:63 column_type:\"int(11)\"}}"}
	t2FieldEvent := []string{"begin", "type:FIELD field_event:{table_name:\"t2\" fields:{name:\"id21\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id21\" column_length:11 charset:63 column_type:\"int(11)\"} fields:{name:\"id22\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id22\" column_length:11 charset:63 column_type:\"int(11)\"}}"}
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

	insertEvents1 := []string{
		"begin",
		"type:FIELD field_event:{table_name:\"t1\" fields:{name:\"id11\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id11\" column_length:11 charset:63 column_type:\"int(11)\"} fields:{name:\"id12\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id12\" column_length:11 charset:63 column_type:\"int(11)\"}}",
		"type:ROW row_event:{table_name:\"t1\" row_changes:{after:{lengths:3 lengths:4 values:\"1011010\"}}}",
		"gtid",
		"commit"}
	insertEvents2 := []string{
		"begin",
		"type:FIELD field_event:{table_name:\"t2\" fields:{name:\"id21\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id21\" column_length:11 charset:63 column_type:\"int(11)\"} fields:{name:\"id22\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id22\" column_length:11 charset:63 column_type:\"int(11)\"}}",
		"type:ROW row_event:{table_name:\"t2\" row_changes:{after:{lengths:3 lengths:4 values:\"2022020\"}}}",
		"gtid",
		"commit"}

	testcases := []testcase{
		{
			input:  []string{},
			output: [][]string{t1FieldEvent, {"gtid"}, t1Events, {"begin", "lastpk", "commit"}, t2FieldEvent, t2Events, {"begin", "lastpk", "commit"}, {"copy_completed"}},
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
	if testing.Short() {
		t.Skip()
	}
	execStatements(t, []string{
		"create table t1(id1 int, id2 int, id3 int, primary key(id1)) ENGINE=InnoDB CHARSET=utf8mb4",
		"create table t2a(id1 int, id2 int, primary key(id1)) ENGINE=InnoDB CHARSET=utf8mb4",
		"create table t2b(id1 varchar(20), id2 int, primary key(id1)) ENGINE=InnoDB CHARSET=utf8mb4",
	})
	defer execStatements(t, []string{
		"drop table t1",
		"drop table t2a",
		"drop table t2b",
	})
	engine.se.Reload(context.Background())
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

	execStatements(t, []string{
		"insert into t1(id1, id2, id3) values (1, 2, 3)",
		"insert into t2a(id1, id2) values (1, 4)",
		"insert into t2b(id1, id2) values ('b', 6)",
		"insert into t2b(id1, id2) values ('a', 5)",
	})

	var expectedEvents = []string{
		"type:BEGIN",
		"type:FIELD field_event:{table_name:\"t1\" fields:{name:\"id1\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id1\" column_length:11 charset:63 column_type:\"int(11)\"} fields:{name:\"id2\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id2\" column_length:11 charset:63 column_type:\"int(11)\"}}",
		"type:GTID",
		"type:ROW row_event:{table_name:\"t1\" row_changes:{after:{lengths:1 lengths:1 values:\"12\"}}}",
		"type:LASTPK last_p_k_event:{table_last_p_k:{table_name:\"t1\" lastpk:{fields:{name:\"id1\" type:INT32 charset:63 flags:53251} rows:{lengths:1 values:\"1\"}}}}",
		"type:COMMIT",
		"type:BEGIN",
		"type:LASTPK last_p_k_event:{table_last_p_k:{table_name:\"t1\"} completed:true}",
		"type:COMMIT",
		"type:BEGIN",
		"type:FIELD field_event:{table_name:\"t2a\" fields:{name:\"id1\" type:INT32 table:\"t2a\" org_table:\"t2a\" database:\"vttest\" org_name:\"id1\" column_length:11 charset:63 column_type:\"int(11)\"} fields:{name:\"id2\" type:INT32 table:\"t2a\" org_table:\"t2a\" database:\"vttest\" org_name:\"id2\" column_length:11 charset:63 column_type:\"int(11)\"}}",
		"type:ROW row_event:{table_name:\"t2a\" row_changes:{after:{lengths:1 lengths:1 values:\"14\"}}}",
		"type:LASTPK last_p_k_event:{table_last_p_k:{table_name:\"t2a\" lastpk:{fields:{name:\"id1\" type:INT32 charset:63 flags:53251} rows:{lengths:1 values:\"1\"}}}}",
		"type:COMMIT",
		"type:BEGIN",
		"type:LASTPK last_p_k_event:{table_last_p_k:{table_name:\"t2a\"} completed:true}",
		"type:COMMIT",
		"type:BEGIN",
		fmt.Sprintf("type:FIELD field_event:{table_name:\"t2b\" fields:{name:\"id1\" type:VARCHAR table:\"t2b\" org_table:\"t2b\" database:\"vttest\" org_name:\"id1\" column_length:80 charset:%d column_type:\"varchar(20)\"} fields:{name:\"id2\" type:INT32 table:\"t2b\" org_table:\"t2b\" database:\"vttest\" org_name:\"id2\" column_length:11 charset:63 column_type:\"int(11)\"}}", testenv.DefaultCollationID),
		"type:ROW row_event:{table_name:\"t2b\" row_changes:{after:{lengths:1 lengths:1 values:\"a5\"}}}",
		"type:ROW row_event:{table_name:\"t2b\" row_changes:{after:{lengths:1 lengths:1 values:\"b6\"}}}",
		fmt.Sprintf("type:LASTPK last_p_k_event:{table_last_p_k:{table_name:\"t2b\" lastpk:{fields:{name:\"id1\" type:VARCHAR charset:%d flags:20483} rows:{lengths:1 values:\"b\"}}}}", testenv.DefaultCollationID),
		"type:COMMIT",
		"type:BEGIN",
		"type:LASTPK last_p_k_event:{table_last_p_k:{table_name:\"t2b\"} completed:true}",
		"type:COMMIT",
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
					if ev.Type == binlogdatapb.VEventType_FIELD {
						for j := range ev.FieldEvent.Fields {
							ev.FieldEvent.Fields[j].Flags = 0
						}
						ev.FieldEvent.Keyspace = ""
						ev.FieldEvent.Shard = ""
					}
					if ev.Type == binlogdatapb.VEventType_ROW {
						ev.RowEvent.Keyspace = ""
						ev.RowEvent.Shard = ""
					}
					got := ev.String()
					want := expectedEvents[i]

					want = env.RemoveAnyDeprecatedDisplayWidths(want)

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
	require.NoError(t, ts.Init())
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
	require.NoError(t, ts.Init())
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
	require.NoError(t, ts.Init())
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
	require.NoError(t, ts.Init())
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
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
		"create table stream2(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table stream1",
		"drop table stream2",
	})
	engine.se.Reload(context.Background())

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into stream1 values (1, 'aaa')",
			"update stream1 set val='bbb' where id = 1",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"stream1" fields:{name:"id" type:INT32 table:"stream1" org_table:"stream1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"stream1" org_table:"stream1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"stream1" row_changes:{after:{lengths:1 lengths:3 values:"1aaa"}}}`,
			`type:ROW row_event:{table_name:"stream1" row_changes:{before:{lengths:1 lengths:3 values:"1aaa"} after:{lengths:1 lengths:3 values:"1bbb"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		// Normal DDL.
		input: "alter table stream1 change column val val varbinary(128)",
		output: [][]string{{
			`gtid`,
			`type:DDL statement:"alter table stream1 change column val val varbinary(128)"`,
		}},
	}, {
		// DDL padded with comments.
		input: " /* prefix */ alter table stream1 change column val val varbinary(256) /* suffix */ ",
		output: [][]string{{
			`gtid`,
			`type:DDL statement:"/* prefix */ alter table stream1 change column val val varbinary(256) /* suffix */"`,
		}},
	}, {
		// Multiple tables, and multiple rows changed per statement.
		input: []string{
			"begin",
			"insert into stream1 values (2, 'bbb')",
			"insert into stream2 values (1, 'aaa')",
			"update stream1 set val='ccc'",
			"delete from stream1",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"stream1" fields:{name:"id" type:INT32 table:"stream1" org_table:"stream1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"stream1" org_table:"stream1" database:"vttest" org_name:"val" column_length:256 charset:63 column_type:"varbinary(256)"}}`,
			`type:ROW row_event:{table_name:"stream1" row_changes:{after:{lengths:1 lengths:3 values:"2bbb"}}}`,
			`type:FIELD field_event:{table_name:"stream2" fields:{name:"id" type:INT32 table:"stream2" org_table:"stream2" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"stream2" org_table:"stream2" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"stream2" row_changes:{after:{lengths:1 lengths:3 values:"1aaa"}}}`,
			`type:ROW row_event:{table_name:"stream1" ` +
				`row_changes:{before:{lengths:1 lengths:3 values:"1bbb"} after:{lengths:1 lengths:3 values:"1ccc"}} ` +
				`row_changes:{before:{lengths:1 lengths:3 values:"2bbb"} after:{lengths:1 lengths:3 values:"2ccc"}}}`,
			`type:ROW row_event:{table_name:"stream1" ` +
				`row_changes:{before:{lengths:1 lengths:3 values:"1ccc"}} ` +
				`row_changes:{before:{lengths:1 lengths:3 values:"2ccc"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		// truncate is a DDL
		input: "truncate table stream2",
		output: [][]string{{
			`gtid`,
			`type:DDL statement:"truncate table stream2"`,
		}},
	}, {
		// Reverse alter table, else FilePos tests fail
		input: " /* prefix */ alter table stream1 change column val val varbinary(128) /* suffix */ ",
		output: [][]string{{
			`gtid`,
			`type:DDL statement:"/* prefix */ alter table stream1 change column val val varbinary(128) /* suffix */"`,
		}},
	}}
	runCases(t, nil, testcases, "current", nil)
	// Test FilePos flavor
	savedEngine := engine
	defer func() { engine = savedEngine }()
	engine = customEngine(t, func(in mysql.ConnParams) mysql.ConnParams {
		in.Flavor = "FilePos"
		return in
	})

	defer engine.Close()
	runCases(t, nil, testcases, "current", nil)
}

// TestOther tests "other" and "priv" statements. These statements can
// produce very different events depending on the version of mysql or
// mariadb. So, we just show that vreplication transmits "OTHER" events
// if the binlog is affected by the statement.
func TestOther(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
		"create table stream2(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table stream1",
		"drop table stream2",
	})
	engine.se.Reload(context.Background())

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

func TestRegexp(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table yes_stream(id int, val varbinary(128), primary key(id))",
		"create table no_stream(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table yes_stream",
		"drop table no_stream",
	})
	engine.se.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/yes.*/",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into yes_stream values (1, 'aaa')",
			"insert into no_stream values (2, 'bbb')",
			"update yes_stream set val='bbb' where id = 1",
			"update no_stream set val='bbb' where id = 2",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"yes_stream" fields:{name:"id" type:INT32 table:"yes_stream" org_table:"yes_stream" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"yes_stream" org_table:"yes_stream" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"yes_stream" row_changes:{after:{lengths:1 lengths:3 values:"1aaa"}}}`,
			`type:ROW row_event:{table_name:"yes_stream" row_changes:{before:{lengths:1 lengths:3 values:"1aaa"} after:{lengths:1 lengths:3 values:"1bbb"}}}`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, "", nil)
}

func TestREKeyRange(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ignoreKeyspaceShardInFieldAndRowEvents = false
	defer func() {
		ignoreKeyspaceShardInFieldAndRowEvents = true
	}()
	// Needed for this test to run if run standalone
	engine.watcherOnce.Do(engine.setWatch)

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	setVSchema(t, shardedVSchema)
	defer env.SetVSchema("{}")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "/.*/",
			Filter: "-80",
		}},
	}
	wg, ch := startStream(ctx, t, filter, "", nil)
	defer wg.Wait()
	// 1, 2, 3 and 5 are in shard -80.
	// 4 and 6 are in shard 80-.
	input := []string{
		"begin",
		"insert into t1 values (1, 4, 'aaa')",
		"insert into t1 values (4, 1, 'bbb')",
		// Stay in shard.
		"update t1 set id1 = 2 where id1 = 1",
		// Move from -80 to 80-.
		"update t1 set id1 = 6 where id1 = 2",
		// Move from 80- to -80.
		"update t1 set id1 = 3 where id1 = 4",
		"commit",
	}
	execStatements(t, input)
	expectLog(ctx, t, input, ch, [][]string{{
		`begin`,
		`type:FIELD field_event:{table_name:"t1" fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} keyspace:"vttest" shard:"0"}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:1 lengths:3 values:"14aaa"}} keyspace:"vttest" shard:"0"}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{before:{lengths:1 lengths:1 lengths:3 values:"14aaa"} after:{lengths:1 lengths:1 lengths:3 values:"24aaa"}} keyspace:"vttest" shard:"0"}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{before:{lengths:1 lengths:1 lengths:3 values:"24aaa"}} keyspace:"vttest" shard:"0"}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:1 lengths:3 values:"31bbb"}} keyspace:"vttest" shard:"0"}`,
		`gtid`,
		`commit`,
	}})

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

	// Only the first insert should be sent.
	input = []string{
		"begin",
		"insert into t1 values (4, 1, 'aaa')",
		"insert into t1 values (1, 4, 'aaa')",
		"commit",
	}
	execStatements(t, input)
	expectLog(ctx, t, input, ch, [][]string{{
		`begin`,
		`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:1 lengths:3 values:"41aaa"}} keyspace:"vttest" shard:"0"}`,
		`gtid`,
		`commit`,
	}})
	cancel()
}

func TestInKeyRangeMultiColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	engine.watcherOnce.Do(engine.setWatch)
	engine.se.Reload(context.Background())

	execStatements(t, []string{
		"create table t1(region int, id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	setVSchema(t, multicolumnVSchema)
	defer env.SetVSchema("{}")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id, region, val, keyspace_id() from t1 where in_keyrange('-80')",
		}},
	}
	wg, ch := startStream(ctx, t, filter, "", nil)
	defer wg.Wait()

	// 1, 2, 3 and 5 are in shard -80.
	// 4 and 6 are in shard 80-.
	input := []string{
		"begin",
		"insert into t1 values (1, 1, 'aaa')",
		"insert into t1 values (128, 2, 'bbb')",
		// Stay in shard.
		"update t1 set region = 2 where id = 1",
		// Move from -80 to 80-.
		"update t1 set region = 128 where id = 1",
		// Move from 80- to -80.
		"update t1 set region = 1 where id = 2",
		"commit",
	}
	execStatements(t, input)
	expectLog(ctx, t, input, ch, [][]string{{
		`begin`,
		`type:FIELD field_event:{table_name:"t1" fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"region" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"region" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} fields:{name:"keyspace_id" type:VARBINARY charset:63}}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:1 lengths:3 lengths:9 values:"11aaa\x01\x16k@\xb4J\xbaK\xd6"}}}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{before:{lengths:1 lengths:1 lengths:3 lengths:9 values:"11aaa\x01\x16k@\xb4J\xbaK\xd6"} ` +
			`after:{lengths:1 lengths:1 lengths:3 lengths:9 values:"12aaa\x02\x16k@\xb4J\xbaK\xd6"}}}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{before:{lengths:1 lengths:1 lengths:3 lengths:9 values:"12aaa\x02\x16k@\xb4J\xbaK\xd6"}}}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:1 lengths:3 lengths:9 values:"21bbb\x01\x06\xe7\xea\"Βp\x8f"}}}`,
		`gtid`,
		`commit`,
	}})
	cancel()
}

func TestREMultiColumnVindex(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	engine.watcherOnce.Do(engine.setWatch)

	execStatements(t, []string{
		"create table t1(region int, id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	setVSchema(t, multicolumnVSchema)
	defer env.SetVSchema("{}")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "/.*/",
			Filter: "-80",
		}},
	}
	wg, ch := startStream(ctx, t, filter, "", nil)
	defer wg.Wait()

	// 1, 2, 3 and 5 are in shard -80.
	// 4 and 6 are in shard 80-.
	input := []string{
		"begin",
		"insert into t1 values (1, 1, 'aaa')",
		"insert into t1 values (128, 2, 'bbb')",
		// Stay in shard.
		"update t1 set region = 2 where id = 1",
		// Move from -80 to 80-.
		"update t1 set region = 128 where id = 1",
		// Move from 80- to -80.
		"update t1 set region = 1 where id = 2",
		"commit",
	}
	execStatements(t, input)
	expectLog(ctx, t, input, ch, [][]string{{
		`begin`,
		`type:FIELD field_event:{table_name:"t1" fields:{name:"region" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"region" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:1 lengths:3 values:"11aaa"}}}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{before:{lengths:1 lengths:1 lengths:3 values:"11aaa"} after:{lengths:1 lengths:1 lengths:3 values:"21aaa"}}}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{before:{lengths:1 lengths:1 lengths:3 values:"21aaa"}}}`,
		`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:1 lengths:3 values:"12bbb"}}}`,
		`gtid`,
		`commit`,
	}})
	cancel()
}

func TestSelectFilter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	engine.se.Reload(context.Background())

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id2, val from t1 where in_keyrange(id2, 'hash', '-80')",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into t1 values (4, 1, 'aaa')",
			"insert into t1 values (2, 4, 'aaa')",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"t1" fields:{name:"id2" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:3 values:"1aaa"}}}`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, "", nil)
}

func TestDDLAddColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table ddl_test1(id int, val1 varbinary(128), primary key(id))",
		"create table ddl_test2(id int, val1 varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table ddl_test1",
		"drop table ddl_test2",
	})

	// Record position before the next few statements.
	pos := primaryPosition(t)
	execStatements(t, []string{
		"begin",
		"insert into ddl_test1 values(1, 'aaa')",
		"insert into ddl_test2 values(1, 'aaa')",
		"commit",
		// Adding columns is allowed.
		"alter table ddl_test1 add column val2 varbinary(128)",
		"alter table ddl_test2 add column val2 varbinary(128)",
		"begin",
		"insert into ddl_test1 values(2, 'bbb', 'ccc')",
		"insert into ddl_test2 values(2, 'bbb', 'ccc')",
		"commit",
	})
	engine.se.Reload(context.Background())
	env.SchemaEngine.Reload(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test RE as well as select-based filters.
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "ddl_test2",
			Filter: "select * from ddl_test2",
		}, {
			Match: "/.*/",
		}},
	}

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		defer close(ch)
		if err := vstream(ctx, t, pos, nil, filter, ch); err != nil {
			t.Error(err)
		}
	}()
	expectLog(ctx, t, "ddls", ch, [][]string{{
		// Current schema has 3 columns, but they'll be truncated to match the two columns in the event.
		`begin`,
		`type:FIELD field_event:{table_name:"ddl_test1" fields:{name:"id" type:INT32 table:"ddl_test1" org_table:"ddl_test1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val1" type:VARBINARY table:"ddl_test1" org_table:"ddl_test1" database:"vttest" org_name:"val1" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
		`type:ROW row_event:{table_name:"ddl_test1" row_changes:{after:{lengths:1 lengths:3 values:"1aaa"}}}`,
		`type:FIELD field_event:{table_name:"ddl_test2" fields:{name:"id" type:INT32 table:"ddl_test2" org_table:"ddl_test2" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val1" type:VARBINARY table:"ddl_test2" org_table:"ddl_test2" database:"vttest" org_name:"val1" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
		`type:ROW row_event:{table_name:"ddl_test2" row_changes:{after:{lengths:1 lengths:3 values:"1aaa"}}}`,
		`gtid`,
		`commit`,
	}, {
		`gtid`,
		`type:DDL statement:"alter table ddl_test1 add column val2 varbinary(128)"`,
	}, {
		`gtid`,
		`type:DDL statement:"alter table ddl_test2 add column val2 varbinary(128)"`,
	}, {
		// The plan will be updated to now include the third column
		// because the new table map will have three columns.
		`begin`,
		`type:FIELD field_event:{table_name:"ddl_test1" fields:{name:"id" type:INT32 table:"ddl_test1" org_table:"ddl_test1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val1" type:VARBINARY table:"ddl_test1" org_table:"ddl_test1" database:"vttest" org_name:"val1" column_length:128 charset:63 column_type:"varbinary(128)"} fields:{name:"val2" type:VARBINARY table:"ddl_test1" org_table:"ddl_test1" database:"vttest" org_name:"val2" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
		`type:ROW row_event:{table_name:"ddl_test1" row_changes:{after:{lengths:1 lengths:3 lengths:3 values:"2bbbccc"}}}`,
		`type:FIELD field_event:{table_name:"ddl_test2" fields:{name:"id" type:INT32 table:"ddl_test2" org_table:"ddl_test2" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val1" type:VARBINARY table:"ddl_test2" org_table:"ddl_test2" database:"vttest" org_name:"val1" column_length:128 charset:63 column_type:"varbinary(128)"} fields:{name:"val2" type:VARBINARY table:"ddl_test2" org_table:"ddl_test2" database:"vttest" org_name:"val2" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
		`type:ROW row_event:{table_name:"ddl_test2" row_changes:{after:{lengths:1 lengths:3 lengths:3 values:"2bbbccc"}}}`,
		`gtid`,
		`commit`,
	}})
}

func TestDDLDropColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	env.SchemaEngine.Reload(context.Background())
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
	engine.se.Reload(context.Background())
	env.SchemaEngine.Reload(context.Background())

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
	if testing.Short() {
		t.Skip()
	}

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
	if testing.Short() {
		t.Skip()
	}

	reset := AdjustPacketSize(10)
	defer reset()

	execStatement(t, "create table packet_test(id int, val varbinary(128), primary key(id))")
	defer execStatement(t, "drop table packet_test")
	engine.se.Reload(context.Background())

	testcases := []testcase{{
		// All rows in one packet.
		input: []string{
			"begin",
			"insert into packet_test values (1, '123')",
			"insert into packet_test values (2, '456')",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"packet_test" fields:{name:"id" type:INT32 table:"packet_test" org_table:"packet_test" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"packet_test" org_table:"packet_test" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"packet_test" row_changes:{after:{lengths:1 lengths:3 values:"1123"}}}`,
			`type:ROW row_event:{table_name:"packet_test" row_changes:{after:{lengths:1 lengths:3 values:"2456"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		// A new row causes packet size to be exceeded.
		// Also test deletes
		input: []string{
			"begin",
			"insert into packet_test values (3, '123456')",
			"insert into packet_test values (4, '789012')",
			"delete from packet_test where id=3",
			"delete from packet_test where id=4",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:ROW row_event:{table_name:"packet_test" row_changes:{after:{lengths:1 lengths:6 values:"3123456"}}}`,
		}, {
			`type:ROW row_event:{table_name:"packet_test" row_changes:{after:{lengths:1 lengths:6 values:"4789012"}}}`,
		}, {
			`type:ROW row_event:{table_name:"packet_test" row_changes:{before:{lengths:1 lengths:6 values:"3123456"}}}`,
		}, {
			`type:ROW row_event:{table_name:"packet_test" row_changes:{before:{lengths:1 lengths:6 values:"4789012"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		// A single row is itself bigger than the packet size.
		input: []string{
			"begin",
			"insert into packet_test values (5, '123456')",
			"insert into packet_test values (6, '12345678901')",
			"insert into packet_test values (7, '23456')",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:ROW row_event:{table_name:"packet_test" row_changes:{after:{lengths:1 lengths:6 values:"5123456"}}}`,
		}, {
			`type:ROW row_event:{table_name:"packet_test" row_changes:{after:{lengths:1 lengths:11 values:"612345678901"}}}`,
		}, {
			`type:ROW row_event:{table_name:"packet_test" row_changes:{after:{lengths:1 lengths:5 values:"723456"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		// An update packet is bigger because it has a before and after image.
		input: []string{
			"begin",
			"insert into packet_test values (8, '123')",
			"update packet_test set val='456' where id=8",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:ROW row_event:{table_name:"packet_test" row_changes:{after:{lengths:1 lengths:3 values:"8123"}}}`,
		}, {
			`type:ROW row_event:{table_name:"packet_test" row_changes:{before:{lengths:1 lengths:3 values:"8123"} after:{lengths:1 lengths:3 values:"8456"}}}`,
			`gtid`,
			`commit`,
		}},
	}, {
		// DDL is in its own packet
		input: []string{
			"alter table packet_test change val val varchar(128)",
		},
		output: [][]string{{
			`gtid`,
			`type:DDL statement:"alter table packet_test change val val varchar(128)"`,
		}},
	}}
	runCases(t, nil, testcases, "", nil)
}

// TestBestEffortNameInFieldEvent tests that we make a valid best effort
// attempt to deduce the type and collation in the event of table renames.
// In both cases the varbinary becomes a varchar. We get the correct
// collation information, however, in the binlog_row_metadata in 8.0 but
// not in 5.7. So in 5.7 our best effort uses varchar with its default
// collation for text fields.
func TestBestEffortNameInFieldEvent(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
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
	engine.se.Reload(context.Background())
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

// test that vstreamer ignores tables created by OnlineDDL
func TestInternalTables(t *testing.T) {
	if version.GoOS == "darwin" {
		t.Skip("internal online ddl table matching doesn't work on Mac because it is case insensitive")
	}
	if testing.Short() {
		t.Skip()
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
	engine.se.Reload(context.Background())
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
	if testing.Short() {
		t.Skip()
	}

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
	engine.se.Reload(context.Background())

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
			fmt.Sprintf(`type:FIELD field_event:{table_name:"vitess_strings" fields:{name:"vb" type:VARBINARY table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"vb" column_length:16 charset:63 column_type:"varbinary(16)"} fields:{name:"c" type:CHAR table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"c" column_length:64 charset:%d column_type:"char(16)"} fields:{name:"vc" type:VARCHAR table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"vc" column_length:64 charset:%d column_type:"varchar(16)"} fields:{name:"b" type:BINARY table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"b" column_length:4 charset:63 column_type:"binary(4)"} fields:{name:"tb" type:BLOB table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"tb" column_length:255 charset:63 column_type:"tinyblob"} fields:{name:"bl" type:BLOB table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"bl" column_length:65535 charset:63 column_type:"blob"} fields:{name:"ttx" type:TEXT table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"ttx" column_length:1020 charset:%d column_type:"tinytext"} fields:{name:"tx" type:TEXT table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"tx" column_length:262140 charset:%d column_type:"text"} fields:{name:"en" type:ENUM table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"en" column_length:4 charset:%d column_type:"enum('a','b')"} fields:{name:"s" type:SET table:"vitess_strings" org_table:"vitess_strings" database:"vttest" org_name:"s" column_length:12 charset:%d column_type:"set('a','b')"}}`, testenv.DefaultCollationID, testenv.DefaultCollationID, testenv.DefaultCollationID, testenv.DefaultCollationID, testenv.DefaultCollationID, testenv.DefaultCollationID),
			`type:ROW row_event:{table_name:"vitess_strings" row_changes:{after:{lengths:1 lengths:1 lengths:1 lengths:4 lengths:1 lengths:1 lengths:1 lengths:1 lengths:1 lengths:1 ` +
				`values:"abcd\x00\x00\x00efgh13"}}}`,
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
	if err := env.Mysqld.ExecuteSuperQuery(context.Background(), "create table vitess_json(id int default 1, val json, primary key(id))"); err != nil {
		// If it's a syntax error, MySQL is an older version. Skip this test.
		if strings.Contains(err.Error(), "syntax") {
			return
		}
		t.Fatal(err)
	}
	defer execStatement(t, "drop table vitess_json")
	engine.se.Reload(context.Background())
	jsonValues := []string{"{}", "123456", `"vtTablet"`, `{"foo": "bar"}`, `["abc", 3.14, true]`}

	var inputs, outputs []string
	var outputsArray [][]string
	fieldAdded := false
	var expect = func(in string) string {
		return strings.ReplaceAll(in, "\"", "\\\"")
	}
	for i, val := range jsonValues {
		inputs = append(inputs, fmt.Sprintf("insert into vitess_json values(%d, %s)", i+1, encodeString(val)))

		outputs = []string{}
		outputs = append(outputs, `begin`)
		if !fieldAdded {
			outputs = append(outputs, `type:FIELD field_event:{table_name:"vitess_json" fields:{name:"id" type:INT32 table:"vitess_json" org_table:"vitess_json" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:JSON table:"vitess_json" org_table:"vitess_json" database:"vttest" org_name:"val" column_length:4294967295 charset:63 column_type:"json"}}`)
			fieldAdded = true
		}
		out := expect(val)

		outputs = append(outputs, fmt.Sprintf(`type:ROW row_event:{table_name:"vitess_json" row_changes:{after:{lengths:1 lengths:%d values:"%d%s"}}}`,
			len(val), i+1 /*id increments*/, out))
		outputs = append(outputs, `gtid`)
		outputs = append(outputs, `commit`)
		outputsArray = append(outputsArray, outputs)
	}
	testcases := []testcase{{
		input:  inputs,
		output: outputsArray,
	}}
	runCases(t, nil, testcases, "", nil)
}

func TestExternalTable(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create database external",
		"create table external.ext(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop database external",
	})
	engine.se.Reload(context.Background())

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
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table if not exists _vt.resharding_journal(id int, db_name varchar(128), val blob, primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table _vt.resharding_journal",
	})
	engine.se.Reload(context.Background())

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
	if testing.Short() {
		t.Skip()
	}

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
	if testing.Short() {
		t.Skip()
	}
	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
		"create table stream2(id int, val varbinary(128), primary key(id))",
	})

	engine.se.Reload(context.Background())

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
	if testing.Short() {
		t.Skip()
	}

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
	if testing.Short() {
		t.Skip()
	}

	// Execute something to make sure we have ranges in GTIDs.
	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table stream1",
	})
	engine.se.Reload(context.Background())

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
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, id3 int, val varbinary(128), primary key(id1))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	setVSchema(t, shardedVSchema)
	defer env.SetVSchema("{}")

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id1, val from t1 where in_keyrange('-80') and id2 = 200 and id3 = 1000 and val = 'newton'",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into t1 values (1, 100, 1000, 'kepler')",
			"insert into t1 values (2, 200, 1000, 'newton')",
			"insert into t1 values (3, 100, 2000, 'kepler')",
			"insert into t1 values (128, 200, 1000, 'newton')",
			"insert into t1 values (5, 200, 2000, 'kepler')",
			"insert into t1 values (129, 200, 1000, 'kepler')",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:{table_name:"t1" fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"}}`,
			`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:6 values:"2newton"}}}`,
			`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:3 lengths:6 values:"128newton"}}}`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, "", nil)
}

// TestGeneratedColumns just confirms that generated columns are sent in a vstream as expected
func TestGeneratedColumns(t *testing.T) {
	execStatements(t, []string{
		"create table t1(id int, val varbinary(6), val2 varbinary(6) as (concat(id, val)), val3 varbinary(6) as (concat(val, id)), id2 int, primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())
	queries := []string{
		"begin",
		"insert into t1(id, val, id2) values (1, 'aaa', 10)",
		"insert into t1(id, val, id2) values (2, 'bbb', 20)",
		"commit",
	}

	fe := &TestFieldEvent{
		table: "t1",
		db:    "vttest",
		cols: []*TestColumn{
			{name: "id", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
			{name: "val", dataType: "VARBINARY", colType: "varbinary(6)", len: 6, collationID: 63},
			{name: "val2", dataType: "VARBINARY", colType: "varbinary(6)", len: 6, collationID: 63},
			{name: "val3", dataType: "VARBINARY", colType: "varbinary(6)", len: 6, collationID: 63},
			{name: "id2", dataType: "INT32", colType: "int(11)", len: 11, collationID: 63},
		},
	}

	testcases := []testcase{{
		input: queries,
		output: [][]string{{
			`begin`,
			fe.String(),
			`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:3 lengths:4 lengths:4 lengths:2 values:"1aaa1aaaaaa110"}}}`,
			`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:3 lengths:4 lengths:4 lengths:2 values:"2bbb2bbbbbb220"}}}`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "current", nil)
}

// TestGeneratedInvisiblePrimaryKey validates that generated invisible primary keys are sent in row events.
func TestGeneratedInvisiblePrimaryKey(t *testing.T) {
	if !env.HasCapability(testenv.ServerCapabilityGeneratedInvisiblePrimaryKey) {
		t.Skip("skipping test as server does not support generated invisible primary keys")
	}
	execStatements(t, []string{
		"SET @@session.sql_generate_invisible_primary_key=ON;",
		"create table t1(val varbinary(6))",
		"SET @@session.sql_generate_invisible_primary_key=OFF;",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())
	queries := []string{
		"begin",
		"insert into t1 values ('aaa')",
		"update t1 set val = 'bbb' where my_row_id = 1",
		"commit",
	}

	fe := &TestFieldEvent{
		table: "t1",
		db:    "vttest",
		cols: []*TestColumn{
			{name: "my_row_id", dataType: "UINT64", colType: "bigint unsigned", len: 20, collationID: 63},
			{name: "val", dataType: "VARBINARY", colType: "varbinary(6)", len: 6, collationID: 63},
		},
	}

	testcases := []testcase{{
		input: queries,
		output: [][]string{{
			`begin`,
			fe.String(),
			`type:ROW row_event:{table_name:"t1" row_changes:{after:{lengths:1 lengths:3 values:"1aaa"}}}`,
			`type:ROW row_event:{table_name:"t1" row_changes:{before:{lengths:1 lengths:3 values:"1aaa"} after:{lengths:1 lengths:3 values:"1bbb"}}}`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "current", nil)
}

func runCases(t *testing.T, filter *binlogdatapb.Filter, testcases []testcase, position string, tablePK []*binlogdatapb.TableLastPK) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg, ch := startStream(ctx, t, filter, position, tablePK)
	defer wg.Wait()
	// If position is 'current', we wait for a heartbeat to be
	// sure the vstreamer has started.
	if position == "current" {
		log.Infof("Starting stream with current position")
		expectLog(ctx, t, "current pos", ch, [][]string{{`gtid`, `type:OTHER`}})
	}

	log.Infof("Starting to run test cases")
	for _, tcase := range testcases {
		switch input := tcase.input.(type) {
		case []string:
			execStatements(t, input)
		case string:
			execStatement(t, input)
		default:
			t.Fatalf("unexpected input: %#v", input)
		}
		engine.se.Reload(ctx)
		expectLog(ctx, t, tcase.input, ch, tcase.output)
	}

	cancel()
	if evs, ok := <-ch; ok {
		t.Fatalf("unexpected evs: %v", evs)
	}
	log.Infof("Last line of runCases")
}

func expectLog(ctx context.Context, t *testing.T, input any, ch <-chan []*binlogdatapb.VEvent, output [][]string) {
	timer := time.NewTimer(1 * time.Minute)
	defer timer.Stop()
	for _, wantset := range output {
		var evs []*binlogdatapb.VEvent
		for {
			select {
			case allevs, ok := <-ch:
				if !ok {
					t.Fatal("expectLog: not ok, stream ended early")
				}
				for _, ev := range allevs {
					// Ignore spurious heartbeats that can happen on slow machines.
					if ev.Type == binlogdatapb.VEventType_HEARTBEAT {
						continue
					}
					if ev.Throttled {
						continue
					}
					evs = append(evs, ev)
				}
			case <-ctx.Done():
				t.Fatalf("expectLog: Done(), stream ended early")
			case <-timer.C:
				t.Fatalf("expectLog: timed out waiting for events: %v", wantset)
			}
			if len(evs) != 0 {
				break
			}
		}

		numEventsToMatch := len(evs)
		if len(wantset) != len(evs) {
			log.Warningf("%v: evs\n%v, want\n%v, >> got length %d, wanted length %d", input, evs, wantset, len(evs), len(wantset))
			if len(wantset) < len(evs) {
				numEventsToMatch = len(wantset)
			}
		}
		for i := 0; i < numEventsToMatch; i++ {
			want := wantset[i]
			// CurrentTime is not testable.
			evs[i].CurrentTime = 0
			evs[i].Keyspace = ""
			evs[i].Shard = ""
			switch want {
			case "begin":
				if evs[i].Type != binlogdatapb.VEventType_BEGIN {
					t.Fatalf("%v (%d): event: %v, want gtid or begin", input, i, evs[i])
				}
			case "gtid":
				if evs[i].Type != binlogdatapb.VEventType_GTID {
					t.Fatalf("%v (%d): event: %v, want gtid", input, i, evs[i])
				}
			case "lastpk":
				if evs[i].Type != binlogdatapb.VEventType_LASTPK {
					t.Fatalf("%v (%d): event: %v, want lastpk", input, i, evs[i])
				}
			case "commit":
				if evs[i].Type != binlogdatapb.VEventType_COMMIT {
					t.Fatalf("%v (%d): event: %v, want commit", input, i, evs[i])
				}
			case "other":
				if evs[i].Type != binlogdatapb.VEventType_OTHER {
					t.Fatalf("%v (%d): event: %v, want other", input, i, evs[i])
				}
			case "ddl":
				if evs[i].Type != binlogdatapb.VEventType_DDL {
					t.Fatalf("%v (%d): event: %v, want ddl", input, i, evs[i])
				}
			case "copy_completed":
				if evs[i].Type != binlogdatapb.VEventType_COPY_COMPLETED {
					t.Fatalf("%v (%d): event: %v, want copy_completed", input, i, evs[i])
				}
			default:
				evs[i].Timestamp = 0
				if evs[i].Type == binlogdatapb.VEventType_FIELD {
					for j := range evs[i].FieldEvent.Fields {
						evs[i].FieldEvent.Fields[j].Flags = 0
						if ignoreKeyspaceShardInFieldAndRowEvents {
							evs[i].FieldEvent.Keyspace = ""
							evs[i].FieldEvent.Shard = ""
						}
					}
				}
				if ignoreKeyspaceShardInFieldAndRowEvents && evs[i].Type == binlogdatapb.VEventType_ROW {
					evs[i].RowEvent.Keyspace = ""
					evs[i].RowEvent.Shard = ""
				}
				if !testRowEventFlags && evs[i].Type == binlogdatapb.VEventType_ROW {
					evs[i].RowEvent.Flags = 0
				}
				want = env.RemoveAnyDeprecatedDisplayWidths(want)
				if got := fmt.Sprintf("%v", evs[i]); got != want {
					log.Errorf("%v (%d): event:\n%q, want\n%q", input, i, got, want)
					t.Fatalf("%v (%d): event:\n%q, want\n%q", input, i, got, want)
				}
			}
		}
		if len(wantset) != len(evs) {
			t.Fatalf("%v: evs\n%v, want\n%v, got length %d, wanted length %d", input, evs, wantset, len(evs), len(wantset))
		}
	}
}

func startStream(ctx context.Context, t *testing.T, filter *binlogdatapb.Filter, position string, tablePKs []*binlogdatapb.TableLastPK) (*sync.WaitGroup, <-chan []*binlogdatapb.VEvent) {
	switch position {
	case "":
		position = primaryPosition(t)
	case "vscopy":
		position = ""
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan []*binlogdatapb.VEvent)

	go func() {
		defer close(ch)
		defer wg.Done()
		vstream(ctx, t, position, tablePKs, filter, ch)
	}()
	return &wg, ch
}

func vstream(ctx context.Context, t *testing.T, pos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, ch chan []*binlogdatapb.VEvent) error {
	if filter == nil {
		filter = &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "/.*/",
			}},
		}
	}
	return engine.Stream(ctx, pos, tablePKs, filter, throttlerapp.VStreamerName, func(evs []*binlogdatapb.VEvent) error {
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()

		log.Infof("Received events: %v", evs)
		select {
		case ch <- evs:
		case <-ctx.Done():
			return fmt.Errorf("engine.Stream Done() stream ended early")
		case <-timer.C:
			t.Log("VStream timed out waiting for events")
			return io.EOF
		}
		return nil
	})
}

func execStatement(t *testing.T, query string) {
	t.Helper()
	if err := env.Mysqld.ExecuteSuperQuery(context.Background(), query); err != nil {
		t.Fatal(err)
	}
}

func execStatements(t *testing.T, queries []string) {
	if err := env.Mysqld.ExecuteSuperQueryList(context.Background(), queries); err != nil {
		t.Fatal(err)
	}
}

func primaryPosition(t *testing.T) string {
	t.Helper()
	// We use the engine's cp because there is one test that overrides
	// the flavor to FilePos. If so, we have to obtain the position
	// in that flavor format.
	connParam, err := engine.env.Config().DB.DbaWithDB().MysqlParams()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := mysql.Connect(context.Background(), connParam)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pos, err := conn.PrimaryPosition()
	if err != nil {
		t.Fatal(err)
	}
	return replication.EncodePosition(pos)
}

func setVSchema(t *testing.T, vschema string) {
	t.Helper()

	curCount := engine.vschemaUpdates.Get()
	if err := env.SetVSchema(vschema); err != nil {
		t.Fatal(err)
	}
	// Wait for curCount to go up.
	updated := false
	for i := 0; i < 10; i++ {
		if engine.vschemaUpdates.Get() != curCount {
			updated = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !updated {
		log.Infof("vschema did not get updated")
		t.Error("vschema did not get updated")
	}
}
