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
	"regexp"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestRowStreamerQuery validates that the correct force index hint and order by is added to the rowstreamer query.
func TestRowStreamerQuery(t *testing.T) {
	execStatements(t, []string{
		"create table t1(id int, uk1 int, val varbinary(128), primary key(id), unique key uk2 (uk1))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	// We need to StreamRows, to get an initialized RowStreamer.
	// Note that the query passed into StreamRows is overwritten while running the test.
	err := engine.StreamRows(context.Background(), "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		type testCase struct {
			directives      string
			sendQuerySuffix string
		}
		queryTemplate := "select %s id, uk1, val from t1"
		getQuery := func(directives string) string {
			return fmt.Sprintf(queryTemplate, directives)
		}
		sendQueryPrefix := "select /*+ MAX_EXECUTION_TIME(3600000) */ id, uk1, val from t1"
		testCases := []testCase{
			{"", "force index (`PRIMARY`) order by id"},
			{"/*vt+ ukColumns=\"uk1\" ukForce=\"uk2\" */", "force index (`uk2`) order by uk1"},
			{"/*vt+ ukForce=\"uk2\" */", "force index (`uk2`) order by uk1"},
			{"/*vt+ ukColumns=\"uk1\" */", "order by uk1"},
		}

		for _, tc := range testCases {
			t.Run(tc.directives, func(t *testing.T) {
				var err error
				var rs *rowStreamer
				// Depending on the order of the test cases, the index of the engine.rowStreamers slice may change.
				for _, rs2 := range engine.rowStreamers {
					if rs2 != nil {
						rs = rs2
						break
					}
				}
				require.NotNil(t, rs)
				rs.query = getQuery(tc.directives)
				err = rs.buildPlan()
				require.NoError(t, err)
				want := fmt.Sprintf("%s %s", sendQueryPrefix, tc.sendQuerySuffix)
				require.Equal(t, want, rs.sendQuery)
			})
		}
		return nil
	}, nil)
	require.NoError(t, err)
}

func TestStreamRowsScan(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		// Single PK
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, 'aaa'), (2, 'bbb')",
		// Composite PK
		"create table t2(id1 int, id2 int, val varbinary(128), primary key(id1, id2))",
		"insert into t2 values (1, 2, 'aaa'), (1, 3, 'bbb')",
		// No PK
		"create table t3(id int, val varbinary(128))",
		"insert into t3 values (1, 'aaa'), (2, 'bbb')",
		// Three-column PK
		"create table t4(id1 int, id2 int, id3 int, val varbinary(128), primary key(id1, id2, id3))",
		"insert into t4 values (1, 2, 3, 'aaa'), (2, 3, 4, 'bbb')",
		// PK equivalent
		"create table t5(id1 int not null, id2 int not null, id3 int not null, val varbinary(128), unique key id1_id2_id3 (id1, id2, id3))",
		"insert into t5 values (1, 2, 3, 'aaa'), (2, 3, 4, 'bbb')",
	})

	defer execStatements(t, []string{
		"drop table t1",
		"drop table t2",
		"drop table t3",
		"drop table t4",
		"drop table t5",
	})

	// t1: simulates rollup
	wantStream := []string{
		`fields:{name:"1" type:INT64 charset:63} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 values:"1"} rows:{lengths:1 values:"1"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery := "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select 1 from t1", nil, wantQuery, wantStream)

	// t1: simulates rollup, with non-pk column
	wantStream = []string{
		`fields:{name:"1" type:INT64 charset:63} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} rows:{lengths:1 lengths:3 values:"1bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select 1, val from t1", nil, wantQuery, wantStream)

	// t1: simulates rollup, with pk and non-pk column
	wantStream = []string{
		`fields:{name:"1" type:INT64 charset:63} fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:3 values:"11aaa"} rows:{lengths:1 lengths:1 lengths:3 values:"12bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select 1, id, val from t1", nil, wantQuery, wantStream)

	// t1: no pk in select list
	wantStream = []string{
		`fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:3 values:"aaa"} rows:{lengths:3 values:"bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select val from t1", nil, wantQuery, wantStream)

	// t1: all rows
	wantStream = []string{
		`fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} rows:{lengths:1 lengths:3 values:"2bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select * from t1", nil, wantQuery, wantStream)

	// t1: lastpk=1
	wantStream = []string{
		`fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"2bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) where (id > 1) order by id"
	checkStream(t, "select * from t1", []sqltypes.Value{sqltypes.NewInt64(1)}, wantQuery, wantStream)

	// t1: different column ordering
	wantStream = []string{
		`fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:3 lengths:1 values:"aaa1"} rows:{lengths:3 lengths:1 values:"bbb2"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select val, id from t1", nil, wantQuery, wantStream)

	// t2: all rows
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t2" org_table:"t2" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t2" org_table:"t2" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t2" org_table:"t2" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:3 values:"12aaa"} rows:{lengths:1 lengths:1 lengths:3 values:"13bbb"} lastpk:{lengths:1 lengths:1 values:"13"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, val from t2 force index (`PRIMARY`) order by id1, id2"
	checkStream(t, "select * from t2", nil, wantQuery, wantStream)

	// t2: lastpk=1,2
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t2" org_table:"t2" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t2" org_table:"t2" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t2" org_table:"t2" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:3 values:"13bbb"} lastpk:{lengths:1 lengths:1 values:"13"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, val from t2 force index (`PRIMARY`) where (id1 = 1 and id2 > 2) or (id1 > 1) order by id1, id2"
	checkStream(t, "select * from t2", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, wantQuery, wantStream)

	// t3: all rows
	wantStream = []string{
		`fields:{name:"id" type:INT32 table:"t3" org_table:"t3" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t3" org_table:"t3" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63} pkfields:{name:"val" type:VARBINARY charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} rows:{lengths:1 lengths:3 values:"2bbb"} lastpk:{lengths:1 lengths:3 values:"2bbb"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t3 order by id, val"
	checkStream(t, "select * from t3", nil, wantQuery, wantStream)

	// t3: lastpk: 1,'aaa'
	wantStream = []string{
		`fields:{name:"id" type:INT32 table:"t3" org_table:"t3" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t3" org_table:"t3" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63} pkfields:{name:"val" type:VARBINARY charset:63}`,
		`rows:{lengths:1 lengths:3 values:"2bbb"} lastpk:{lengths:1 lengths:3 values:"2bbb"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t3 where (id = 1 and val > _binary'aaa') or (id > 1) order by id, val"
	checkStream(t, "select * from t3", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarBinary("aaa")}, wantQuery, wantStream)

	// t4: all rows
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id3" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id3" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t4" org_table:"t4" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63} pkfields:{name:"id3" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:"123aaa"} rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:"234bbb"} lastpk:{lengths:1 lengths:1 lengths:1 values:"234"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, id3, val from t4 force index (`PRIMARY`) order by id1, id2, id3"
	checkStream(t, "select * from t4", nil, wantQuery, wantStream)

	// t4: lastpk: 1,2,3
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id3" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id3" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t4" org_table:"t4" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63} pkfields:{name:"id3" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:"234bbb"} lastpk:{lengths:1 lengths:1 lengths:1 values:"234"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, id3, val from t4 force index (`PRIMARY`) where (id1 = 1 and id2 = 2 and id3 > 3) or (id1 = 1 and id2 > 2) or (id1 > 1) order by id1, id2, id3"
	checkStream(t, "select * from t4", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(3)}, wantQuery, wantStream)

	// t5: No PK, but a PKE
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t5" org_table:"t5" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t5" org_table:"t5" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id3" type:INT32 table:"t5" org_table:"t5" database:"vttest" org_name:"id3" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t5" org_table:"t5" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63} pkfields:{name:"id3" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:"234bbb"} lastpk:{lengths:1 lengths:1 lengths:1 values:"234"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, id3, val from t5 force index (`id1_id2_id3`) where (id1 = 1 and id2 = 2 and id3 > 3) or (id1 = 1 and id2 > 2) or (id1 > 1) order by id1, id2, id3"
	checkStream(t, "select * from t5", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(3)}, wantQuery, wantStream)

	// t5: test for unsupported integer literal
	wantError := "only the integer literal 1 is supported"
	expectStreamError(t, "select 2 from t5", wantError)

	// t5: test for unsupported literal type
	wantError = "only integer literals are supported"
	expectStreamError(t, "select 'a' from t5", wantError)
}

func TestStreamRowsUnicode(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id int, val varchar(128) COLLATE utf8_unicode_ci, primary key(id))",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	// Use an engine with latin1 charset.
	savedEngine := engine
	defer func() {
		engine = savedEngine
	}()
	engine = customEngine(t, func(in mysql.ConnParams) mysql.ConnParams {
		in.Charset = collations.CollationLatin1Swedish
		return in
	})
	defer engine.Close()
	// We need a latin1 connection.
	conn, err := env.Mysqld.GetDbaConnection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.ExecuteFetch("set names latin1", 10000, false); err != nil {
		t.Fatal(err)
	}
	// This will get "Mojibaked" into the utf8 column.
	if _, err := conn.ExecuteFetch("insert into t1 values(1, '👍')", 10000, false); err != nil {
		t.Fatal(err)
	}

	err = engine.StreamRows(context.Background(), "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		// Skip fields.
		if len(rows.Rows) == 0 {
			return nil
		}
		got := fmt.Sprintf("%q", rows.Rows[0].Values)
		// We should expect a "Mojibaked" version of the string.
		want := `"1ðŸ‘\u008d"`
		if got != want {
			t.Errorf("rows.Rows[0].Values: %s, want %s", got, want)
		}
		return nil
	}, nil)
	require.NoError(t, err)
}

func TestStreamRowsKeyRange(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	defer env.SetVSchema("{}")

	execStatements(t, []string{
		"create table t1(id1 int, val varbinary(128), primary key(id1))",
		"insert into t1 values (1, 'aaa'), (6, 'bbb')",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	// Only the first row should be returned, but lastpk should be 6.
	wantStream := []string{
		`fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} lastpk:{lengths:1 values:"6"}`,
	}
	wantQuery := "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, val from t1 force index (`PRIMARY`) order by id1"
	checkStream(t, "select * from t1 where in_keyrange('-80')", nil, wantQuery, wantStream)
}

func TestStreamRowsFilterInt(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	engine.rowStreamerNumPackets.Reset()
	engine.rowStreamerNumRows.Reset()

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	defer env.SetVSchema("{}")

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
		"insert into t1 values (1, 100, 'aaa'), (2, 200, 'bbb'), (3, 200, 'ccc'), (4, 100, 'ddd'), (5, 200, 'eee')",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	wantStream := []string{
		`fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} rows:{lengths:1 lengths:3 values:"4ddd"} lastpk:{lengths:1 values:"4"}`,
	}
	wantQuery := "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, val from t1 where (id2 = 100) order by id1"
	checkStream(t, "select id1, val from t1 where (id2 = 100)", nil, wantQuery, wantStream)
	require.Equal(t, int64(0), engine.rowStreamerNumPackets.Get())
	require.Equal(t, int64(2), engine.rowStreamerNumRows.Get())
	require.Less(t, int64(0), engine.vstreamerPacketSize.Get())
}

func TestStreamRowsFilterBetween(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	engine.rowStreamerNumPackets.Reset()
	engine.rowStreamerNumRows.Reset()

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	defer env.SetVSchema("{}")

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
		"insert into t1 values (1, 100, 'aaa'), (2, 200, 'bbb'), (3, 200, 'ccc'), (4, 100, 'ddd'), (5, 200, 'eee')",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	// Test for BETWEEN
	wantStream := []string{
		`fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"2bbb"} rows:{lengths:1 lengths:3 values:"3ccc"} rows:{lengths:1 lengths:3 values:"4ddd"} lastpk:{lengths:1 values:"4"}`,
	}
	wantQuery := "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, val from t1 where (id1 between 2 and 4) order by id1"
	checkStream(t, "select id1, val from t1 where (id1 between 2 and 4)", nil, wantQuery, wantStream)
	require.Equal(t, int64(0), engine.rowStreamerNumPackets.Get())
	require.Equal(t, int64(3), engine.rowStreamerNumRows.Get())
	require.NotZero(t, engine.vstreamerPacketSize.Get())

	engine.rowStreamerNumPackets.Reset()
	engine.rowStreamerNumRows.Reset()

	// Test for NOT BETWEEN
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} rows:{lengths:1 lengths:3 values:"5eee"} lastpk:{lengths:1 values:"5"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, val from t1 where (id1 not between 2 and 4) order by id1"
	checkStream(t, "select id1, val from t1 where (id1 not between 2 and 4)", nil, wantQuery, wantStream)
	require.Equal(t, int64(0), engine.rowStreamerNumPackets.Get())
	require.Equal(t, int64(2), engine.rowStreamerNumRows.Get())
	require.NotZero(t, engine.vstreamerPacketSize.Get())
}

func TestStreamRowsFilterIn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	engine.rowStreamerNumPackets.Reset()
	engine.rowStreamerNumRows.Reset()

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	defer env.SetVSchema("{}")

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
		"insert into t1 values (1, 100, 'aaa'), (2, 200, 'bbb'), (3, 200, 'ccc'), (4, 100, 'ddd'), (5, 200, 'eee')",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	wantStream := []string{
		`fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} rows:{lengths:1 lengths:3 values:"2bbb"} rows:{lengths:1 lengths:3 values:"3ccc"} lastpk:{lengths:1 values:"3"}`,
	}
	wantQuery := "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, val from t1 where (id1 in (1, 2, 3)) order by id1"
	checkStream(t, "select id1, val from t1 where id1 in (1, 2, 3)", nil, wantQuery, wantStream)
	require.Equal(t, int64(0), engine.rowStreamerNumPackets.Get())
	require.Equal(t, int64(3), engine.rowStreamerNumRows.Get())
	require.NotZero(t, engine.vstreamerPacketSize.Get())
}

func TestStreamRowsFilterVarBinary(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	defer env.SetVSchema("{}")

	execStatements(t, []string{
		"create table t1(id1 int, val varbinary(128), primary key(id1))",
		"insert into t1 values (1,'kepler'), (2, 'newton'), (3, 'newton'), (4, 'kepler'), (5, 'newton'), (6, 'kepler')",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	wantStream := []string{
		`fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:6 values:"2newton"} rows:{lengths:1 lengths:6 values:"3newton"} rows:{lengths:1 lengths:6 values:"5newton"} lastpk:{lengths:1 values:"5"}`,
	}
	wantQuery := "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, val from t1 where (val = 'newton') order by id1"
	checkStream(t, "select id1, val from t1 where (val = 'newton')", nil, wantQuery, wantStream)
}

func TestStreamRowsFilterVarChar(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	err := env.SetVSchema(shardedVSchema)
	require.NoError(t, err)
	defer env.SetVSchema("{}")

	execStatements(t, []string{
		"create table t1(id1 int, val varchar(128), primary key(id1)) character set utf8mb4 collate utf8mb4_general_ci", // Use general_ci so that we have the same behavior across 5.7 and 8.0
		"insert into t1 values (1,'kepler'), (2, 'Ñewton'), (3, 'nEwton'), (4, 'kepler'), (5, 'neẅton'), (6, 'kepler')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})

	wantStream := []string{
		`fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARCHAR table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:512 charset:45 column_type:"varchar(128)"} pkfields:{name:"id1" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:7 values:"2Ñewton"} rows:{lengths:1 lengths:6 values:"3nEwton"} rows:{lengths:1 lengths:8 values:"5neẅton"} lastpk:{lengths:1 values:"5"}`,
	}
	wantQuery := "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, val from t1 where (val = 'newton') order by id1"
	checkStream(t, "select id1, val from t1 where (val = 'newton')", nil, wantQuery, wantStream)
}

func TestStreamRowsMultiPacket(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	reset := AdjustPacketSize(10)
	defer reset()

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, '234'), (2, '6789'), (3, '1'), (4, '2345678901'), (5, '2')",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	wantStream := []string{
		`fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1234"} rows:{lengths:1 lengths:4 values:"26789"} rows:{lengths:1 lengths:1 values:"31"} lastpk:{lengths:1 values:"3"}`,
		`rows:{lengths:1 lengths:10 values:"42345678901"} lastpk:{lengths:1 values:"4"}`,
		`rows:{lengths:1 lengths:1 values:"52"} lastpk:{lengths:1 values:"5"}`,
	}
	wantQuery := "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select * from t1", nil, wantQuery, wantStream)
}

func TestStreamRowsCancel(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	reset := AdjustPacketSize(10)
	defer reset()

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, '234567890'), (2, '234')",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var options binlogdatapb.VStreamOptions
	options.ConfigOverrides = make(map[string]string)

	// Support both formats for backwards compatibility
	// TODO(v25): Remove underscore versions
	utils.SetFlagVariantsForTests(options.ConfigOverrides, "vstream-dynamic-packet-size", "false")
	utils.SetFlagVariantsForTests(options.ConfigOverrides, "vstream-packet-size", "10")

	err := engine.StreamRows(ctx, "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		cancel()
		return nil
	}, &options)
	if got, want := err.Error(), "stream ended: context canceled"; got != want {
		t.Errorf("err: %v, want %s", err, want)
	}
}

func TestStreamRowsHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Save original heartbeat interval and restore it after test
	originalInterval := rowStreamertHeartbeatInterval
	defer func() {
		rowStreamertHeartbeatInterval = originalInterval
	}()

	// Set a very short heartbeat interval for testing (100ms)
	rowStreamertHeartbeatInterval = 10 * time.Millisecond

	execStatements(t, []string{
		"create table t1(id int, val varchar(128), primary key(id))",
		"insert into t1 values (1, 'test1')",
		"insert into t1 values (2, 'test2')",
		"insert into t1 values (3, 'test3')",
		"insert into t1 values (4, 'test4')",
		"insert into t1 values (5, 'test5')",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var heartbeatCount int32
	dataReceived := false

	var options binlogdatapb.VStreamOptions
	options.ConfigOverrides = make(map[string]string)
	options.ConfigOverrides["vstream_dynamic_packet_size"] = "false"
	options.ConfigOverrides["vstream_packet_size"] = "10"

	err := engine.StreamRows(ctx, "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		if rows.Heartbeat {
			atomic.AddInt32(&heartbeatCount, 1)
			// After receiving at least 3 heartbeats, we can be confident the fix is working
			if atomic.LoadInt32(&heartbeatCount) >= 3 {
				cancel()
				return nil
			}
		} else if len(rows.Rows) > 0 {
			dataReceived = true
		}
		// Add a small delay to allow heartbeats to be sent
		time.Sleep(50 * time.Millisecond)
		return nil
	}, &options)

	// We expect context canceled error since we cancel after receiving heartbeats
	if err != nil && err.Error() != "stream ended: context canceled" {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify we received data
	if !dataReceived {
		t.Error("expected to receive data rows")
	}

	// This is the critical test: we should receive multiple heartbeats
	// Without the fix (missing for loop), we would only get 1 heartbeat
	// With the fix, we should get at least 3 heartbeats
	if atomic.LoadInt32(&heartbeatCount) < 3 {
		t.Errorf("expected at least 3 heartbeats, got %d. This indicates the heartbeat goroutine is not running continuously", heartbeatCount)
	}
}

func checkStream(t *testing.T, query string, lastpk []sqltypes.Value, wantQuery string, wantStream []string) {
	t.Helper()

	i := 0
	ch := make(chan error)
	// We don't want to report errors inside callback functions because
	// line numbers come out wrong.
	go func() {
		first := true
		defer close(ch)
		var options binlogdatapb.VStreamOptions
		options.ConfigOverrides = make(map[string]string)

		// Support both formats for backwards compatibility
		// TODO(v25): Remove underscore versions
		utils.SetFlagVariantsForTests(options.ConfigOverrides, "vstream-dynamic-packet-size", strconv.FormatBool(vttablet.VStreamerUseDynamicPacketSize))
		utils.SetFlagVariantsForTests(options.ConfigOverrides, "vstream-packet-size", strconv.Itoa(vttablet.VStreamerDefaultPacketSize))

		err := engine.StreamRows(context.Background(), query, lastpk, func(rows *binlogdatapb.VStreamRowsResponse) error {
			if first {
				if rows.Gtid == "" {
					ch <- fmt.Errorf("stream gtid is empty")
				}
				if got := engine.rowStreamers[engine.streamIdx-1].sendQuery; got != wantQuery {
					log.Infof("Got: %v", got)
					ch <- fmt.Errorf("query sent:\n%v, want\n%v", got, wantQuery)
				}
			}
			first = false
			rows.Gtid = ""
			if i >= len(wantStream) {
				ch <- fmt.Errorf("unexpected stream rows: %v", rows)
				return nil
			}
			srows := fmt.Sprintf("%v", rows)
			re, _ := regexp.Compile(` flags:[\d]+`)
			srows = re.ReplaceAllString(srows, "")

			want := env.RemoveAnyDeprecatedDisplayWidths(wantStream[i])

			if srows != want {
				ch <- fmt.Errorf("stream %d:\n%s, want\n%s", i, srows, wantStream[i])
			}
			i++
			return nil
		}, &options)
		if err != nil {
			ch <- err
		}
	}()
	for err := range ch {
		t.Error(err)
	}
}

func expectStreamError(t *testing.T, query string, want string) {
	t.Helper()
	ch := make(chan error)
	go func() {
		defer close(ch)
		err := engine.StreamRows(context.Background(), query, nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
			return nil
		}, nil)
		require.EqualError(t, err, want, "Got incorrect error")
	}()
}
