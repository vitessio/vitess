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
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	err := engine.StreamRows(t.Context(), "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
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
	checkStream(t, "select 1 from t1", nil, wantQuery, wantStream, nil)

	// t1: simulates rollup, with non-pk column
	wantStream = []string{
		`fields:{name:"1" type:INT64 charset:63} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} rows:{lengths:1 lengths:3 values:"1bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select 1, val from t1", nil, wantQuery, wantStream, nil)

	// t1: simulates rollup, with pk and non-pk column
	wantStream = []string{
		`fields:{name:"1" type:INT64 charset:63} fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:3 values:"11aaa"} rows:{lengths:1 lengths:1 lengths:3 values:"12bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select 1, id, val from t1", nil, wantQuery, wantStream, nil)

	// t1: no pk in select list
	wantStream = []string{
		`fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:3 values:"aaa"} rows:{lengths:3 values:"bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select val from t1", nil, wantQuery, wantStream, nil)

	// t1: all rows
	wantStream = []string{
		`fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} rows:{lengths:1 lengths:3 values:"2bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select * from t1", nil, wantQuery, wantStream, nil)

	// t1: lastpk=1
	wantStream = []string{
		`fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:3 values:"2bbb"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) where (id > 1) order by id"
	checkStream(t, "select * from t1", []sqltypes.Value{sqltypes.NewInt64(1)}, wantQuery, wantStream, nil)

	// t1: different column ordering
	wantStream = []string{
		`fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} fields:{name:"id" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} pkfields:{name:"id" type:INT32 charset:63}`,
		`rows:{lengths:3 lengths:1 values:"aaa1"} rows:{lengths:3 lengths:1 values:"bbb2"} lastpk:{lengths:1 values:"2"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) order by id"
	checkStream(t, "select val, id from t1", nil, wantQuery, wantStream, nil)

	// t2: all rows
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t2" org_table:"t2" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t2" org_table:"t2" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t2" org_table:"t2" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:3 values:"12aaa"} rows:{lengths:1 lengths:1 lengths:3 values:"13bbb"} lastpk:{lengths:1 lengths:1 values:"13"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, val from t2 force index (`PRIMARY`) order by id1, id2"
	checkStream(t, "select * from t2", nil, wantQuery, wantStream, nil)

	// t2: lastpk=1,2
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t2" org_table:"t2" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t2" org_table:"t2" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t2" org_table:"t2" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:3 values:"13bbb"} lastpk:{lengths:1 lengths:1 values:"13"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, val from t2 force index (`PRIMARY`) where (id1 = 1 and id2 > 2) or (id1 > 1) order by id1, id2"
	checkStream(t, "select * from t2", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, wantQuery, wantStream, nil)

	// t3: all rows
	wantStream = []string{
		`fields:{name:"id" type:INT32 table:"t3" org_table:"t3" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t3" org_table:"t3" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63} pkfields:{name:"val" type:VARBINARY charset:63}`,
		`rows:{lengths:1 lengths:3 values:"1aaa"} rows:{lengths:1 lengths:3 values:"2bbb"} lastpk:{lengths:1 lengths:3 values:"2bbb"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t3 order by id, val"
	checkStream(t, "select * from t3", nil, wantQuery, wantStream, nil)

	// t3: lastpk: 1,'aaa'
	wantStream = []string{
		`fields:{name:"id" type:INT32 table:"t3" org_table:"t3" database:"vttest" org_name:"id" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t3" org_table:"t3" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id" type:INT32 charset:63} pkfields:{name:"val" type:VARBINARY charset:63}`,
		`rows:{lengths:1 lengths:3 values:"2bbb"} lastpk:{lengths:1 lengths:3 values:"2bbb"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t3 where (id = 1 and val > _binary'aaa') or (id > 1) order by id, val"
	checkStream(t, "select * from t3", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarBinary("aaa")}, wantQuery, wantStream, nil)

	// t4: all rows
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id3" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id3" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t4" org_table:"t4" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63} pkfields:{name:"id3" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:"123aaa"} rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:"234bbb"} lastpk:{lengths:1 lengths:1 lengths:1 values:"234"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, id3, val from t4 force index (`PRIMARY`) order by id1, id2, id3"
	checkStream(t, "select * from t4", nil, wantQuery, wantStream, nil)

	// t4: lastpk: 1,2,3
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id3" type:INT32 table:"t4" org_table:"t4" database:"vttest" org_name:"id3" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t4" org_table:"t4" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63} pkfields:{name:"id3" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:"234bbb"} lastpk:{lengths:1 lengths:1 lengths:1 values:"234"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, id3, val from t4 force index (`PRIMARY`) where (id1 = 1 and id2 = 2 and id3 > 3) or (id1 = 1 and id2 > 2) or (id1 > 1) order by id1, id2, id3"
	checkStream(t, "select * from t4", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(3)}, wantQuery, wantStream, nil)

	// t5: No PK, but a PKE
	wantStream = []string{
		`fields:{name:"id1" type:INT32 table:"t5" org_table:"t5" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t5" org_table:"t5" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id3" type:INT32 table:"t5" org_table:"t5" database:"vttest" org_name:"id3" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t5" org_table:"t5" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63} pkfields:{name:"id3" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:"234bbb"} lastpk:{lengths:1 lengths:1 lengths:1 values:"234"}`,
	}
	wantQuery = "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, id3, val from t5 force index (`id1_id2_id3`) where (id1 = 1 and id2 = 2 and id3 > 3) or (id1 = 1 and id2 > 2) or (id1 > 1) order by id1, id2, id3"
	checkStream(t, "select * from t5", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(3)}, wantQuery, wantStream, nil)

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
	conn, err := env.Mysqld.GetDbaConnection(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("set names latin1", 10000, false)
	require.NoError(t, err)
	// This will get "Mojibaked" into the utf8 column.
	_, err = conn.ExecuteFetch("insert into t1 values(1, '👍')", 10000, false)
	require.NoError(t, err)

	err = engine.StreamRows(t.Context(), "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		// Skip fields.
		if len(rows.Rows) == 0 {
			return nil
		}
		got := fmt.Sprintf("%q", rows.Rows[0].Values)
		// We should expect a "Mojibaked" version of the string.
		want := `"1ðŸ‘\u008d"`
		assert.Equalf(t, want, got, "rows.Rows[0].Values: %s, want %s", got, want)
		return nil
	}, nil)
	require.NoError(t, err)
}

func TestStreamRowsKeyRange(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	require.NoError(t, env.SetVSchema(shardedVSchema))
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
	checkStream(t, "select * from t1 where in_keyrange('-80')", nil, wantQuery, wantStream, nil)
}

func TestStreamRowsWithNoTimeouts(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	err := env.SetVSchema(shardedVSchema)
	require.NoError(t, err)
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

	// Pass the NoTimeouts option so that we do not get the MAX_EXECUTION_TIME query hint added to the query.
	wantQuery := "select id1, val from t1 force index (`PRIMARY`) order by id1"
	checkStream(t, "select * from t1 where in_keyrange('-80')", nil, wantQuery, wantStream, &binlogdatapb.VStreamOptions{NoTimeouts: true})
}

func TestStreamRowsFilterInt(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	engine.rowStreamerNumPackets.Reset()
	engine.rowStreamerNumRows.Reset()

	require.NoError(t, env.SetVSchema(shardedVSchema))
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
	checkStream(t, "select id1, val from t1 where (id2 = 100)", nil, wantQuery, wantStream, nil)
	require.Equal(t, int64(0), engine.rowStreamerNumPackets.Get())
	require.Equal(t, int64(2), engine.rowStreamerNumRows.Get())
	require.Positive(t, engine.vstreamerPacketSize.Get())
}

func TestStreamRowsFilterBetween(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	engine.rowStreamerNumPackets.Reset()
	engine.rowStreamerNumRows.Reset()

	require.NoError(t, env.SetVSchema(shardedVSchema))
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
	checkStream(t, "select id1, val from t1 where (id1 between 2 and 4)", nil, wantQuery, wantStream, nil)
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
	checkStream(t, "select id1, val from t1 where (id1 not between 2 and 4)", nil, wantQuery, wantStream, nil)
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

	require.NoError(t, env.SetVSchema(shardedVSchema))
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
	checkStream(t, "select id1, val from t1 where id1 in (1, 2, 3)", nil, wantQuery, wantStream, nil)
	require.Equal(t, int64(0), engine.rowStreamerNumPackets.Get())
	require.Equal(t, int64(3), engine.rowStreamerNumRows.Get())
	require.NotZero(t, engine.vstreamerPacketSize.Get())
}

// TestStreamRowsFilterWithLastPK confirms that pushed down predicates are
// ANDed with the *entire* lastpk clause when a copy phase cycle resumes.
// The lastpk clause for a composite PK is a disjunction, so it has to be
// parenthesized as a unit; otherwise AND binds more tightly than OR and the
// trailing OR terms match rows that the pushed down predicates exclude.
// Such rows are dropped in memory by the plan's filters before being
// streamed, so the row output is identical either way and the generated
// query assertion below is what guards the fix: the cost of the bug is
// mysqld needlessly scanning and returning the excluded rows.
func TestStreamRowsFilterWithLastPK(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	require.NoError(t, env.SetVSchema(shardedVSchema))
	defer env.SetVSchema("{}")

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1, id2))",
		// (2,1) is the row that the unparenthesized lastpk disjunction lets
		// escape the filter in the generated query: it fails the filter but
		// satisfies `id1 > 1`.
		"insert into t1 values (1, 1, 'include'), (1, 2, 'include'), (1, 3, 'exclude'), (2, 1, 'exclude'), (2, 2, 'include')",
	})

	defer execStatements(t, []string{
		"drop table t1",
	})

	wantStream := []string{
		`fields:{name:"id1" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id1" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"id2" type:INT32 table:"t1" org_table:"t1" database:"vttest" org_name:"id2" column_length:11 charset:63 column_type:"int(11)"} fields:{name:"val" type:VARBINARY table:"t1" org_table:"t1" database:"vttest" org_name:"val" column_length:128 charset:63 column_type:"varbinary(128)"} pkfields:{name:"id1" type:INT32 charset:63} pkfields:{name:"id2" type:INT32 charset:63}`,
		`rows:{lengths:1 lengths:1 lengths:7 values:"12include"} rows:{lengths:1 lengths:1 lengths:7 values:"22include"} lastpk:{lengths:1 lengths:1 values:"22"}`,
	}
	wantQuery := "select /*+ MAX_EXECUTION_TIME(3600000) */ id1, id2, val from t1 where (val = 'include') and ((id1 = 1 and id2 > 1) or (id1 > 1)) order by id1, id2"
	checkStream(t, "select id1, id2, val from t1 where (val = 'include')",
		[]sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1)}, wantQuery, wantStream, nil)
}

func TestStreamRowsFilterVarBinary(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	require.NoError(t, env.SetVSchema(shardedVSchema))
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
	checkStream(t, "select id1, val from t1 where (val = 'newton')", nil, wantQuery, wantStream, nil)
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
	checkStream(t, "select id1, val from t1 where (val = 'newton')", nil, wantQuery, wantStream, nil)
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
	checkStream(t, "select * from t1", nil, wantQuery, wantStream, nil)
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

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var options binlogdatapb.VStreamOptions
	options.ConfigOverrides = make(map[string]string)

	// Support both formats for backwards compatibility
	options.ConfigOverrides["vstream-dynamic-packet-size"] = "false"
	options.ConfigOverrides["vstream-packet-size"] = "10"

	err := engine.StreamRows(ctx, "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		cancel()
		return nil
	}, &options)
	assert.ErrorIsf(t, err, context.Canceled, "err: %v, want context.Canceled", err)
}

func TestStreamRowsSendError(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, 'aaa')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})

	var options binlogdatapb.VStreamOptions
	options.ConfigOverrides = make(map[string]string)
	options.ConfigOverrides["vstream-dynamic-packet-size"] = "false"
	options.ConfigOverrides["vstream-packet-size"] = "10"

	sendErr := errors.New("send failed")
	err := engine.StreamRows(t.Context(), "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		return sendErr
	}, &options)
	assert.ErrorIsf(t, err, sendErr, "err: %v, want %v", err, sendErr)
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
	rowStreamertHeartbeatInterval = 5 * time.Millisecond

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

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var heartbeatCount int32
	var streamCanceled atomic.Int32
	var rowsAfterCancel atomic.Int32
	var heartbeatsAfterCancel atomic.Int32
	dataReceived := false

	var options binlogdatapb.VStreamOptions
	options.ConfigOverrides = make(map[string]string)
	options.ConfigOverrides["vstream-dynamic-packet-size"] = "false"
	options.ConfigOverrides["vstream-packet-size"] = "1"

	err := engine.StreamRows(ctx, "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		if rows.Heartbeat {
			if streamCanceled.Load() != 0 {
				heartbeatsAfterCancel.Add(1)
				return nil
			}
			atomic.AddInt32(&heartbeatCount, 1)
			// After receiving at least 3 heartbeats, we can be confident the fix is working
			if atomic.LoadInt32(&heartbeatCount) >= 3 {
				streamCanceled.Store(1)
				cancel()
				return nil
			}
		} else if len(rows.Rows) > 0 {
			if streamCanceled.Load() != 0 {
				rowsAfterCancel.Add(1)
				return nil
			}
			dataReceived = true
		}
		// Add a small delay to allow heartbeats to be sent
		time.Sleep(100 * time.Millisecond)
		return nil
	}, &options)
	// We expect context canceled error since we cancel after receiving heartbeats
	if err != nil && !errors.Is(err, context.Canceled) {
		require.FailNowf(t, "unexpected error type (expected context.Canceled)", "got: %v", err)
	}

	// Verify we received data
	assert.True(t, dataReceived, "expected to receive data rows")

	// This is the critical test: we should receive multiple heartbeats
	// Without the fix (missing for loop), we would only get 1 heartbeat
	// With the fix, we should get at least 3 heartbeats
	if atomic.LoadInt32(&heartbeatCount) < 3 {
		assert.Failf(t, "expected at least 3 heartbeats", "expected at least 3 heartbeats, got %d. This indicates the heartbeat goroutine is not running continuously", heartbeatCount)
	}

	require.Never(t, func() bool {
		return rowsAfterCancel.Load() != 0 || heartbeatsAfterCancel.Load() != 0
	}, 50*time.Millisecond, time.Millisecond, "expected context cancellation to stop row and heartbeat callbacks")
}

func checkStream(t *testing.T, query string, lastpk []sqltypes.Value, wantQuery string, wantStream []string, options *binlogdatapb.VStreamOptions) {
	t.Helper()

	i := 0
	ch := make(chan error)
	// We don't want to report errors inside callback functions because
	// line numbers come out wrong.
	go func() {
		first := true
		defer close(ch)
		if options == nil {
			options = &binlogdatapb.VStreamOptions{}
		}
		options.ConfigOverrides = make(map[string]string)

		// Support both formats for backwards compatibility
		options.ConfigOverrides["vstream-dynamic-packet-size"] = strconv.FormatBool(vttablet.VStreamerUseDynamicPacketSize)
		options.ConfigOverrides["vstream-packet-size"] = strconv.Itoa(vttablet.VStreamerDefaultPacketSize)

		err := engine.StreamRows(t.Context(), query, lastpk, func(rows *binlogdatapb.VStreamRowsResponse) error {
			if first {
				if rows.Gtid == "" {
					ch <- errors.New("stream gtid is empty")
				}
				if got := engine.rowStreamers[engine.streamIdx-1].sendQuery; got != wantQuery {
					log.Info(fmt.Sprintf("Got: %v", got))
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
		}, options)
		if err != nil {
			ch <- err
		}
	}()
	for err := range ch {
		assert.NoError(t, err)
	}
}

func expectStreamError(t *testing.T, query string, want string) {
	t.Helper()
	err := engine.StreamRows(t.Context(), query, nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		return nil
	}, nil)
	require.EqualError(t, err, want, "Got incorrect error")
}

func TestTrimRowPool(t *testing.T) {
	rows := []*querypb.Row{{Values: make([]byte, 0, 1024)}, {Values: make([]byte, 0, 2048)}}

	// Under budget: the pool and poolBytes are unchanged, not dropped.
	kept, poolBytes, dropped := trimRowPool(rows, 3072, 4096)
	require.False(t, dropped)
	require.Equal(t, 3072, poolBytes)
	require.Len(t, kept, 2)

	// Over budget: the whole pool is dropped and poolBytes resets to 0.
	kept, poolBytes, dropped = trimRowPool(rows, 5000, 4096)
	require.True(t, dropped)
	require.Equal(t, 0, poolBytes)
	require.Nil(t, kept)
}

// smallColumnCount is how many one-byte columns the row-pool tests put next to the id: with 64 columns a
// row's Lengths (512 bytes) outweigh its values and the fixed per-row cost, so a budget or an accounting
// that leaves Lengths out is off by more than the headroom.
const smallColumnCount = 63

// smallColumns repeats what (a column definition or a value) for the small columns of the row-pool tests.
func smallColumns(what string) string {
	parts := make([]string, smallColumnCount)
	for i := range parts {
		parts[i] = what
		if strings.HasPrefix(what, "varbinary") {
			parts[i] = fmt.Sprintf("c%d %s", i+1, what)
		}
	}
	return strings.Join(parts, ", ")
}

// TestStreamRowsReusesRowPool streams tables whose packets are all alike through the engine and checks
// in the send callback that the row pool is filled once and then reused: the same *Row objects serve
// every packet and the pool neither grows nor is dropped (vitessio/vitess#20954). The rows are shaped so
// that the pool's per-row overhead (Lengths and the Row structs) outweighs the values, which is what
// catches a budget or an accounting that counts only the values.
func TestStreamRowsReusesRowPool(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	cases := []struct {
		name       string
		create     string
		rowValues  func(id int) string
		rows       int
		packetSize int
		minPackets int
	}{
		{
			// About 3.8k rows of 64 one-byte columns per 256 KiB packet: each row's Lengths (512 bytes) outweigh
			// its values (69 bytes) and the fixed per-row cost (64 bytes), so the pool holds about 2.2 MB per
			// packet, over the 1 MiB floor and over four times the values it carries. A budget or an
			// accounting that ignores Lengths drops it.
			name:       "many small rows",
			create:     "create table t1(id int, " + smallColumns("varbinary(1)") + ", primary key(id))",
			rowValues:  func(id int) string { return fmt.Sprintf("(%d, %s)", id, smallColumns("'a'")) },
			rows:       60000,
			packetSize: 256 << 10,
			minPackets: 12,
		},
		{
			// A large value in every row, one row per packet: a value that stays in its slot is the shape of
			// every packet, not retention.
			name:       "one large row per packet",
			create:     "create table t1(id int, val longblob, primary key(id))",
			rowValues:  func(id int) string { return fmt.Sprintf("(%d, repeat('b', %d))", id, 64<<10) },
			rows:       60,
			packetSize: 64 << 10,
			minPackets: 60,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			execStatements(t, []string{tc.create})
			defer execStatements(t, []string{"drop table t1"})

			// Six-digit ids keep every row the same length, so every full packet holds the same number of rows.
			const batch, firstID = 5000, 100000
			var stmts []string
			for first := firstID; first < firstID+tc.rows; first += batch {
				var values []string
				for id := first; id < first+batch && id < firstID+tc.rows; id++ {
					values = append(values, tc.rowValues(id))
				}
				stmts = append(stmts, "insert into t1 values "+strings.Join(values, ","))
			}
			execStatements(t, stmts)

			var options binlogdatapb.VStreamOptions
			options.ConfigOverrides = map[string]string{
				"vstream-dynamic-packet-size": "false",
				"vstream-packet-size":         strconv.Itoa(tc.packetSize),
				"vstream-memory-bounds":       "true", // not the flag's default: the backports ship it off
			}

			type seen struct {
				rows     int
				retained int
				first    *querypb.Row
			}
			var got []seen
			err := engine.StreamRows(t.Context(), "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
				if len(rows.Rows) == 0 {
					return nil
				}
				got = append(got, seen{rows: len(rows.Rows), retained: sqltypes.RowPoolRetained(rows.Rows), first: rows.Rows[0]})
				return nil
			}, &options)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(got), tc.minPackets)

			// The first packet fills the pool and every later one is served by the same rows. The last packet
			// is whatever is left of the table, so the full-packet checks stop before it.
			warm := got[0]
			for i := 1; i < len(got); i++ {
				assert.Same(t, warm.first, got[i].first, "packet %d: the pool was dropped under uniform rows", i)
				if i < len(got)-1 {
					assert.Equal(t, warm.rows, got[i].rows, "packet %d: not a full packet", i)
					assert.Equal(t, warm.retained, got[i].retained, "packet %d: the pool grew under uniform rows", i)
				}
			}
		})
	}
}

// TestStreamRowsDropsDensePoolForLargeRows streams dense packets of many-column rows and then packets of a
// few large rows through the engine. The dense packets leave a pool whose retained bytes are mostly
// Lengths; the large-row packets have a budget near the floor, so the pool must be dropped on the first of
// them. An accounting that leaves Lengths out of poolBytes would see a pool well under budget and keep it
// (vitessio/vitess#20954).
func TestStreamRowsDropsDensePoolForLargeRows(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	const (
		denseRows  = 20000
		largeRows  = 12
		largeBytes = 64 << 10
		packetSize = 256 << 10
		firstID    = 100000
	)

	execStatements(t, []string{
		"create table t1(id int, " + smallColumns("varbinary(1)") + ", val longblob, primary key(id))",
	})
	defer execStatements(t, []string{"drop table t1"})

	var stmts []string
	for first := firstID; first < firstID+denseRows; first += 5000 {
		var values []string
		for id := first; id < first+5000 && id < firstID+denseRows; id++ {
			values = append(values, fmt.Sprintf("(%d, %s, 'x')", id, smallColumns("'a'")))
		}
		stmts = append(stmts, "insert into t1 values "+strings.Join(values, ","))
	}
	var large []string
	for id := firstID + denseRows; id < firstID+denseRows+largeRows; id++ {
		large = append(large, fmt.Sprintf("(%d, %s, repeat('b', %d))", id, smallColumns("'a'"), largeBytes))
	}
	stmts = append(stmts, "insert into t1 values "+strings.Join(large, ","))
	execStatements(t, stmts)

	var options binlogdatapb.VStreamOptions
	options.ConfigOverrides = map[string]string{
		"vstream-dynamic-packet-size": "false",
		"vstream-packet-size":         strconv.Itoa(packetSize),
		"vstream-memory-bounds":       "true", // not the flag's default: the backports ship it off
	}

	type seen struct {
		rows     int
		retained int
		first    *querypb.Row
	}
	var got []seen
	err := engine.StreamRows(t.Context(), "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		if len(rows.Rows) == 0 {
			return nil
		}
		got = append(got, seen{rows: len(rows.Rows), retained: sqltypes.RowPoolRetained(rows.Rows), first: rows.Rows[0]})
		return nil
	}, &options)
	require.NoError(t, err)

	// Find the first packet of large rows: far fewer rows than a dense packet.
	dense := got[0]
	firstLarge := -1
	for i, g := range got {
		if g.rows < dense.rows/10 {
			firstLarge = i
			break
		}
	}
	require.Positive(t, firstLarge, "no packet of large rows was sent")
	require.Greater(t, dense.retained, sqltypes.RowPoolMinBytes, "the dense pool must exceed the floor for the drop to be required")

	// The dense packets reuse the pool; the first large-row packet still sees it (the callback runs before
	// the trim), and the one after it must be served by a fresh pool.
	for i := 1; i < firstLarge; i++ {
		assert.Same(t, dense.first, got[i].first, "packet %d: the pool was dropped under uniform dense rows", i)
	}
	require.Greater(t, len(got), firstLarge+1, "need a packet after the first large-row packet")
	assert.NotSame(t, dense.first, got[firstLarge+1].first, "the pool holding the dense packets' Lengths was kept for the large rows")
	assert.Less(t, got[firstLarge+1].retained, dense.retained/2, "the pool after the drop still retains the dense packets' Lengths")
}

// TestStreamRowsBoundsRowPoolRetention streams a table whose one large value lands in a new slot of the
// reused row pool in every packet, and checks in the send callback what the pool retains
// (rows[:cap(rows)]): with the memory bounds on, the trim after each packet keeps it near the budget;
// with them off (--vstream-memory-bounds=false, here as the workflow override), every slot keeps its
// high-water mark and the pool grows to about packets × the large value by the end of the stream
// (vitessio/vitess#20954).
func TestStreamRowsBoundsRowPoolRetention(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	const (
		packets  = 100
		bigBytes = 64 << 10
	)

	execStatements(t, []string{
		"create table t1(id int, val longblob, primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})

	// Packet k holds k small rows followed by the large one, so the large value sits in slot k of that
	// packet; with a fixed packet size of bigBytes the packet closes right after it.
	var stmts []string
	id := 0
	for k := range packets {
		var values []string
		for range k {
			id++
			values = append(values, fmt.Sprintf("(%d, 's')", id))
		}
		id++
		values = append(values, fmt.Sprintf("(%d, repeat('b', %d))", id, bigBytes))
		stmts = append(stmts, "insert into t1 values "+strings.Join(values, ","))
	}
	execStatements(t, stmts)

	stream := func(t *testing.T, memoryBounds bool) (sent, maxRetained, lastRetained int) {
		var options binlogdatapb.VStreamOptions
		options.ConfigOverrides = map[string]string{
			"vstream-dynamic-packet-size": "false",
			"vstream-packet-size":         strconv.Itoa(bigBytes),
			"vstream-memory-bounds":       strconv.FormatBool(memoryBounds),
		}
		err := engine.StreamRows(t.Context(), "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
			if len(rows.Rows) == 0 {
				return nil
			}
			sent++
			retained := sqltypes.RowPoolRetained(rows.Rows)
			maxRetained = max(maxRetained, retained)
			lastRetained = retained
			return nil
		}, &options)
		require.NoError(t, err)
		require.GreaterOrEqual(t, sent, packets, "every large row should have closed a packet")
		return sent, maxRetained, lastRetained
	}

	t.Run("bounded", func(t *testing.T) {
		_, maxRetained, lastRetained := stream(t, true)
		// The send callback sees the pool after the packet's rows were written into it and before the trim,
		// so it holds at most the previous packet's budget (the 1 MiB floor here, as the packets are small)
		// plus this packet's growth. Untrimmed, the last packet would retain about packets × bigBytes.
		bound := sqltypes.RowPoolMinBytes + 2*bigBytes + packets*(2*8+64)
		assert.LessOrEqual(t, maxRetained, bound, "the pool retained %d bytes at some packet, untrimmed it would reach %d", maxRetained, packets*bigBytes)
		assert.LessOrEqual(t, lastRetained, bound)
		assert.Less(t, lastRetained, packets*bigBytes/4, "the pool kept most high-water marks: %d bytes", lastRetained)
	})

	t.Run("unbounded", func(t *testing.T) {
		_, _, lastRetained := stream(t, false)
		// Today's behavior: every slot the large value visited keeps it.
		assert.Greater(t, lastRetained, packets*bigBytes/2, "with the bounds off the pool should keep its high-water marks: %d bytes", lastRetained)
	})
}

// TestRowStreamerMemoryBoundsFlag checks that the vttablet flag, not only the workflow override, reaches
// the row streamer: with --vstream-memory-bounds=false and no override the streamer runs without the
// bounds and its dynamic packet sizer has no cap, and with the flag on it gets both.
func TestRowStreamerMemoryBoundsFlag(t *testing.T) {
	origFlag := vttablet.VStreamerMemoryBounds
	t.Cleanup(func() {
		vttablet.VStreamerMemoryBounds = origFlag
		vttablet.GetVReplicationConfigDefaults(false)
	})

	for _, bounds := range []bool{true, false} {
		t.Run(strconv.FormatBool(bounds), func(t *testing.T) {
			vttablet.VStreamerMemoryBounds = bounds
			vttablet.GetVReplicationConfigDefaults(false)

			rs, err := newRowStreamer(t.Context(), engine.env.Config().DB.FilteredWithDB(), engine.se, "select * from t1", nil,
				engine.lvschema, nil, engine, RowStreamerModeSingleTable, nil, nil)
			require.NoError(t, err)
			t.Cleanup(rs.Cancel)

			assert.Equal(t, bounds, rs.config.VStreamMemoryBounds, "the flag must reach the streamer's config")
			ps, ok := rs.pktsize.(*dynamicPacketSizer)
			require.True(t, ok, "dynamic packet sizing is on by default")
			if bounds {
				assert.Equal(t, dynamicPacketSizerMaxGrowthFactor*vttablet.VStreamerDefaultPacketSize, ps.maxSize)
			} else {
				assert.Zero(t, ps.maxSize, "no packet-size cap with the flag off")
			}
		})
	}
}

// TestStreamsRejectUnknownOverride checks that an override the source does not know fails every stream
// kind with an INVALID_ARGUMENT error that names the key, instead of a nil streamer and a panic: the copy
// (StreamRows, StreamTables) and the running phase (Stream, from "current" and from a position).
func TestStreamsRejectUnknownOverride(t *testing.T) {
	options := &binlogdatapb.VStreamOptions{ConfigOverrides: map[string]string{"no-such-override": "1"}}
	filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: "/.*/"}}}

	streams := map[string]func() error{
		"StreamRows": func() error {
			return engine.StreamRows(t.Context(), "select * from t1", nil, func(*binlogdatapb.VStreamRowsResponse) error { return nil }, options)
		},
		"StreamTables": func() error {
			return engine.StreamTables(t.Context(), func(*binlogdatapb.VStreamTablesResponse) error { return nil }, options)
		},
		"Stream from current": func() error {
			return engine.Stream(t.Context(), "current", nil, filter, "", func([]*binlogdatapb.VEvent) error { return nil }, options)
		},
		"Stream from a position": func() error {
			return engine.Stream(t.Context(), primaryPosition(t), nil, filter, "", func([]*binlogdatapb.VEvent) error { return nil }, options)
		},
	}

	for name, stream := range streams {
		t.Run(name, func(t *testing.T) {
			err := stream()
			require.ErrorContains(t, err, "no-such-override")
			assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
		})
	}
}
