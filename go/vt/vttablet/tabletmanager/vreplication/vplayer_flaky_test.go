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

package vreplication

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"

	"vitess.io/vitess/go/vt/vttablet"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	qh "vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication/queryhistory"
)

// TestPlayerGeneratedInvisiblePrimaryKey confirms that the gipk column is replicated by vplayer, both for target
// tables that have a gipk column and those that make it visible.
func TestPlayerGeneratedInvisiblePrimaryKey(t *testing.T) {
	if !env.HasCapability(testenv.ServerCapabilityGeneratedInvisiblePrimaryKey) {
		t.Skip("skipping test as server does not support generated invisible primary keys")
	}
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"SET @@session.sql_generate_invisible_primary_key=ON;",
		"create table t1(val varbinary(128))",
		fmt.Sprintf("create table %s.t1(val varbinary(128))", vrepldb),
		"create table t2(val varbinary(128))",
		"SET @@session.sql_generate_invisible_primary_key=OFF;",
		fmt.Sprintf("create table %s.t2(my_row_id int, val varbinary(128), primary key(my_row_id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
		"drop table t2",
		fmt.Sprintf("drop table %s.t2", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}, {
			Match:  "t2",
			Filter: "select * from t2",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input       string
		output      string
		table       string
		data        [][]string
		query       string
		queryResult [][]string
	}{{
		input:  "insert into t1(val) values ('aaa')",
		output: "insert into t1(my_row_id,val) values (1,'aaa')",
		table:  "t1",
		data: [][]string{
			{"aaa"},
		},
		query: "select my_row_id, val from t1",
		queryResult: [][]string{
			{"1", "aaa"},
		},
	}, {
		input:  "insert into t2(val) values ('bbb')",
		output: "insert into t2(my_row_id,val) values (1,'bbb')",
		table:  "t2",
		data: [][]string{
			{"1", "bbb"},
		},
		query: "select my_row_id, val from t2",
		queryResult: [][]string{
			{"1", "bbb"},
		},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		output := qh.Expect(tcases.output)
		expectNontxQueries(t, output)
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
		if tcases.query != "" {
			expectQueryResult(t, tcases.query, tcases.queryResult)
		}
	}
}

func TestPlayerInvisibleColumns(t *testing.T) {
	if !supportsInvisibleColumns() {
		t.Skip()
	}
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, val varchar(20), id2 int invisible, pk2 int invisible, primary key(id, pk2))",
		fmt.Sprintf("create table %s.t1(id int, val varchar(20), id2 int invisible, pk2 int invisible, primary key(id, pk2))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input       string
		output      string
		table       string
		data        [][]string
		query       string
		queryResult [][]string
	}{{
		input:  "insert into t1(id,val,id2,pk2) values (1,'aaa',10,100)",
		output: "insert into t1(id,val,id2,pk2) values (1,'aaa',10,100)",
		table:  "t1",
		data: [][]string{
			{"1", "aaa"},
		},
		query: "select id, val, id2, pk2 from t1",
		queryResult: [][]string{
			{"1", "aaa", "10", "100"},
		},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		output := qh.Expect(tcases.output)
		expectNontxQueries(t, output)
		time.Sleep(1 * time.Second)
		log.Flush()
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
		if tcases.query != "" {
			expectQueryResult(t, tcases.query, tcases.queryResult)
		}
	}
}

func TestHeartbeatFrequencyFlag(t *testing.T) {
	origVReplicationHeartbeatUpdateInterval := vreplicationHeartbeatUpdateInterval
	defer func() {
		vreplicationHeartbeatUpdateInterval = origVReplicationHeartbeatUpdateInterval
	}()

	stats := binlogplayer.NewStats()
	vp := &vplayer{vr: &vreplicator{dbClient: newVDBClient(realDBClientFactory(), stats), stats: stats}}

	type testcount struct {
		count      int
		mustUpdate bool
	}
	type testcase struct {
		name     string
		interval int
		counts   []testcount
	}
	testcases := []*testcase{
		{"default frequency", 1, []testcount{{count: 0, mustUpdate: false}, {1, true}}},
		{"custom frequency", 4, []testcount{{count: 0, mustUpdate: false}, {count: 3, mustUpdate: false}, {4, true}}},
		{"minumum frequency", 61, []testcount{{count: 59, mustUpdate: false}, {count: 60, mustUpdate: true}, {61, true}}},
	}
	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			vreplicationHeartbeatUpdateInterval = tcase.interval
			for _, tcount := range tcase.counts {
				vp.numAccumulatedHeartbeats = tcount.count
				require.Equal(t, tcount.mustUpdate, vp.mustUpdateHeartbeat())
			}
		})
	}
}

func TestVReplicationTimeUpdated(t *testing.T) {
	ctx := context.Background()
	defer deleteTablet(addTablet(100))
	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	execStatements(t, []string{
		"insert into t1 values(1, 'aaa')",
	})

	var getTimestamps = func() (int64, int64, int64) {
		qr, err := env.Mysqld.FetchSuperQuery(ctx, "select time_updated, transaction_timestamp, time_heartbeat from _vt.vreplication")
		require.NoError(t, err)
		require.NotNil(t, qr)
		require.Equal(t, 1, len(qr.Rows))
		row := qr.Named().Row()
		timeUpdated, err := row.ToInt64("time_updated")
		require.NoError(t, err)
		transactionTimestamp, err := row.ToInt64("transaction_timestamp")
		require.NoError(t, err)
		timeHeartbeat, err := row.ToInt64("time_heartbeat")
		require.NoError(t, err)
		return timeUpdated, transactionTimestamp, timeHeartbeat
	}
	expectNontxQueries(t, qh.Expect("insert into t1(id,val) values (1,'aaa')"))
	time.Sleep(1 * time.Second)
	timeUpdated1, transactionTimestamp1, timeHeartbeat1 := getTimestamps()
	time.Sleep(2 * time.Second)
	timeUpdated2, _, timeHeartbeat2 := getTimestamps()
	require.Greater(t, timeUpdated2, timeUpdated1, "time_updated not updated")
	require.Greater(t, timeUpdated2, transactionTimestamp1, "transaction_timestamp should not be < time_updated")
	require.Greater(t, timeHeartbeat2, timeHeartbeat1, "time_heartbeat not updated")
}

func TestCharPK(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, val binary(2), primary key(val))",
		fmt.Sprintf("create table %s.t1(id int, val binary(2), primary key(val))", vrepldb),
		"create table t2(id int, val char(2), primary key(val))",
		fmt.Sprintf("create table %s.t2(id int, val char(2), primary key(val))", vrepldb),
		"create table t3(id int, val varbinary(2), primary key(val))",
		fmt.Sprintf("create table %s.t3(id int, val varbinary(2), primary key(val))", vrepldb),
		"create table t4(id int, val varchar(2), primary key(val))",
		fmt.Sprintf("create table %s.t4(id int, val varchar(2), primary key(val))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
		"drop table t2",
		fmt.Sprintf("drop table %s.t2", vrepldb),
		"drop table t3",
		fmt.Sprintf("drop table %s.t3", vrepldb),
		"drop table t4",
		fmt.Sprintf("drop table %s.t4", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}, {
			Match:  "t2",
			Filter: "select * from t2",
		}, {
			Match:  "t3",
			Filter: "select * from t3",
		}, {
			Match:  "t4",
			Filter: "select * from t4",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input  string
		output string
		table  string
		data   [][]string
	}{{ //binary(2)
		input:  "insert into t1 values(1, 'a')",
		output: "insert into t1(id,val) values (1,'a\\0')",
		table:  "t1",
		data: [][]string{
			{"1", "a\000"},
		},
	}, {
		input:  "update t1 set id = 2 where val = 'a\000'",
		output: "update t1 set id=2 where val='a\\0'",
		table:  "t1",
		data: [][]string{
			{"2", "a\000"},
		},
	}, { //char(2)
		input:  "insert into t2 values(1, 'a')",
		output: "insert into t2(id,val) values (1,'a')",
		table:  "t2",
		data: [][]string{
			{"1", "a"},
		},
	}, {
		input:  "update t2 set id = 2 where val = 'a'",
		output: "update t2 set id=2 where val='a'",
		table:  "t2",
		data: [][]string{
			{"2", "a"},
		},
	}, { //varbinary(2)
		input:  "insert into t3 values(1, 'a')",
		output: "insert into t3(id,val) values (1,'a')",
		table:  "t3",
		data: [][]string{
			{"1", "a"},
		},
	}, {
		input:  "update t3 set id = 2 where val = 'a'",
		output: "update t3 set id=2 where val='a'",
		table:  "t3",
		data: [][]string{
			{"2", "a"},
		},
	}, { //varchar(2)
		input:  "insert into t4 values(1, 'a')",
		output: "insert into t4(id,val) values (1,'a')",
		table:  "t4",
		data: [][]string{
			{"1", "a"},
		},
	}, {
		input:  "update t4 set id = 2 where val = 'a'",
		output: "update t4 set id=2 where val='a'",
		table:  "t4",
		data: [][]string{
			{"2", "a"},
		},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		output := qh.Expect(
			"begin",
			tcases.output,
			"/update _vt.vreplication set pos",
			"commit",
		)
		expectDBClientQueries(t, output)
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
	}
}

func TestRollup(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, val varchar(20), primary key(id))",
		fmt.Sprintf("create table %s.t1(rollupname varchar(20), kount int, primary key(rollupname))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select 'total' as rollupname, count(*) as kount from t1 group by rollupname",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input  string
		output string
		table  string
		data   [][]string
	}{{
		// Start with all nulls
		input:  "insert into t1 values(1, 'a')",
		output: "insert into t1(rollupname,kount) values ('total',1) on duplicate key update kount=kount+1",
		table:  "t1",
		data: [][]string{
			{"total", "1"},
		},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		output := qh.Expect(
			"begin",
			tcases.output,
			"/update _vt.vreplication set pos",
			"commit",
		)
		expectDBClientQueries(t, output)
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
	}
}

func TestPlayerSavepoint(t *testing.T) {
	defer deleteTablet(addTablet(100))
	execStatements(t, []string{
		"create table t1(id int, primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	// Issue a dummy change to ensure vreplication is initialized. Otherwise there
	// is a race between the DDLs and the schema loader of vstreamer.
	// Root cause seems to be with MySQL where t1 shows up in information_schema before
	// the actual table is created.
	execStatements(t, []string{"insert into t1 values(1)"})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into t1(id) values (1)",
		"/update _vt.vreplication set pos=",
		"commit",
	))

	execStatements(t, []string{
		"begin",
		"savepoint vrepl_a",
		"insert into t1(id) values (2)",
		"savepoint vrepl_b",
		"insert into t1(id) values (3)",
		"release savepoint vrepl_b",
		"savepoint vrepl_a",
		"insert into t1(id) values (42)",
		"rollback work to savepoint vrepl_a",
		"commit",
	})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"/insert into t1.*2.*",
		"/insert into t1.*3.*",
		"/update _vt.vreplication set pos=",
		"commit",
	))
	cancel()
}

func TestPlayerStatementModeWithFilter(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table src1",
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select * from src1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	input := []string{
		"set @@session.binlog_format='STATEMENT'",
		"insert into src1 values(1, 'aaa')",
		"set @@session.binlog_format='ROW'",
	}

	// It does not work when filter is enabled
	output := qh.Expect(
		"begin",
		"rollback",
		"/update _vt.vreplication set message='filter rules are not supported for SBR",
	)

	execStatements(t, input)
	expectDBClientQueries(t, output)
}

func TestPlayerStatementMode(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.src1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.src1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "/.*",
			Filter: "",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	input := []string{
		"set @@session.binlog_format='STATEMENT'",
		"insert into src1 values(1, 'aaa')",
		"set @@session.binlog_format='ROW'",
	}

	output := qh.Expect(
		"begin",
		"insert into src1 values(1, 'aaa')",
		"/update _vt.vreplication set pos=",
		"commit",
	)

	execStatements(t, input)
	expectDBClientQueries(t, output)
}

func TestPlayerFilters(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.dst1(id int, val varbinary(128), primary key(id))", vrepldb),
		"create table src2(id int, val1 int, val2 int, primary key(id))",
		fmt.Sprintf("create table %s.dst2(id int, val1 int, sval2 int, rcount int, primary key(id))", vrepldb),
		"create table src3(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.dst3(id int, val varbinary(128), primary key(id))", vrepldb),
		"create table yes(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.yes(id int, val varbinary(128), primary key(id))", vrepldb),
		"create table no(id int, val varbinary(128), primary key(id))",
		"create table nopk(id int, val varbinary(128))",
		fmt.Sprintf("create table %s.nopk(id int, val varbinary(128))", vrepldb),
		"create table src4(id1 int, id2 int, val varbinary(128), primary key(id1))",
		fmt.Sprintf("create table %s.dst4(id1 int, val varbinary(128), primary key(id1))", vrepldb),
		"create table src5(id1 int, id2 int, val varbinary(128), primary key(id1))",
		fmt.Sprintf("create table %s.dst5(id1 int, val varbinary(128), primary key(id1))", vrepldb),
		"create table srcCharset(id1 int, val varchar(128) character set utf8mb4 collate utf8mb4_bin, primary key(id1))",
		fmt.Sprintf("create table %s.dstCharset(id1 int, val varchar(128) character set utf8mb4 collate utf8mb4_bin, val2 varchar(128) character set utf8mb4 collate utf8mb4_bin, primary key(id1))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.dst1", vrepldb),
		"drop table src2",
		fmt.Sprintf("drop table %s.dst2", vrepldb),
		"drop table src3",
		fmt.Sprintf("drop table %s.dst3", vrepldb),
		"drop table yes",
		fmt.Sprintf("drop table %s.yes", vrepldb),
		"drop table no",
		"drop table nopk",
		fmt.Sprintf("drop table %s.nopk", vrepldb),
		"drop table src4",
		fmt.Sprintf("drop table %s.dst4", vrepldb),
		"drop table src5",
		fmt.Sprintf("drop table %s.dst5", vrepldb),
		"drop table srcCharset",
		fmt.Sprintf("drop table %s.dstCharset", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select * from src1",
		}, {
			Match:  "dst2",
			Filter: "select id, val1, sum(val2) as sval2, count(*) as rcount from src2 group by id",
		}, {
			Match:  "dst3",
			Filter: "select id, val from src3 group by id, val",
		}, {
			Match: "/yes",
		}, {
			Match: "/nopk",
		}, {
			Match:  "dst4",
			Filter: "select id1, val from src4 where id2 = 100",
		}, {
			Match:  "dst5",
			Filter: "select id1, val from src5 where val = 'abc'",
		}, {
			Match:  "dstCharset",
			Filter: "select id1, concat(substr(_utf8mb4 val collate utf8mb4_bin,1,1),'abcxyz') val, concat(substr(_utf8mb4 val collate utf8mb4_bin,1,1),'abcxyz') val2 from srcCharset",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}

	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input  string
		output qh.ExpectationSequence
		table  string
		data   [][]string
		logs   []LogExpectation // logs are defined for a few testcases since they are enough to test all log events
	}{{
		// insert with insertNormal
		input: "insert into src1 values(1, 'aaa')",
		output: qh.Expect(
			"begin",
			"insert into dst1(id,val) values (1,'aaa')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst1",
		data: [][]string{
			{"1", "aaa"},
		},
		logs: []LogExpectation{
			{"FIELD", "/src1.*id.*INT32.*val.*VARBINARY.*"},
			{"ROWCHANGE", "insert into dst1(id,val) values (1,'aaa')"},
			{"ROW", "/src1.*3.*1aaa.*"},
		},
	}, {
		// update with insertNormal
		input: "update src1 set val='bbb'",
		output: qh.Expect(
			"begin",
			"update dst1 set val='bbb' where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst1",
		data: [][]string{
			{"1", "bbb"},
		},
		logs: []LogExpectation{
			{"ROWCHANGE", "update dst1 set val='bbb' where id=1"},
			{"ROW", "/src1.*3.*1aaa.*"},
		},
	}, {
		// delete with insertNormal
		input: "delete from src1 where id=1",
		output: qh.Expect(
			"begin",
			"delete from dst1 where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst1",
		data:  [][]string{},
		logs: []LogExpectation{
			{"ROWCHANGE", "delete from dst1 where id=1"},
			{"ROW", "/src1.*3.*1bbb.*"},
		},
	}, {
		// insert with insertOnDup
		input: "insert into src2 values(1, 2, 3)",
		output: qh.Expect(
			"begin",
			"insert into dst2(id,val1,sval2,rcount) values (1,2,ifnull(3, 0),1) on duplicate key update val1=values(val1), sval2=sval2+ifnull(values(sval2), 0), rcount=rcount+1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst2",
		data: [][]string{
			{"1", "2", "3", "1"},
		},
		logs: []LogExpectation{
			{"FIELD", "/src2.*id.*val1.*val2.*"},
			{"ROWCHANGE", "insert into dst2(id,val1,sval2,rcount) values (1,2,ifnull(3, 0),1) on duplicate key update val1=values(val1), sval2=sval2+ifnull(values(sval2), 0), rcount=rcount+1"},
		},
	}, {
		// update with insertOnDup
		input: "update src2 set val1=5, val2=1 where id=1",
		output: qh.Expect(
			"begin",
			"update dst2 set val1=5, sval2=sval2-ifnull(3, 0)+ifnull(1, 0), rcount=rcount where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst2",
		data: [][]string{
			{"1", "5", "1", "1"},
		},
		logs: []LogExpectation{
			{"ROWCHANGE", "update dst2 set val1=5, sval2=sval2-ifnull(3, 0)+ifnull(1, 0), rcount=rcount where id=1"},
			{"ROW", "/src2.*123.*"},
		},
	}, {
		// delete with insertOnDup
		input: "delete from src2 where id=1",
		output: qh.Expect(
			"begin",
			"update dst2 set val1=null, sval2=sval2-ifnull(1, 0), rcount=rcount-1 where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst2",
		data: [][]string{
			{"1", "", "0", "0"},
		},
	}, {
		// insert with insertIgnore
		input: "insert into src3 values(1, 'aaa')",
		output: qh.Expect(
			"begin",
			"insert ignore into dst3(id,val) values (1,'aaa')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst3",
		data: [][]string{
			{"1", "aaa"},
		},
	}, {
		// update with insertIgnore
		input: "update src3 set val='bbb'",
		output: qh.Expect(
			"begin",
			"insert ignore into dst3(id,val) values (1,'bbb')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst3",
		data: [][]string{
			{"1", "aaa"},
		},
	}, {
		// delete with insertIgnore
		input: "delete from src3 where id=1",
		output: qh.Expect(
			"begin",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst3",
		data: [][]string{
			{"1", "aaa"},
		},
	}, {
		// insert: regular expression filter
		input: "insert into yes values(1, 'aaa')",
		output: qh.Expect(
			"begin",
			"insert into yes(id,val) values (1,'aaa')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "yes",
		data: [][]string{
			{"1", "aaa"},
		},
	}, {
		// update: regular expression filter
		input: "update yes set val='bbb'",
		output: qh.Expect(
			"begin",
			"update yes set val='bbb' where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "yes",
		data: [][]string{
			{"1", "bbb"},
		},
	}, {
		// table should not match a rule
		input:  "insert into no values(1, 'aaa')",
		output: qh.ExpectNone(),
	}, {
		// nopk: insert
		input: "insert into nopk values(1, 'aaa')",
		output: qh.Expect(
			"begin",
			"insert into nopk(id,val) values (1,'aaa')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "nopk",
		data: [][]string{
			{"1", "aaa"},
		},
	}, {
		// nopk: update
		input: "update nopk set val='bbb' where id=1",
		output: qh.Expect(
			"begin",
			"delete from nopk where id=1 and val='aaa'",
			"insert into nopk(id,val) values (1,'bbb')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "nopk",
		data: [][]string{
			{"1", "bbb"},
		},
	}, {
		// nopk: delete
		input: "delete from nopk where id=1",
		output: qh.Expect(
			"begin",
			"delete from nopk where id=1 and val='bbb'",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "nopk",
		data:  [][]string{},
	}, {
		// filter by int
		input: "insert into src4 values (1,100,'aaa'),(2,200,'bbb'),(3,100,'ccc')",
		output: qh.Expect(
			"begin",
			"insert into dst4(id1,val) values (1,'aaa')",
			"insert into dst4(id1,val) values (3,'ccc')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst4",
		data:  [][]string{{"1", "aaa"}, {"3", "ccc"}},
	}, {
		// filter by int
		input: "insert into src5 values (1,100,'abc'),(2,200,'xyz'),(3,100,'xyz'),(4,300,'abc'),(5,200,'xyz')",
		output: qh.Expect(
			"begin",
			"insert into dst5(id1,val) values (1,'abc')",
			"insert into dst5(id1,val) values (4,'abc')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst5",
		data:  [][]string{{"1", "abc"}, {"4", "abc"}},
	}, {
		// test collation + filter
		input: "insert into srcCharset values (1,'木元')",
		output: qh.Expect(
			"begin",
			"insert into dstCharset(id1,val,val2) values (1,concat(substr(_utf8mb4 '木元' collate utf8mb4_bin, 1, 1), 'abcxyz'),concat(substr(_utf8mb4 '木元' collate utf8mb4_bin, 1, 1), 'abcxyz'))",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dstCharset",
		data:  [][]string{{"1", "木abcxyz", "木abcxyz"}},
	}}

	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.logs != nil {
				logch := vrLogStatsLogger.Subscribe("vrlogstats")
				defer expectLogsAndUnsubscribe(t, tcase.logs, logch)
			}
			execStatements(t, []string{tcase.input})
			expectDBClientQueries(t, tcase.output)
			if tcase.table != "" {
				expectData(t, tcase.table, tcase.data)
			}
		})
	}
}

func TestPlayerKeywordNames(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table `begin`(`primary` int, `column` varbinary(128), primary key(`primary`))",
		fmt.Sprintf("create table %s.`begin`(`primary` int, `column` varbinary(128), primary key(`primary`))", vrepldb),
		"create table `rollback`(`primary` int, `column` varbinary(128), primary key(`primary`))",
		fmt.Sprintf("create table %s.`rollback`(`primary` int, `column` varbinary(128), primary key(`primary`))", vrepldb),
		"create table `commit`(`primary` int, `column` varbinary(128), primary key(`primary`))",
		fmt.Sprintf("create table %s.`commit`(`primary` int, `column` varbinary(128), primary key(`primary`))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table `begin`",
		fmt.Sprintf("drop table %s.`begin`", vrepldb),
		"drop table `rollback`",
		fmt.Sprintf("drop table %s.`rollback`", vrepldb),
		"drop table `commit`",
		fmt.Sprintf("drop table %s.`commit`", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "begin",
			Filter: "select * from `begin`",
		}, {
			Match:  "rollback",
			Filter: "select `primary`, `column` from `rollback`",
		}, {
			Match:  "commit",
			Filter: "select `primary`+1 as `primary`, concat(`column`, 'a') as `column` from `commit`",
		}},
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}

	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input  string
		output qh.ExpectationSequence
		table  string
		data   [][]string
	}{{
		input: "insert into `begin` values(1, 'aaa')",
		output: qh.Expect(
			"begin",
			"insert into `begin`(`primary`,`column`) values (1,'aaa')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "begin",
		data: [][]string{
			{"1", "aaa"},
		},
	}, {
		input: "update `begin` set `column`='bbb'",
		output: qh.Expect(
			"begin",
			"update `begin` set `column`='bbb' where `primary`=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "begin",
		data: [][]string{
			{"1", "bbb"},
		},
	}, {
		input: "delete from `begin` where `primary`=1",
		output: qh.Expect(
			"begin",
			"delete from `begin` where `primary`=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "begin",
		data:  [][]string{},
	}, {
		input: "insert into `rollback` values(1, 'aaa')",
		output: qh.Expect(
			"begin",
			"insert into `rollback`(`primary`,`column`) values (1,'aaa')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "rollback",
		data: [][]string{
			{"1", "aaa"},
		},
	}, {
		input: "update `rollback` set `column`='bbb'",
		output: qh.Expect(
			"begin",
			"update `rollback` set `column`='bbb' where `primary`=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "rollback",
		data: [][]string{
			{"1", "bbb"},
		},
	}, {
		input: "delete from `rollback` where `primary`=1",
		output: qh.Expect(
			"begin",
			"delete from `rollback` where `primary`=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "rollback",
		data:  [][]string{},
	}, {
		input: "insert into `commit` values(1, 'aaa')",
		output: qh.Expect(
			"begin",
			"insert into `commit`(`primary`,`column`) values (1 + 1,concat('aaa', 'a'))",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "commit",
		data: [][]string{
			{"2", "aaaa"},
		},
	}, {
		input: "update `commit` set `column`='bbb' where `primary`=1",
		output: qh.Expect(
			"begin",
			"update `commit` set `column`=concat('bbb', 'a') where `primary`=(1 + 1)",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "commit",
		data: [][]string{
			{"2", "bbba"},
		},
	}, {
		input: "update `commit` set `primary`=2 where `primary`=1",
		output: qh.Expect(
			"begin",
			"delete from `commit` where `primary`=(1 + 1)",
			"insert into `commit`(`primary`,`column`) values (2 + 1,concat('bbb', 'a'))",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "commit",
		data: [][]string{
			{"3", "bbba"},
		},
	}, {
		input: "delete from `commit` where `primary`=2",
		output: qh.Expect(
			"begin",
			"delete from `commit` where `primary`=(2 + 1)",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "commit",
		data:  [][]string{},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		expectDBClientQueries(t, tcases.output)
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
	}
}

var shardedVSchema = `{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "src1": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ]
    }
  }
}`

func TestPlayerKeyspaceID(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.dst1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.dst1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	defer env.SetVSchema("{}")

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select id, keyspace_id() as val from src1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input  string
		output qh.ExpectationSequence
		table  string
		data   [][]string
	}{{
		// insert with insertNormal
		input: "insert into src1 values(1, 'aaa')",
		output: qh.Expect(
			"begin",
			"insert into dst1(id,val) values (1,'\x16k@\xb4J\xbaK\xd6')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst1",
		data: [][]string{
			{"1", "\x16k@\xb4J\xbaK\xd6"},
		},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		expectDBClientQueries(t, tcases.output)
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
	}
}

func TestUnicode(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varchar(128) COLLATE utf8_unicode_ci, primary key(id))",
		fmt.Sprintf("create table %s.dst1(id int, val varchar(128) COLLATE utf8_unicode_ci, primary key(id)) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.dst1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select * from src1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input  string
		output qh.ExpectationSequence
		table  string
		data   [][]string
	}{{
		// insert with insertNormal
		input: "insert into src1 values(1, '👍')",
		output: qh.Expect(
			"begin",
			// We should expect the "Mojibaked" version.
			"insert into dst1(id,val) values (1,'ðŸ‘\u008d')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst1",
		data: [][]string{
			{"1", "👍"},
		},
	}}

	// We need a latin1 connection.
	conn, err := env.Mysqld.GetDbaConnection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.ExecuteFetch("set names latin1", 10000, false); err != nil {
		t.Fatal(err)
	}

	for _, tcases := range testcases {
		if _, err := conn.ExecuteFetch(tcases.input, 10000, false); err != nil {
			t.Error(err)
		}
		expectDBClientQueries(t, tcases.output)
		if tcases.table != "" {
			customExpectData(t, tcases.table, tcases.data, func(ctx context.Context, query string) (*sqltypes.Result, error) {
				return conn.ExecuteFetch(query, 10000, true)
			})
		}
	}
}

func TestPlayerUpdates(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, grouped int, ungrouped int, summed int, primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, grouped int, ungrouped int, summed int, rcount int, primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id, grouped, ungrouped, sum(summed) as summed, count(*) as rcount from t1 group by id, grouped",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input  string
		output string
		table  string
		data   [][]string
	}{{
		// Start with all nulls
		input:  "insert into t1 values(1, null, null, null)",
		output: "insert into t1(id,grouped,ungrouped,summed,rcount) values (1,null,null,ifnull(null, 0),1) on duplicate key update ungrouped=values(ungrouped), summed=summed+ifnull(values(summed), 0), rcount=rcount+1",
		table:  "t1",
		data: [][]string{
			{"1", "", "", "0", "1"},
		},
	}, {
		// null to null values
		input:  "update t1 set grouped=1 where id=1",
		output: "update t1 set ungrouped=null, summed=summed-ifnull(null, 0)+ifnull(null, 0), rcount=rcount where id=1",
		table:  "t1",
		data: [][]string{
			{"1", "", "", "0", "1"},
		},
	}, {
		// null to non-null values
		input:  "update t1 set ungrouped=1, summed=1 where id=1",
		output: "update t1 set ungrouped=1, summed=summed-ifnull(null, 0)+ifnull(1, 0), rcount=rcount where id=1",
		table:  "t1",
		data: [][]string{
			{"1", "", "1", "1", "1"},
		},
	}, {
		// non-null to non-null values
		input:  "update t1 set ungrouped=2, summed=2 where id=1",
		output: "update t1 set ungrouped=2, summed=summed-ifnull(1, 0)+ifnull(2, 0), rcount=rcount where id=1",
		table:  "t1",
		data: [][]string{
			{"1", "", "2", "2", "1"},
		},
	}, {
		// non-null to null values
		input:  "update t1 set ungrouped=null, summed=null where id=1",
		output: "update t1 set ungrouped=null, summed=summed-ifnull(2, 0)+ifnull(null, 0), rcount=rcount where id=1",
		table:  "t1",
		data: [][]string{
			{"1", "", "", "0", "1"},
		},
	}, {
		// insert non-null values
		input:  "insert into t1 values(2, 2, 3, 4)",
		output: "insert into t1(id,grouped,ungrouped,summed,rcount) values (2,2,3,ifnull(4, 0),1) on duplicate key update ungrouped=values(ungrouped), summed=summed+ifnull(values(summed), 0), rcount=rcount+1",
		table:  "t1",
		data: [][]string{
			{"1", "", "", "0", "1"},
			{"2", "2", "3", "4", "1"},
		},
	}, {
		// delete non-null values
		input:  "delete from t1 where id=2",
		output: "update t1 set ungrouped=null, summed=summed-ifnull(4, 0), rcount=rcount-1 where id=2",
		table:  "t1",
		data: [][]string{
			{"1", "", "", "0", "1"},
			{"2", "2", "", "0", "0"},
		},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		output := qh.Expect(
			"begin",
			tcases.output,
			"/update _vt.vreplication set pos=",
			"commit",
		)
		if tcases.output == "" {
			output = qh.Expect(
				"begin",
				"/update _vt.vreplication set pos=",
				"commit",
			)
		}
		expectDBClientQueries(t, output)
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
	}
	validateQueryCountStat(t, "replicate", 7)
}

func TestPlayerRowMove(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src(id int, val1 int, val2 int, primary key(id))",
		fmt.Sprintf("create table %s.dst(val1 int, sval2 int, rcount int, primary key(val1))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src",
		fmt.Sprintf("drop table %s.dst", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst",
			Filter: "select val1, sum(val2) as sval2, count(*) as rcount from src group by val1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	execStatements(t, []string{
		"insert into src values(1, 1, 1), (2, 2, 2), (3, 2, 3)",
	})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into dst(val1,sval2,rcount) values (1,ifnull(1, 0),1) on duplicate key update sval2=sval2+ifnull(values(sval2), 0), rcount=rcount+1",
		"insert into dst(val1,sval2,rcount) values (2,ifnull(2, 0),1) on duplicate key update sval2=sval2+ifnull(values(sval2), 0), rcount=rcount+1",
		"insert into dst(val1,sval2,rcount) values (2,ifnull(3, 0),1) on duplicate key update sval2=sval2+ifnull(values(sval2), 0), rcount=rcount+1",
		"/update _vt.vreplication set pos=",
		"commit",
	))
	expectData(t, "dst", [][]string{
		{"1", "1", "1"},
		{"2", "5", "2"},
	})
	validateQueryCountStat(t, "replicate", 3)

	execStatements(t, []string{
		"update src set val1=1, val2=4 where id=3",
	})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"update dst set sval2=sval2-ifnull(3, 0), rcount=rcount-1 where val1=2",
		"insert into dst(val1,sval2,rcount) values (1,ifnull(4, 0),1) on duplicate key update sval2=sval2+ifnull(values(sval2), 0), rcount=rcount+1",
		"/update _vt.vreplication set pos=",
		"commit",
	))
	expectData(t, "dst", [][]string{
		{"1", "5", "2"},
		{"2", "2", "1"},
	})
	validateQueryCountStat(t, "replicate", 5)
}

func TestPlayerTypes(t *testing.T) {
	defer deleteTablet(addTablet(100))
	execStatements(t, []string{
		"create table vitess_ints(tiny tinyint, tinyu tinyint unsigned, small smallint, smallu smallint unsigned, medium mediumint, mediumu mediumint unsigned, normal int, normalu int unsigned, big bigint, bigu bigint unsigned, y year, primary key(tiny))",
		fmt.Sprintf("create table %s.vitess_ints(tiny tinyint, tinyu tinyint unsigned, small smallint, smallu smallint unsigned, medium mediumint, mediumu mediumint unsigned, normal int, normalu int unsigned, big bigint, bigu bigint unsigned, y year, primary key(tiny))", vrepldb),
		"create table vitess_fracts(id int, deci decimal(5,2), num numeric(5,2), f float, d double, primary key(id))",
		fmt.Sprintf("create table %s.vitess_fracts(id int, deci decimal(5,2), num numeric(5,2), f float, d double, primary key(id))", vrepldb),
		"create table vitess_strings(vb varbinary(16), c char(16), vc varchar(16), b binary(5), tb tinyblob, bl blob, ttx tinytext, tx text, en enum('a','b'), s set('a','b'), primary key(vb))",
		fmt.Sprintf("create table %s.vitess_strings(vb varbinary(16), c char(16), vc varchar(16), b binary(5), tb tinyblob, bl blob, ttx tinytext, tx text, en enum('a','b'), s set('a','b'), primary key(vb))", vrepldb),
		"create table vitess_misc(id int, b bit(8), d date, dt datetime, t time, g geometry, primary key(id))",
		fmt.Sprintf("create table %s.vitess_misc(id int, b bit(8), d date, dt datetime, t time, g geometry, primary key(id))", vrepldb),
		"create table vitess_null(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.vitess_null(id int, val varbinary(128), primary key(id))", vrepldb),
		"create table src1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.src1(id int, val varbinary(128), primary key(id))", vrepldb),
		"create table binary_pk(b binary(4), val varbinary(4), primary key(b))",
		fmt.Sprintf("create table %s.binary_pk(b binary(4), val varbinary(4), primary key(b))", vrepldb),
		"create table vitess_decimal(id int, d1 decimal(8,0) default null, d2 decimal(8,0) default null, d3 decimal(8,0) default null, d4 decimal(8, 1), d5 decimal(8, 1), d6 decimal(8, 1), primary key(id))",
		fmt.Sprintf("create table %s.vitess_decimal(id int, d1 decimal(8,0) default null, d2 decimal(8,0) default null, d3 decimal(8,0) default null, d4 decimal(8, 1), d5 decimal(8, 1), d6 decimal(8, 1), primary key(id))", vrepldb),
		"create table vitess_json(id int auto_increment, val1 json, val2 json, val3 json, val4 json, val5 json, primary key(id))",
		fmt.Sprintf("create table %s.vitess_json(id int, val1 json, val2 json, val3 json, val4 json, val5 json, primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table vitess_ints",
		fmt.Sprintf("drop table %s.vitess_ints", vrepldb),
		"drop table vitess_fracts",
		fmt.Sprintf("drop table %s.vitess_fracts", vrepldb),
		"drop table vitess_strings",
		fmt.Sprintf("drop table %s.vitess_strings", vrepldb),
		"drop table vitess_misc",
		fmt.Sprintf("drop table %s.vitess_misc", vrepldb),
		"drop table vitess_null",
		fmt.Sprintf("drop table %s.vitess_null", vrepldb),
		"drop table src1",
		fmt.Sprintf("drop table %s.src1", vrepldb),
		"drop table binary_pk",
		fmt.Sprintf("drop table %s.binary_pk", vrepldb),
		"drop table vitess_decimal",
		fmt.Sprintf("drop table %s.vitess_decimal", vrepldb),
		"drop table vitess_json",
		fmt.Sprintf("drop table %s.vitess_json", vrepldb),
	})

	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()
	type testcase struct {
		input  string
		output string
		table  string
		data   [][]string
	}
	testcases := []testcase{{
		input:  "insert into vitess_ints values(-128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 2012)",
		output: "insert into vitess_ints(tiny,tinyu,small,smallu,medium,mediumu,normal,normalu,big,bigu,y) values (-128,255,-32768,65535,-8388608,16777215,-2147483648,4294967295,-9223372036854775808,18446744073709551615,2012)",
		table:  "vitess_ints",
		data: [][]string{
			{"-128", "255", "-32768", "65535", "-8388608", "16777215", "-2147483648", "4294967295", "-9223372036854775808", "18446744073709551615", "2012"},
		},
	}, {
		input:  "insert into vitess_fracts values(1, 1.99, 2.99, 3.99, 4.99)",
		output: "insert into vitess_fracts(id,deci,num,f,d) values (1,1.99,2.99,3.99E+00,4.99E+00)",
		table:  "vitess_fracts",
		data: [][]string{
			{"1", "1.99", "2.99", "3.99", "4.99"},
		},
	}, {
		input:  "insert into vitess_strings values('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'a', 'a,b')",
		output: "insert into vitess_strings(vb,c,vc,b,tb,bl,ttx,tx,en,s) values ('a','b','c','d\\0\\0\\0\\0','e','f','g','h',1,'3')",
		table:  "vitess_strings",
		data: [][]string{
			{"a", "b", "c", "d\000\000\000\000", "e", "f", "g", "h", "a", "a,b"},
		},
	}, {
		input:  "insert into vitess_misc values(1, '\x01', '2012-01-01', '2012-01-01 15:45:45', '15:45:45', point(1, 2))",
		output: "insert into vitess_misc(id,b,d,dt,t,g) values (1,b'00000001','2012-01-01','2012-01-01 15:45:45','15:45:45','\\0\\0\\0\\0\x01\x01\\0\\0\\0\\0\\0\\0\\0\\0\\0\xf0?\\0\\0\\0\\0\\0\\0\\0@')",
		table:  "vitess_misc",
		data: [][]string{
			{"1", "\x01", "2012-01-01", "2012-01-01 15:45:45", "15:45:45", "\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"},
		},
	}, {
		input:  "insert into vitess_null values(1, null)",
		output: "insert into vitess_null(id,val) values (1,null)",
		table:  "vitess_null",
		data: [][]string{
			{"1", ""},
		},
	}, {
		input:  "insert into binary_pk values('a', 'aaa')",
		output: "insert into binary_pk(b,val) values ('a\\0\\0\\0','aaa')",
		table:  "binary_pk",
		data: [][]string{
			{"a\000\000\000", "aaa"},
		},
	}, {
		input:  "insert into vitess_decimal values(1, 0, 1, null, 0, 1.1, 1)",
		output: "insert into vitess_decimal(id,d1,d2,d3,d4,d5,d6) values (1,0,1,null,.0,1.1,1.0)",
		table:  "vitess_decimal",
		data: [][]string{
			{"1", "0", "1", "", "0.0", "1.1", "1.0"},
		},
	}, {
		// Binary pk is a special case: https://github.com/vitessio/vitess/issues/3984
		input:  "update binary_pk set val='bbb' where b='a\\0\\0\\0'",
		output: "update binary_pk set val='bbb' where b='a\\0\\0\\0'",
		table:  "binary_pk",
		data: [][]string{
			{"a\000\000\000", "bbb"},
		},
	}, {
		input:  "insert into vitess_json(val1,val2,val3,val4,val5) values (null,'{}','123','{\"a\":[42,100]}','{\"foo\": \"bar\"}')",
		output: "insert into vitess_json(id,val1,val2,val3,val4,val5) values (1,CAST(null as JSON),JSON_OBJECT(),CAST(123 as JSON),JSON_OBJECT(_utf8mb4'a', JSON_ARRAY(42, 100)),JSON_OBJECT(_utf8mb4'foo', _utf8mb4'bar'))",
		table:  "vitess_json",
		data: [][]string{
			{"1", "", "{}", "123", `{"a": [42, 100]}`, `{"foo": "bar"}`},
		},
	}, {
		input:  "update vitess_json set val1 = '{\"bar\": \"foo\"}', val4 = '{\"a\": [98, 123]}', val5 = convert(x'7b7d' using utf8mb4)",
		output: "update vitess_json set val1=JSON_OBJECT(_utf8mb4'bar', _utf8mb4'foo'), val2=JSON_OBJECT(), val3=CAST(123 as JSON), val4=JSON_OBJECT(_utf8mb4'a', JSON_ARRAY(98, 123)), val5=JSON_OBJECT() where id=1",
		table:  "vitess_json",
		data: [][]string{
			{"1", `{"bar": "foo"}`, "{}", "123", `{"a": [98, 123]}`, `{}`},
		},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		want := qh.Expect(
			"begin",
			tcases.output,
			"/update _vt.vreplication set pos=",
			"commit",
		)
		expectDBClientQueries(t, want)
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
	}
}

func TestPlayerDDL(t *testing.T) {
	defer deleteTablet(addTablet(100))
	execStatements(t, []string{
		"create table t1(id int, primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	// Issue a dummy change to ensure vreplication is initialized. Otherwise there
	// is a race between the DDLs and the schema loader of vstreamer.
	// Root cause seems to be with MySQL where t1 shows up in information_schema before
	// the actual table is created.
	execStatements(t, []string{"insert into t1 values(1)"})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into t1(id) values (1)",
		"/update _vt.vreplication set pos=",
		"commit",
	))

	execStatements(t, []string{"alter table t1 add column val varchar(128)"})
	execStatements(t, []string{"alter table t1 drop column val"})
	expectDBClientQueries(t, qh.Expect(
		"/update _vt.vreplication set pos=",
		"/update _vt.vreplication set pos=",
	))
	cancel()
	bls = &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_STOP,
	}
	cancel, id := startVReplication(t, bls, "")
	pos0 := primaryPosition(t) //For debugging only
	execStatements(t, []string{"alter table t1 add column val varchar(128)"})
	pos1 := primaryPosition(t)
	// The stop position must be the GTID of the first DDL
	expectDBClientQueries(t, qh.Expect(
		"begin",
		fmt.Sprintf("/update _vt.vreplication set pos='%s'", pos1),
		"/update _vt.vreplication set state='Stopped'",
		"commit",
	))
	pos2b := primaryPosition(t)
	execStatements(t, []string{"alter table t1 drop column val"})
	pos2 := primaryPosition(t)
	log.Errorf("Expected log:: TestPlayerDDL Positions are: before first alter %v, after first alter %v, before second alter %v, after second alter %v",
		pos0, pos1, pos2b, pos2) //For debugging only: to check what are the positions when test works and if/when it fails
	// Restart vreplication
	if _, err := playerEngine.Exec(fmt.Sprintf(`update _vt.vreplication set state = 'Running', message='' where id=%d`, id)); err != nil {
		t.Fatal(err)
	}
	// It should stop at the next DDL
	expectDBClientQueries(t, qh.Expect(
		"/update.*'Running'",
		// Second update is from vreplicator.
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update.*'Running'",
		"begin",
		fmt.Sprintf("/update.*'%s'", pos2),
		"/update _vt.vreplication set state='Stopped'",
		"commit",
	))
	cancel()
	bls = &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_EXEC,
	}
	execStatements(t, []string{fmt.Sprintf("alter table %s.t1 add column val2 varchar(128)", vrepldb)})
	cancel, _ = startVReplication(t, bls, "")
	execStatements(t, []string{"alter table t1 add column val1 varchar(128)"})
	expectDBClientQueries(t, qh.Expect(
		"alter table t1 add column val1 varchar(128)",
		"/update _vt.vreplication set pos=",
		// The apply of the DDL on target generates an "other" event.
		"/update _vt.vreplication set pos=",
	))
	execStatements(t, []string{"alter table t1 add column val2 varchar(128)"})
	expectDBClientQueries(t, qh.Expect(
		"alter table t1 add column val2 varchar(128)",
		"/update _vt.vreplication set message='Duplicate",
		"/update _vt.vreplication set state='Error', message='Duplicate",
	))
	cancel()

	execStatements(t, []string{
		"alter table t1 drop column val1",
		"alter table t1 drop column val2",
		fmt.Sprintf("alter table %s.t1 drop column val1", vrepldb),
	})

	bls = &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_EXEC_IGNORE,
	}
	execStatements(t, []string{fmt.Sprintf("create table %s.t2(id int, primary key(id))", vrepldb)})
	defer execStatements(t, []string{fmt.Sprintf("drop table %s.t2", vrepldb)})

	cancel, _ = startVReplication(t, bls, "")
	execStatements(t, []string{"alter table t1 add column val1 varchar(128)"})
	expectDBClientQueries(t, qh.Expect(
		"alter table t1 add column val1 varchar(128)",
		"/update _vt.vreplication set pos=",
		// The apply of the DDL on target generates an "other" event.
		"/update _vt.vreplication set pos=",
	))
	execStatements(t, []string{"alter table t1 add column val2 varchar(128)"})
	expectDBClientQueries(t, qh.Expect(
		"alter table t1 add column val2 varchar(128)",
		"/update _vt.vreplication set pos=",
	))
	cancel()
}

func TestGTIDCompress(t *testing.T) {
	ctx := context.Background()
	defer deleteTablet(addTablet(100))
	err := env.Mysqld.ExecuteSuperQuery(ctx, "insert into _vt.vreplication (id, workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state,db_name) values (1, '', '', '', 0,0,0,0,'Stopped','')")
	require.NoError(t, err)

	type testCase struct {
		name, gtid string
		compress   bool
	}

	testCases := []testCase{
		{"cleartext1", "MySQL56/14b68925-696a-11ea-aee7-fec597a91f5e:1-308092", false},
		{"cleartext2", "MySQL56/14b68925-696a-11ea-aee7-fec597a91f5e:1-308092,320a5e98-6965-11ea-b949-eeafd34ae6e4:1-3,81cbdbf8-6969-11ea-aeb1-a6143b021f67:1-524891956,c9a0f301-6965-11ea-ba9d-02c229065569:1-3,cb698dac-6969-11ea-ac38-16e5d0ac5c3a:1-524441991,e39fca4d-6960-11ea-b4c2-1e895fd49fa0:1-3", false},
		{"compress1", "MySQL56/14b68925-696a-11ea-aee7-fec597a91f5e:1-308092", true},
		{"compress2", "MySQL56/14b68925-696a-11ea-aee7-fec597a91f5e:1-308092,320a5e98-6965-11ea-b949-eeafd34ae6e4:1-3,81cbdbf8-6969-11ea-aeb1-a6143b021f67:1-524891956,c9a0f301-6965-11ea-ba9d-02c229065569:1-3,cb698dac-6969-11ea-ac38-16e5d0ac5c3a:1-524441991,e39fca4d-6960-11ea-b4c2-1e895fd49fa0:1-3", true},
		{"nil-compress", "", true},
		{"nil-clear", "", false},
	}
	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			strGTID := fmt.Sprintf("'%s'", tCase.gtid)
			if tCase.compress {
				strGTID = fmt.Sprintf("compress(%s)", strGTID)
			}
			err := env.Mysqld.ExecuteSuperQuery(ctx, fmt.Sprintf("update _vt.vreplication set pos=%s where id = 1", strGTID))
			require.NoError(t, err)
			qr, err := env.Mysqld.FetchSuperQuery(ctx, "select pos from _vt.vreplication where id = 1")
			require.NoError(t, err)
			require.NotNil(t, qr)
			require.Equal(t, 1, len(qr.Rows))
			gotGTID := qr.Rows[0][0].ToString()
			pos, err := mysql.DecodePosition(gotGTID)
			if tCase.compress {
				require.True(t, pos.IsZero())
				pos, err = binlogplayer.DecodePosition(gotGTID)
				require.NoError(t, err)
				require.NotNil(t, pos)
				tpos, err := mysql.DecodePosition(tCase.gtid)
				require.NoError(t, err)
				require.Equal(t, tpos.String(), pos.String())
			} else {
				require.NoError(t, err)
				require.NotNil(t, pos)
				require.Equal(t, tCase.gtid, gotGTID)
			}
		})
	}
}

func TestPlayerStopPos(t *testing.T) {
	defer deleteTablet(addTablet(100))
	vreplicationStoreCompressedGTID = true
	defer func() {
		vreplicationStoreCompressedGTID = false
	}()
	execStatements(t, []string{
		"create table yes(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.yes(id int, val varbinary(128), primary key(id))", vrepldb),
		"create table no(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table yes",
		fmt.Sprintf("drop table %s.yes", vrepldb),
		"drop table no",
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/yes",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	startPos := primaryPosition(t)
	query := binlogplayer.CreateVReplicationState("test", bls, startPos, binlogplayer.BlpStopped, vrepldb, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	id := int32(qr.InsertID)
	for q := range globalDBQueries {
		if strings.HasPrefix(q, "insert into _vt.vreplication") {
			break
		}
	}

	// Test normal stop.
	execStatements(t, []string{
		"insert into yes values(1, 'aaa')",
	})
	stopPos := primaryPosition(t)
	query = binlogplayer.StartVReplicationUntil(id, stopPos)
	if _, err := playerEngine.Exec(query); err != nil {
		t.Fatal(err)
	}
	expectDBClientQueries(t, qh.Expect(
		"/update.*'Running'",
		// Second update is from vreplicator.
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update.*'Running'",
		"begin",
		"insert into yes(id,val) values (1,'aaa')",
		fmt.Sprintf("/update.*compress.*'%s'", stopPos),
		"/update.*'Stopped'",
		"commit",
	))

	// Test stopping at empty transaction.
	execStatements(t, []string{
		"insert into no values(2, 'aaa')",
		"insert into no values(3, 'aaa')",
	})
	stopPos = primaryPosition(t)
	execStatements(t, []string{
		"insert into no values(4, 'aaa')",
	})
	query = binlogplayer.StartVReplicationUntil(id, stopPos)
	if _, err := playerEngine.Exec(query); err != nil {
		t.Fatal(err)
	}
	expectDBClientQueries(t, qh.Expect(
		"/update.*'Running'",
		// Second update is from vreplicator.
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update.*'Running'",
		"begin",
		// Since 'no' generates empty transactions that are skipped by
		// vplayer, a commit is done only for the stop position event.
		fmt.Sprintf("/update.*'%s'", stopPos),
		"/update.*'Stopped'",
		"commit",
	))

	// Test stopping when position is already reached.
	query = binlogplayer.StartVReplicationUntil(id, stopPos)
	if _, err := playerEngine.Exec(query); err != nil {
		t.Fatal(err)
	}
	expectDBClientQueries(t, qh.Expect(
		"/update.*'Running'",
		// Second update is from vreplicator.
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update.*'Running'",
		"/update.*'Stopped'.*already reached",
	))
}

func TestPlayerStopAtOther(t *testing.T) {
	t.Skip("This test was written to verify a bug fix, but is extremely flaky. Only a manual test is possible")

	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	// Insert a source row.
	execStatements(t, []string{
		"insert into t1 values(1, 'aaa')",
	})
	startPos := primaryPosition(t)
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	query := binlogplayer.CreateVReplicationState("test", bls, startPos, binlogplayer.BlpStopped, vrepldb, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	id := int32(qr.InsertID)
	for q := range globalDBQueries {
		if strings.HasPrefix(q, "insert into _vt.vreplication") {
			break
		}
	}
	defer func() {
		if _, err := playerEngine.Exec(fmt.Sprintf("delete from _vt.vreplication where id = %d", id)); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	vconn := &realDBClient{nolog: true}
	if err := vconn.Connect(); err != nil {
		t.Error(err)
	}
	defer vconn.Close()

	// Insert the same row on the target and lock it.
	if _, err := vconn.ExecuteFetch("insert into t1 values(1, 'aaa')", 1); err != nil {
		t.Error(err)
	}
	if _, err := vconn.ExecuteFetch("begin", 1); err != nil {
		t.Error(err)
	}
	if _, err := vconn.ExecuteFetch("update t1 set val='bbb' where id=1", 1); err != nil {
		t.Error(err)
	}

	// Start a VReplication where the first transaction updates the locked row.
	// It will cause the apply to wait, which will cause the other two events
	// to accumulate. The stop position will be on the grant.
	// We're testing the behavior where an OTHER transaction is part of a batch,
	// we have to commit its stop position correctly.
	execStatements(t, []string{
		"update t1 set val='ccc' where id=1",
		"insert into t1 values(2, 'ddd')",
		"grant select on *.* to 'vt_app'@'127.0.0.1'",
	})
	stopPos := primaryPosition(t)
	query = binlogplayer.StartVReplicationUntil(id, stopPos)
	if _, err := playerEngine.Exec(query); err != nil {
		t.Fatal(err)
	}

	// Wait for the begin. The update will be blocked.
	expectDBClientQueries(t, qh.Expect(
		"/update.*'Running'",
		// Second update is from vreplicator.
		"/update.*'Running'",
		"begin",
	))

	// Give time for the other two transactions to reach the relay log.
	time.Sleep(100 * time.Millisecond)
	_, _ = vconn.ExecuteFetch("rollback", 1)

	// This is approximately the expected sequence of updates.
	expectDBClientQueries(t, qh.Expect(
		"update t1 set val='ccc' where id=1",
		"/update _vt.vreplication set pos=",
		"commit",
		"begin",
		"insert into t1(id,val) values (2,'ddd')",
		"/update _vt.vreplication set pos=",
		"commit",
		fmt.Sprintf("/update _vt.vreplication set pos='%s'", stopPos),
		"/update.*'Stopped'",
	))
}

func TestPlayerIdleUpdate(t *testing.T) {
	defer deleteTablet(addTablet(100))

	savedIdleTimeout := idleTimeout
	defer func() { idleTimeout = savedIdleTimeout }()
	idleTimeout = 100 * time.Millisecond

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	execStatements(t, []string{
		"insert into t1 values(1, 'aaa')",
	})
	start := time.Now()
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into t1(id,val) values (1,'aaa')",
		"/update _vt.vreplication set pos=",
		"commit",
	),
		"/update _vt.vreplication set pos=",
	)
	// The above write will generate a new binlog event, and
	// that event will loopback into player as an empty event.
	// But it must not get saved until idleTimeout has passed.
	// The exact positions are hard to verify because of this
	// loopback mechanism.
	expectDBClientQueries(t, qh.Expect(
		"/update _vt.vreplication set pos=",
	))
	if duration := time.Since(start); duration < idleTimeout {
		t.Errorf("duration: %v, must be at least %v", duration, idleTimeout)
	}
}

func TestPlayerSplitTransaction(t *testing.T) {
	defer deleteTablet(addTablet(100))
	setFlag("vstream_packet_size", "10")
	defer setFlag("vstream_packet_size", "10000")

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	execStatements(t, []string{
		"begin",
		"insert into t1 values(1, '123456')",
		"insert into t1 values(2, '789012')",
		"commit",
	})
	// Because the packet size is 10, this is received as two events,
	// but still combined as one transaction.
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into t1(id,val) values (1,'123456')",
		"insert into t1(id,val) values (2,'789012')",
		"/update _vt.vreplication set pos=",
		"commit",
	))
}

func TestPlayerLockErrors(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	execStatements(t, []string{
		"begin",
		"insert into t1 values(1, 'aaa')",
		"insert into t1 values(2, 'bbb')",
		"commit",
	})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into t1(id,val) values (1,'aaa')",
		"insert into t1(id,val) values (2,'bbb')",
		"/update _vt.vreplication set pos=",
		"commit",
	))

	vconn := &realDBClient{nolog: true}
	if err := vconn.Connect(); err != nil {
		t.Error(err)
	}
	defer vconn.Close()

	// Start a transaction and lock the second row.
	if _, err := vconn.ExecuteFetch("begin", 1); err != nil {
		t.Error(err)
	}
	if _, err := vconn.ExecuteFetch("update t1 set val='bbb' where id=2", 1); err != nil {
		t.Error(err)
	}

	execStatements(t, []string{
		"begin",
		"update t1 set val='ccc' where id=1",
		"update t1 set val='ccc' where id=2",
		"commit",
	})
	// The innodb lock wait timeout is set to 1s.
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"update t1 set val='ccc' where id=1",
		"update t1 set val='ccc' where id=2",
		"rollback",
	))

	// Release the lock, and watch the retry go through.
	_, _ = vconn.ExecuteFetch("rollback", 1)
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"update t1 set val='ccc' where id=1",
		"update t1 set val='ccc' where id=2",
		"/update _vt.vreplication set pos=",
		"commit",
	))
}

func TestPlayerCancelOnLock(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	execStatements(t, []string{
		"begin",
		"insert into t1 values(1, 'aaa')",
		"commit",
	})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into t1(id,val) values (1,'aaa')",
		"/update _vt.vreplication set pos=",
		"commit",
	))

	vconn := &realDBClient{nolog: true}
	if err := vconn.Connect(); err != nil {
		t.Error(err)
	}
	defer vconn.Close()

	// Start a transaction and lock the row.
	if _, err := vconn.ExecuteFetch("begin", 1); err != nil {
		t.Error(err)
	}
	if _, err := vconn.ExecuteFetch("update t1 set val='bbb' where id=1", 1); err != nil {
		t.Error(err)
	}

	execStatements(t, []string{
		"begin",
		"update t1 set val='ccc' where id=1",
		"commit",
	})
	// The innodb lock wait timeout is set to 1s.
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"update t1 set val='ccc' where id=1",
		"rollback",
	))

	// VReplication should not get stuck if you cancel now.
	done := make(chan bool)
	go func() {
		cancel()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("cancel is hung")
	}
}

func TestPlayerBatching(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_EXEC,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	execStatements(t, []string{
		"insert into t1 values(1, 'aaa')",
	})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into t1(id,val) values (1,'aaa')",
		"/update _vt.vreplication set pos=",
		"commit",
	))

	vconn := &realDBClient{nolog: true}
	if err := vconn.Connect(); err != nil {
		t.Error(err)
	}
	defer vconn.Close()

	// Start a transaction and lock the row.
	if _, err := vconn.ExecuteFetch("begin", 1); err != nil {
		t.Error(err)
	}
	if _, err := vconn.ExecuteFetch("update t1 set val='bbb' where id=1", 1); err != nil {
		t.Error(err)
	}

	// create one transaction
	execStatements(t, []string{
		"update t1 set val='ccc' where id=1",
	})
	// Wait for the begin. The update will be blocked.
	expectDBClientQueries(t, qh.Expect(
		"begin",
	))

	// Create two more transactions. They will go and wait in the relayLog.
	execStatements(t, []string{
		"insert into t1 values(2, 'aaa')",
		"insert into t1 values(3, 'aaa')",
		"alter table t1 add column val2 varbinary(128)",
		"alter table t1 drop column val2",
	})

	// Release the lock.
	_, _ = vconn.ExecuteFetch("rollback", 1)
	// First transaction will complete. The other two
	// transactions must be batched into one. But the
	// DDLs should be on their own.
	expectDBClientQueries(t, qh.Expect(
		"update t1 set val='ccc' where id=1",
		"/update _vt.vreplication set pos=",
		"commit",
		"begin",
		"insert into t1(id,val) values (2,'aaa')",
		"insert into t1(id,val) values (3,'aaa')",
		"/update _vt.vreplication set pos=",
		"commit",
		"alter table t1 add column val2 varbinary(128)",
		"/update _vt.vreplication set pos=",
		"alter table t1 drop column val2",
		"/update _vt.vreplication set pos=",
		// The apply of the DDLs on target generates two "other" event.
		"/update _vt.vreplication set pos=",
		"/update _vt.vreplication set pos=",
	))
}

func TestPlayerRelayLogMaxSize(t *testing.T) {
	defer deleteTablet(addTablet(100))

	for i := 0; i < 2; i++ {
		// First iteration checks max size, second checks max items
		func() {
			switch i {
			case 0:
				savedSize := relayLogMaxSize
				defer func() { relayLogMaxSize = savedSize }()
				relayLogMaxSize = 10
			case 1:
				savedLen := relayLogMaxItems
				defer func() { relayLogMaxItems = savedLen }()
				relayLogMaxItems = 2
			}

			execStatements(t, []string{
				"create table t1(id int, val varbinary(128), primary key(id))",
				fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
			})
			defer execStatements(t, []string{
				"drop table t1",
				fmt.Sprintf("drop table %s.t1", vrepldb),
			})
			env.SchemaEngine.Reload(context.Background())

			filter := &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match: "/.*",
				}},
			}
			bls := &binlogdatapb.BinlogSource{
				Keyspace: env.KeyspaceName,
				Shard:    env.ShardName,
				Filter:   filter,
				OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
			}
			cancel, _ := startVReplication(t, bls, "")
			defer cancel()

			execStatements(t, []string{
				"insert into t1 values(1, '123456')",
			})
			expectDBClientQueries(t, qh.Expect(
				"begin",
				"insert into t1(id,val) values (1,'123456')",
				"/update _vt.vreplication set pos=",
				"commit",
			))

			vconn := &realDBClient{nolog: true}
			if err := vconn.Connect(); err != nil {
				t.Error(err)
			}
			defer vconn.Close()

			// Start a transaction and lock the row.
			if _, err := vconn.ExecuteFetch("begin", 1); err != nil {
				t.Error(err)
			}
			if _, err := vconn.ExecuteFetch("update t1 set val='bbb' where id=1", 1); err != nil {
				t.Error(err)
			}

			// create one transaction
			execStatements(t, []string{
				"update t1 set val='ccc' where id=1",
			})
			// Wait for the begin. The update will be blocked.
			expectDBClientQueries(t, qh.Expect(
				"begin",
			))

			// Create two more transactions. They will go and wait in the relayLog.
			execStatements(t, []string{
				"insert into t1 values(2, '789012')",
				"insert into t1 values(3, '345678')",
				"insert into t1 values(4, '901234')",
			})

			// Release the lock.
			_, _ = vconn.ExecuteFetch("rollback", 1)
			// First transaction will complete. The other two
			// transactions must be batched into one. The last transaction
			// will wait to be sent to the relay until the player fetches
			// them.
			expectDBClientQueries(t, qh.Expect(
				"update t1 set val='ccc' where id=1",
				"/update _vt.vreplication set pos=",
				"commit",
				"begin",
				"insert into t1(id,val) values (2,'789012')",
				"insert into t1(id,val) values (3,'345678')",
				"/update _vt.vreplication set pos=",
				"commit",
				"begin",
				"insert into t1(id,val) values (4,'901234')",
				"/update _vt.vreplication set pos=",
				"commit",
			))
		}()
	}
}

func TestRestartOnVStreamEnd(t *testing.T) {
	defer deleteTablet(addTablet(100))

	savedDelay := retryDelay
	defer func() { retryDelay = savedDelay }()
	retryDelay = 1 * time.Millisecond

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	execStatements(t, []string{
		"insert into t1 values(1, 'aaa')",
	})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into t1(id,val) values (1,'aaa')",
		"/update _vt.vreplication set pos=",
		"commit",
	))

	streamerEngine.Close()
	expectDBClientQueries(t, qh.Expect(
		"/update _vt.vreplication set message='vstream ended'",
	))
	streamerEngine.Open()
	execStatements(t, []string{
		"insert into t1 values(2, 'aaa')",
	})
	expectDBClientQueries(t, qh.Expect(
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set state='Running'",
		"begin",
		"insert into t1(id,val) values (2,'aaa')",
		"/update _vt.vreplication set pos=",
		"commit",
	))
}

func TestTimestamp(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, ts timestamp, dt datetime)",
		fmt.Sprintf("create table %s.t1(id int, ts timestamp, dt datetime)", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	qr, err := env.Mysqld.FetchSuperQuery(context.Background(), "select now()")
	if err != nil {
		t.Fatal(err)
	}
	want := qr.Rows[0][0].ToString()
	t.Logf("want: %s", want)

	execStatements(t, []string{
		fmt.Sprintf("insert into t1 values(1, '%s', '%s')", want, want),
	})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		// The insert value for ts will be in UTC.
		// We'll check the row instead.
		"/insert into t1",
		"/update _vt.vreplication set pos=",
		"commit",
	))

	expectData(t, "t1", [][]string{{"1", want, want}})
}

// TestPlayerJSONDocs validates more complex and 'large' json docs. It only validates that the data on target matches that on source.
// TestPlayerTypes, above, also verifies the sql queries applied on the target.
func TestPlayerJSONDocs(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table vitess_json(id int auto_increment, val json, primary key(id))",
		fmt.Sprintf("create table %s.vitess_json(id int, val json, primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table vitess_json",
		fmt.Sprintf("drop table %s.vitess_json", vrepldb),
	})

	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()
	type testcase struct {
		name  string
		input string
		data  [][]string
	}
	var testcases []testcase
	id := 0
	var addTestCase = func(name, val string) {
		id++
		testcases = append(testcases, testcase{
			name:  name,
			input: fmt.Sprintf("insert into vitess_json(val) values (%s)", encodeString(val)),
			data: [][]string{
				{strconv.Itoa(id), val},
			},
		})
	}
	addTestCase("singleDoc", jsonSingleDoc)
	addTestCase("multipleDocs", jsonMultipleDocs)
	longString := strings.Repeat("aa", math.MaxInt16)

	largeObject := fmt.Sprintf(singleLargeObjectTemplate, longString)
	addTestCase("singleLargeObject", largeObject)

	largeArray := fmt.Sprintf(`[1, 1234567890, "a", true, %s]`, largeObject)
	_ = largeArray
	addTestCase("singleLargeArray", largeArray)

	// the json doc is repeated multiple times to hit the 64K threshold: 140 is got by trial and error
	addTestCase("largeArrayDoc", repeatJSON(jsonSingleDoc, 140, largeJSONArrayCollection))
	addTestCase("largeObjectDoc", repeatJSON(jsonSingleDoc, 140, largeJSONObjectCollection))
	id = 0
	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			id++
			execStatements(t, []string{tcase.input})
			want := qh.Expect(
				"begin",
				"/insert into vitess_json",
				"/update _vt.vreplication set pos=",
				"commit",
			)
			expectDBClientQueries(t, want)
			expectJSON(t, "vitess_json", tcase.data, id, env.Mysqld.FetchSuperQuery)
		})
	}
}

// TestPlayerJSONTwoColumns tests for two json columns in a table
func TestPlayerJSONTwoColumns(t *testing.T) {
	defer deleteTablet(addTablet(100))
	execStatements(t, []string{
		"create table vitess_json2(id int auto_increment, val json, val2 json, primary key(id))",
		fmt.Sprintf("create table %s.vitess_json2(id int, val json, val2 json, primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table vitess_json2",
		fmt.Sprintf("drop table %s.vitess_json2", vrepldb),
	})

	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()
	type testcase struct {
		name  string
		input string
		data  [][]string
	}
	var testcases []testcase
	id := 0
	var addTestCase = func(name, val, val2 string) {
		id++
		testcases = append(testcases, testcase{
			name:  name,
			input: fmt.Sprintf("insert into vitess_json2(val, val2) values (%s, %s)", encodeString(val), encodeString(val2)),
			data: [][]string{
				{strconv.Itoa(id), val, val2},
			},
		})
	}
	longString := strings.Repeat("aa", math.MaxInt16)
	largeObject := fmt.Sprintf(singleLargeObjectTemplate, longString)
	addTestCase("twoCols", jsonSingleDoc, largeObject)
	id = 0
	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			id++
			execStatements(t, []string{tcase.input})
			want := qh.Expect(
				"begin",
				"/insert into vitess_json2",
				"/update _vt.vreplication set pos=",
				"commit",
			)
			expectDBClientQueries(t, want)
			expectJSON(t, "vitess_json2", tcase.data, id, env.Mysqld.FetchSuperQuery)
		})
	}

}

func TestVReplicationLogs(t *testing.T) {
	defer deleteTablet(addTablet(100))
	dbClient := playerEngine.dbClientFactoryDba()
	err := dbClient.Connect()
	require.NoError(t, err)
	defer dbClient.Close()
	vdbc := newVDBClient(dbClient, binlogplayer.NewStats())
	query := "select vrepl_id, state, message, count from _vt.vreplication_log order by id desc limit 1"

	expected := []string{
		"[[INT32(1) VARBINARY(\"Running\") TEXT(\"message1\") INT64(1)]]",
		"[[INT32(1) VARBINARY(\"Running\") TEXT(\"message1\") INT64(2)]]",
	}

	for _, want := range expected {
		t.Run("", func(t *testing.T) {
			err = insertLog(vdbc, LogMessage, 1, "Running", "message1")
			require.NoError(t, err)
			qr, err := env.Mysqld.FetchSuperQuery(context.Background(), query)
			require.NoError(t, err)
			require.Equal(t, want, fmt.Sprintf("%v", qr.Rows))
		})

	}
}

func TestGeneratedColumns(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table t1(id int, val varbinary(6), val2 varbinary(6) as (concat(id, val)), val3 varbinary(6) as (concat(val, id)), id2 int, primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(6), val2 varbinary(6) as (concat(id, val)), val3 varbinary(6), id2 int, primary key(id))", vrepldb),
		"create table t2(id int, val varbinary(128), val2 varbinary(128) as (concat(id, val)) stored, val3 varbinary(128) as (concat(val, id)), id2 int, primary key(id))",
		fmt.Sprintf("create table %s.t2(id int, val3 varbinary(128), val varbinary(128), id2 int, primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
		"drop table t2",
		fmt.Sprintf("drop table %s.t2", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}, {
			Match:  "t2",
			Filter: "select id, val3, val, id2 from t2",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input  string
		output string
		table  string
		data   [][]string
	}{{
		input:  "insert into t1(id, val, id2) values (1, 'aaa', 10)",
		output: "insert into t1(id,val,val3,id2) values (1,'aaa','aaa1',10)",
		table:  "t1",
		data: [][]string{
			{"1", "aaa", "1aaa", "aaa1", "10"},
		},
	}, {
		input:  "update t1 set val = 'bbb', id2 = 11 where id = 1",
		output: "update t1 set val='bbb', val3='bbb1', id2=11 where id=1",
		table:  "t1",
		data: [][]string{
			{"1", "bbb", "1bbb", "bbb1", "11"},
		},
	}, {
		input:  "insert into t2(id, val, id2) values (1, 'aaa', 10)",
		output: "insert into t2(id,val3,val,id2) values (1,'aaa1','aaa',10)",
		table:  "t2",
		data: [][]string{
			{"1", "aaa1", "aaa", "10"},
		},
	}, {
		input:  "update t2 set val = 'bbb', id2 = 11 where id = 1",
		output: "update t2 set val3='bbb1', val='bbb', id2=11 where id=1",
		table:  "t2",
		data: [][]string{
			{"1", "bbb1", "bbb", "11"},
		},
	}}
	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		output := qh.Expect(tcases.output)
		expectNontxQueries(t, output)
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
	}
}

func TestPlayerInvalidDates(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, dt date, primary key(id))",
		fmt.Sprintf("create table %s.dst1(id int, dt date, primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.dst1", vrepldb),
	})
	pos := primaryPosition(t)
	execStatements(t, []string{"set sql_mode='';insert into src1 values(1, '0000-00-00');set sql_mode='STRICT_TRANS_TABLES';"})
	env.SchemaEngine.Reload(context.Background())

	// default mysql flavor allows invalid dates: so disallow explicitly for this test
	if err := env.Mysqld.ExecuteSuperQuery(context.Background(), "SET @@global.sql_mode=REPLACE(REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')"); err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
	}
	defer func() {
		if err := env.Mysqld.ExecuteSuperQuery(context.Background(), "SET @@global.sql_mode=REPLACE(@@global.sql_mode, ',NO_ZERO_DATE,NO_ZERO_IN_DATE','')"); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
		}
	}()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select * from src1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplication(t, bls, pos)
	defer cancel()
	testcases := []struct {
		input  string
		output string
		table  string
		data   [][]string
	}{{
		input:  "select 1 from dual",
		output: "insert into dst1(id,dt) values (1,'0000-00-00')",
		table:  "dst1",
		data: [][]string{
			{"1", "0000-00-00"},
		},
	}, {
		input:  "insert into src1 values (2, '2020-01-01')",
		output: "insert into dst1(id,dt) values (2,'2020-01-01')",
		table:  "dst1",
		data: [][]string{
			{"1", "0000-00-00"},
			{"2", "2020-01-01"},
		},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		output := qh.Expect(tcases.output)
		expectNontxQueries(t, output)

		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
	}
}

// TestPlayerNoBlob sets up a new environment with mysql running with binlog_row_image as noblob. It creates DMLs for
// tables with blob and text columns and executes DMLs with different combinations of columns with and without
// blob/text columns. It confirms that we handle the partial images sent by vstreamer and generates the correct
// dmls on the target.
func TestPlayerNoBlob(t *testing.T) {
	if !runNoBlobTest {
		t.Skip()
	}
	oldVreplicationExperimentalFlags := vttablet.VReplicationExperimentalFlags
	vttablet.VReplicationExperimentalFlags = vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage
	defer func() {
		vttablet.VReplicationExperimentalFlags = oldVreplicationExperimentalFlags
	}()

	defer deleteTablet(addTablet(100))
	execStatements(t, []string{
		"create table t1(id int, val1 varchar(20), blb1 blob, id2 int, blb2 longblob, val2 varbinary(10), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val1 varchar(20), blb1 blob, id2 int, blb2 longblob, val2 varbinary(10), primary key(id))", vrepldb),
		"create table t2(id int, val1 varchar(20), txt1 text, id2 int, val2 varbinary(10), unique key(id, val1))",
		fmt.Sprintf("create table %s.t2(id int, val1 varchar(20), txt1 text, id2 int, val2 varbinary(10), primary key(id, val1))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
		"drop table t2",
		fmt.Sprintf("drop table %s.t2", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}, {
			Match:  "t2",
			Filter: "select * from t2",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, vrId := startVReplication(t, bls, "")
	defer cancel()

	testcases := []struct {
		input  string
		output string
		table  string
		data   [][]string
	}{{ // 1. PartialQueryTemplate-Insert=1, PartialQueryCount-Insert=1 (blb1,blb2 are not inserted)
		input:  "insert into t1(id,val1,blb1,id2,val2) values (1,'aaa','blb1',10,'AAA')",
		output: "insert into t1(id,val1,blb1,id2,val2) values (1,'aaa','blb1',10,'AAA')",
		table:  "t1",
		data: [][]string{
			{"1", "aaa", "blb1", "10", "", "AAA"},
		},
	}, { // 2. PartialQueryTemplate-Update=1, PartialQueryCount-Update=1 (blb1 is not updated)
		input:  "update t1 set blb2 = 'blb22' where id = 1",
		output: "update t1 set val1='aaa', id2=10, blb2='blb22', val2='AAA' where id=1",
		table:  "t1",
		data: [][]string{
			{"1", "aaa", "blb1", "10", "blb22", "AAA"},
		},
	}, { // 3. PartialQueryTemplate-Update=2, PartialQueryCount-Update=2 (blb1 and blb2 are not updated)
		input:  "update t1 set val1 = 'bbb' where id = 1",
		output: "update t1 set val1='bbb', id2=10, val2='AAA' where id=1",
		table:  "t1",
		data: [][]string{
			{"1", "bbb", "blb1", "10", "blb22", "AAA"},
		},
	}, { // 4. PartialQueryTemplate-Update=2, PartialQueryCount-Update=3 (blb1 and blb2 are not updated, same #3)
		input:  "update t1 set val2 = 'CCC', id2=99 where id = 1",
		output: "update t1 set val1='bbb', id2=99, val2='CCC' where id=1",
		table:  "t1",
		data: [][]string{
			{"1", "bbb", "blb1", "99", "blb22", "CCC"},
		},
	}, { // 5. PartialQueryTemplate-Update=2, PartialQueryCount-Update=4 (blb1 is not updated, same as #1)
		input:  "update t1 set blb2 = 'blb21' where id = 1",
		output: "update t1 set val1='bbb', id2=99, blb2='blb21', val2='CCC' where id=1",

		table: "t1",
		data: [][]string{
			{"1", "bbb", "blb1", "99", "blb21", "CCC"},
		},
	}, { // 6. Not a partial update
		input:  "update t1 set blb2 = 'blb222', blb1 = 'blb11' where id = 1",
		output: "update t1 set val1='bbb', blb1='blb11', id2=99, blb2='blb222', val2='CCC' where id=1",
		table:  "t1",
		data: [][]string{
			{"1", "bbb", "blb11", "99", "blb222", "CCC"},
		},
	}, { // 7. PartialQueryTemplate-Insert=2, PartialQueryCount-Insert=2 (txt1 is not inserted)
		input:  "insert into t2(id,val1,id2,val2) values (1,'aaa',10,'AAA')",
		output: "insert into t2(id,val1,id2,val2) values (1,'aaa',10,'AAA')",
		table:  "t2",
		data: [][]string{
			{"1", "aaa", "", "10", "AAA"},
		},
	}, { // 7. PartialQueryTemplate-Insert=2, PartialQueryCount-Insert=3 (txt1 is not inserted, same as #7)
		input:  "insert into t2(id,val1,id2,val2) values (1,'bbb',20,'BBB')",
		output: "insert into t2(id,val1,id2,val2) values (1,'bbb',20,'BBB')",
		table:  "t2",
		data: [][]string{
			{"1", "aaa", "", "10", "AAA"},
			{"1", "bbb", "", "20", "BBB"},
		},
	}, { // 8. Not a partial update, all columns are present
		input:  "update t2 set txt1 = 'txt1' where id = 1 and val1 = 'aaa'",
		output: "update t2 set txt1='txt1', id2=10, val2='AAA' where id=1 and val1='aaa'",
		table:  "t2",
		data: [][]string{
			{"1", "aaa", "txt1", "10", "AAA"},
			{"1", "bbb", "", "20", "BBB"},
		},
	}, { // 9. Not a partial update, all columns are present, same as #8
		input:  "update t2 set val2 = 'DDD', txt1 = 'txt2' where id = 1 and val1 = 'bbb'",
		output: "update t2 set txt1='txt2', id2=20, val2='DDD' where id=1 and val1='bbb'",
		table:  "t2",
		data: [][]string{
			{"1", "aaa", "txt1", "10", "AAA"},
			{"1", "bbb", "txt2", "20", "DDD"},
		},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		output := qh.Expect(tcases.output)
		expectNontxQueries(t, output)
		time.Sleep(1 * time.Second)
		log.Flush()
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
	}
	stats := globalStats.controllers[int32(vrId)].blpStats
	require.Equal(t, 2, len(stats.PartialQueryCount.Counts()))
	require.Equal(t, 2, len(stats.PartialQueryCacheSize.Counts()))
	require.Equal(t, int64(2), stats.PartialQueryCacheSize.Counts()["insert"])
	require.Equal(t, int64(3), stats.PartialQueryCount.Counts()["insert"])
	require.Equal(t, int64(2), stats.PartialQueryCacheSize.Counts()["update"])
	require.Equal(t, int64(4), stats.PartialQueryCount.Counts()["update"])
}

func expectJSON(t *testing.T, table string, values [][]string, id int, exec func(ctx context.Context, query string) (*sqltypes.Result, error)) {
	t.Helper()

	var query string
	if len(strings.Split(table, ".")) == 1 {
		query = fmt.Sprintf("select * from %s.%s where id=%d", vrepldb, table, id)
	} else {
		query = fmt.Sprintf("select * from %s where id=%d", table, id)
	}
	qr, err := exec(context.Background(), query)
	if err != nil {
		t.Error(err)
		return
	}
	if len(values) != len(qr.Rows) {
		t.Fatalf("row counts don't match: %d, want %d", len(qr.Rows), len(values))
	}
	for i, row := range values {
		if len(row) != len(qr.Rows[i]) {
			t.Fatalf("Too few columns, \nrow: %d, \nresult: %d:%v, \nwant: %d:%v", i, len(qr.Rows[i]), qr.Rows[i], len(row), row)
		}
		if qr.Rows[i][0].ToString() != row[0] {
			t.Fatalf("Id mismatch: want %s, got %s", qr.Rows[i][0].ToString(), row[0])
		}

		opts := jsondiff.DefaultConsoleOptions()
		compare, s := jsondiff.Compare(qr.Rows[i][1].Raw(), []byte(row[1]), &opts)
		if compare != jsondiff.FullMatch {
			t.Errorf("Diff:\n%s\n", s)
		}
	}
}

func startVReplication(t *testing.T, bls *binlogdatapb.BinlogSource, pos string) (cancelFunc func(), id int) {
	t.Helper()

	if pos == "" {
		pos = primaryPosition(t)
	}
	// fake workflow type as MoveTables so that we can test with "noblob" binlog row image
	query := binlogplayer.CreateVReplication("test", bls, pos, 9223372036854775807, 9223372036854775807, 0, vrepldb, binlogdatapb.VReplicationWorkflowType_MoveTables, 0, false)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	expectDBClientQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set state='Running'",
	))
	var once sync.Once
	return func() {
		t.Helper()
		once.Do(func() {
			query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
			if _, err := playerEngine.Exec(query); err != nil {
				t.Fatal(err)
			}
			expectDeleteQueries(t)
		})
	}, int(qr.InsertID)
}
