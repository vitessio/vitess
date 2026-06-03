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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"

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
		"create table t1(val varchar(128))",
		fmt.Sprintf("create table %s.t1(val varchar(128))", vrepldb),
		"create table t2(val varchar(128))",
		"SET @@session.sql_generate_invisible_primary_key=OFF;",
		fmt.Sprintf("create table %s.t2(my_row_id int, val varchar(128), primary key(my_row_id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
		"drop table t2",
		fmt.Sprintf("drop table %s.t2", vrepldb),
	})

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
		expectNontxQueries(t, output, recvTimeout)
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
		expectNontxQueries(t, output, recvTimeout)
		time.Sleep(1 * time.Second)
		if tcases.table != "" {
			expectData(t, tcases.table, tcases.data)
		}
		if tcases.query != "" {
			expectQueryResult(t, tcases.query, tcases.queryResult)
		}
	}
}

func TestHeartbeatFrequencyFlag(t *testing.T) {
	origVReplicationHeartbeatUpdateInterval := vttablet.DefaultVReplicationConfig.HeartbeatUpdateInterval
	defer func() {
		vttablet.DefaultVReplicationConfig.HeartbeatUpdateInterval = origVReplicationHeartbeatUpdateInterval
	}()

	stats := binlogplayer.NewStats()
	defer stats.Stop()
	vp := &vplayer{vr: &vreplicator{
		dbClient:       newVDBClient(realDBClientFactory(), stats, vttablet.DefaultVReplicationConfig.RelayLogMaxItems),
		stats:          stats,
		workflowConfig: vttablet.DefaultVReplicationConfig,
	}}

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
			vttablet.DefaultVReplicationConfig.HeartbeatUpdateInterval = tcase.interval
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
		"create table t1(id int, val varchar(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varchar(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})

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

	getTimestamps := func() (int64, int64, int64) {
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
	expectNontxQueries(t, qh.Expect("insert into t1(id,val) values (1,'aaa')"), recvTimeout)
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
	}{{ // binary(2)
		input:  "insert into t1 values(1, 'a')",
		output: "insert into t1(id,val) values (1,_binary'a\\0')",
		table:  "t1",
		data: [][]string{
			{"1", "a\000"},
		},
	}, {
		input:  "update t1 set id = 2 where val = 'a\000'",
		output: "update t1 set id=2 where val=_binary'a\\0'",
		table:  "t1",
		data: [][]string{
			{"2", "a\000"},
		},
	}, { // char(2)
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
	}, { // varbinary(2)
		input:  "insert into t3 values(1, 'a')",
		output: "insert into t3(id,val) values (1,_binary'a')",
		table:  "t3",
		data: [][]string{
			{"1", "a"},
		},
	}, {
		input:  "update t3 set id = 2 where val = 'a'",
		output: "update t3 set id=2 where val=_binary'a'",
		table:  "t3",
		data: [][]string{
			{"2", "a"},
		},
	}, { // varchar(2)
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

// TestPlayerForeignKeyCheck tests that we can insert a row into a child table without the corresponding foreign key
// if the foreign_key_checks is not set.
func TestPlayerForeignKeyCheck(t *testing.T) {
	doNotLogDBQueries = true
	defer func() { doNotLogDBQueries = false }()

	defer deleteTablet(addTablet(100))
	execStatements(t, []string{
		"create table parent(id int, name varchar(128), primary key(id))",
		fmt.Sprintf("create table %s.parent(id int, name varchar(128), primary key(id))", vrepldb),
		"create table child(id int, parent_id int, name varchar(128), primary key(id), foreign key(parent_id) references parent(id) on delete cascade)",
		fmt.Sprintf("create table %s.child(id int, parent_id int, name varchar(128), primary key(id), foreign key(parent_id) references parent(id) on delete cascade)", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table child",
		fmt.Sprintf("drop table %s.child", vrepldb),
		"drop table parent",
		fmt.Sprintf("drop table %s.parent", vrepldb),
	})

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

	testSetForeignKeyQueries = true
	defer func() {
		testSetForeignKeyQueries = false
	}()

	execStatements(t, []string{
		"insert into parent values(1, 'parent1')",
		"insert into child values(1, 1, 'child1')",
		"set foreign_key_checks=0",
		"insert into child values(2, 100, 'child100')",
	})
	expectData(t, "parent", [][]string{
		{"1", "parent1"},
	})
	expectData(t, "child", [][]string{
		{"1", "1", "child1"},
		{"2", "100", "child100"},
	})
	cancel()
}

// TestPlayerStatementModeWithFilterAndErrorHandling confirms that we get the
// expected error when using a filter with statement mode. It also tests the
// general vplayer applyEvent error and log message handling.
func TestPlayerStatementModeWithFilterAndErrorHandling(t *testing.T) {
	defer deleteTablet(addTablet(100))

	// We want to check for the expected log message.
	ole := log.Errorf
	logger := logutil.NewMemoryLogger()
	log.Errorf = logger.Errorf
	defer func() {
		log.Errorf = ole
	}()

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table src1",
	})

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

	const gtid = "37f16b4c-5a74-11ef-87de-56bfd605e62e:100"
	input := []string{
		"set @@session.binlog_format='STATEMENT'",
		fmt.Sprintf("set @@session.gtid_next='%s'", gtid),
		"insert into src1 values(1, 'aaa')",
		"set @@session.gtid_next='AUTOMATIC'",
		"set @@session.binlog_format='ROW'",
	}

	expectedMsg := fmt.Sprintf("[Ee]rror applying event while processing position .*%s.* filter rules are not supported for SBR.*", gtid)

	// It does not work when filter is enabled
	output := qh.Expect(
		"rollback",
		fmt.Sprintf("/update _vt.vreplication set message='%s", expectedMsg),
	)

	execStatements(t, input)
	expectDBClientQueries(t, output)

	logs := logger.String()
	require.Regexp(t, expectedMsg, logs)
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

	vttablet.DefaultVReplicationConfig.EnableHttpLog = true

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
		"create table src_charset(id1 int, val varchar(128) character set utf8mb4 collate utf8mb4_bin, primary key(id1))",
		fmt.Sprintf("create table %s.dst_charset(id1 int, val varchar(128) character set utf8mb4 collate utf8mb4_bin, val2 varchar(128) character set utf8mb4 collate utf8mb4_bin, primary key(id1))", vrepldb),
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
		"drop table src_charset",
		fmt.Sprintf("drop table %s.dst_charset", vrepldb),
	})

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
			Match:  "dst_charset",
			Filter: "select id1, concat(substr(CONVERT(val USING utf8mb4) COLLATE utf8mb4_bin,1,1),'abcxyz') val, concat(substr(CONVERT(val USING utf8mb4) COLLATE utf8mb4_bin,1,1),'abcxyz') val2 from src_charset",
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
			"insert into dst1(id,val) values (1,_binary'aaa')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst1",
		data: [][]string{
			{"1", "aaa"},
		},
		logs: []LogExpectation{
			{"FIELD", "/src1.*id.*INT32.*val.*VARBINARY.*"},
			{"ROWCHANGE", "insert into dst1(id,val) values (1,_binary'aaa')"},
			{"ROW", "/src1.*3.*1aaa.*"},
		},
	}, {
		// update with insertNormal
		input: "update src1 set val='bbb'",
		output: qh.Expect(
			"begin",
			"update dst1 set val=_binary'bbb' where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst1",
		data: [][]string{
			{"1", "bbb"},
		},
		logs: []LogExpectation{
			{"ROWCHANGE", "update dst1 set val=_binary'bbb' where id=1"},
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
			"insert ignore into dst3(id,val) values (1,_binary'aaa')",
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
			"insert ignore into dst3(id,val) values (1,_binary'bbb')",
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
			"insert into yes(id,val) values (1,_binary'aaa')",
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
			"update yes set val=_binary'bbb' where id=1",
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
			"insert into nopk(id,val) values (1,_binary'aaa')",
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
			"delete from nopk where id=1 and val=_binary'aaa'",
			"insert into nopk(id,val) values (1,_binary'bbb')",
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
			"delete from nopk where id=1 and val=_binary'bbb'",
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
			"insert into dst4(id1,val) values (1,_binary'aaa'), (3,_binary'ccc')",
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
			"insert into dst5(id1,val) values (1,_binary'abc'), (4,_binary'abc')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst5",
		data:  [][]string{{"1", "abc"}, {"4", "abc"}},
	}, {
		// test collation + filter
		input: "insert into src_charset values (1,'木元')",
		output: qh.Expect(
			"begin",
			"insert into dst_charset(id1,val,val2) values (1,concat(substr(convert(_binary'木元' using utf8mb4) collate utf8mb4_bin, 1, 1), 'abcxyz'),concat(substr(convert(_binary'木元' using utf8mb4) collate utf8mb4_bin, 1, 1), 'abcxyz'))",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst_charset",
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
		"create table `begin`(`primary` int, `column` varchar(128), primary key(`primary`))",
		fmt.Sprintf("create table %s.`begin`(`primary` int, `column` varchar(128), primary key(`primary`))", vrepldb),
		"create table `rollback`(`primary` int, `column` varchar(128), primary key(`primary`))",
		fmt.Sprintf("create table %s.`rollback`(`primary` int, `column` varchar(128), primary key(`primary`))", vrepldb),
		"create table `commit`(`primary` int, `column` varchar(128), primary key(`primary`))",
		fmt.Sprintf("create table %s.`commit`(`primary` int, `column` varchar(128), primary key(`primary`))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table `begin`",
		fmt.Sprintf("drop table %s.`begin`", vrepldb),
		"drop table `rollback`",
		fmt.Sprintf("drop table %s.`rollback`", vrepldb),
		"drop table `commit`",
		fmt.Sprintf("drop table %s.`commit`", vrepldb),
	})

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
			"insert into dst1(id,val) values (1,_binary'\x16k@\xb4J\xbaK\xd6')",
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
		input: "insert into src1 values(1, '\U0001f44d')",
		output: qh.Expect(
			"begin",
			// We should expect the "Mojibaked" version.
			"insert into dst1(id,val) values (1,'ðŸ'')",
			"/update _vt.vreplication set pos=",
			"commit",
		),
		table: "dst1",
		data: [][]string{
			{"1", "\U0001f44d"},
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
