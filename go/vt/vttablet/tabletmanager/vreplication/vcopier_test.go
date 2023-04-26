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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"

	"context"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	qh "vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication/queryhistory"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
)

type vcopierTestCase struct {
	vreplicationExperimentalFlags     int64
	vreplicationParallelInsertWorkers int
}

func commonVcopierTestCases() []vcopierTestCase {
	return []vcopierTestCase{
		// Default experimental flags.
		{
			vreplicationExperimentalFlags: vttablet.VReplicationExperimentalFlags,
		},
		// Parallel bulk inserts enabled with 4 workers.
		{
			vreplicationExperimentalFlags:     vttablet.VReplicationExperimentalFlags,
			vreplicationParallelInsertWorkers: 4,
		},
	}
}

func testVcopierTestCases(t *testing.T, test func(*testing.T), cases []vcopierTestCase) {
	oldVreplicationExperimentalFlags := vttablet.VReplicationExperimentalFlags
	oldVreplicationParallelInsertWorkers := vreplicationParallelInsertWorkers
	// Extra reset at the end in case we return prematurely.
	defer func() {
		vttablet.VReplicationExperimentalFlags = oldVreplicationExperimentalFlags
		vreplicationParallelInsertWorkers = oldVreplicationParallelInsertWorkers
	}()

	for _, tc := range cases {
		tc := tc // Avoid export loop bugs.
		// Set test flags.
		vttablet.VReplicationExperimentalFlags = tc.vreplicationExperimentalFlags
		vreplicationParallelInsertWorkers = tc.vreplicationParallelInsertWorkers
		// Run test case.
		t.Run(
			fmt.Sprintf(
				"vreplication_experimental_flags=%d,vreplication_parallel_insert_workers=%d",
				tc.vreplicationExperimentalFlags, tc.vreplicationParallelInsertWorkers,
			),
			test,
		)
		// Reset.
		vttablet.VReplicationExperimentalFlags = oldVreplicationExperimentalFlags
		vreplicationParallelInsertWorkers = oldVreplicationParallelInsertWorkers
	}
}

func TestPlayerCopyCharPK(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyCharPK, commonVcopierTestCases())
}

func testPlayerCopyCharPK(t *testing.T) {
	defer deleteTablet(addTablet(100))

	reset := vstreamer.AdjustPacketSize(1)
	defer reset()

	savedCopyPhaseDuration := copyPhaseDuration
	// copyPhaseDuration should be low enough to have time to send one row.
	copyPhaseDuration = 500 * time.Millisecond
	defer func() { copyPhaseDuration = savedCopyPhaseDuration }()

	savedWaitRetryTime := waitRetryTime
	// waitRetry time should be very low to cause the wait loop to execute multipel times.
	waitRetryTime = 10 * time.Millisecond
	defer func() { waitRetryTime = savedWaitRetryTime }()

	execStatements(t, []string{
		"create table src(idc binary(2) , val int, primary key(idc))",
		"insert into src values('a', 1), ('c', 2)",
		fmt.Sprintf("create table %s.dst(idc binary(2), val int, primary key(idc))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src",
		fmt.Sprintf("drop table %s.dst", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	count := 0
	vstreamRowsSendHook = func(ctx context.Context) {
		defer func() { count++ }()
		// Allow the first two calls to go through: field info and one row.
		if count <= 1 {
			return
		}
		// Insert a row with PK which is < the lastPK till now because of the utf8mb4 collation
		execStatements(t, []string{
			"update src set val = 3 where idc = 'a\000'",
		})
		// Wait for context to expire and then send the row.
		// This will cause the copier to abort and go back to catchup mode.
		<-ctx.Done()
		// Do this no more than once.
		vstreamRowsSendHook = nil
	}

	vstreamHook = func(context.Context) {
		// Sleeping 50ms guarantees that the catchup wait loop executes multiple times.
		// This is because waitRetryTime is set to 10ms.
		time.Sleep(50 * time.Millisecond)
		// Do this no more than once.
		vstreamHook = nil
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst",
			Filter: "select * from src",
		}},
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}

	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectNontxQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"insert into dst(idc,val) values ('a\\0',1)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"idc\\" type:BINARY} rows:{lengths:2 values:\\"a\\\\x00\\"}'.*`,
		`update dst set val=3 where idc='a\0' and ('a\0') <= ('a\0')`,
		"insert into dst(idc,val) values ('c\\0',2)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"idc\\" type:BINARY} rows:{lengths:2 values:\\"c\\\\x00\\"}'.*`,
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst",
		"/update _vt.vreplication set state='Running",
	))

	expectData(t, "dst", [][]string{
		{"a\000", "3"},
		{"c\000", "2"},
	})
}

// TestPlayerCopyVarcharPKCaseInsensitive tests the copy/catchup phase for a table with a varchar primary key
// which is case insensitive.
func TestPlayerCopyVarcharPKCaseInsensitive(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyVarcharPKCaseInsensitive, commonVcopierTestCases())
}

func testPlayerCopyVarcharPKCaseInsensitive(t *testing.T) {
	defer deleteTablet(addTablet(100))

	// Set packet size low so that we send one row at a time.
	reset := vstreamer.AdjustPacketSize(1)
	defer reset()

	savedCopyPhaseDuration := copyPhaseDuration
	// copyPhaseDuration should be low enough to have time to send one row.
	copyPhaseDuration = 500 * time.Millisecond
	defer func() { copyPhaseDuration = savedCopyPhaseDuration }()

	savedWaitRetryTime := waitRetryTime
	// waitRetry time should be very low to cause the wait loop to execute multiple times.
	waitRetryTime = 10 * time.Millisecond
	defer func() { waitRetryTime = savedWaitRetryTime }()

	execStatements(t, []string{
		"create table src(idc varchar(20), val int, primary key(idc))",
		"insert into src values('a', 1), ('c', 2)",
		fmt.Sprintf("create table %s.dst(idc varchar(20), val int, primary key(idc))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src",
		fmt.Sprintf("drop table %s.dst", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	count := 0
	vstreamRowsSendHook = func(ctx context.Context) {
		defer func() { count++ }()
		// Allow the first two calls to go through: field info and one row.
		if count <= 1 {
			return
		}
		// Insert a row with PK which is < the lastPK till now because of the utf8mb4 collation
		execStatements(t, []string{
			"insert into src values('B', 3)",
		})
		// Wait for context to expire and then send the row.
		// This will cause the copier to abort and go back to catchup mode.
		<-ctx.Done()
		// Do this no more than once.
		vstreamRowsSendHook = nil
	}

	vstreamHook = func(context.Context) {
		// Sleeping 50ms guarantees that the catchup wait loop executes multiple times.
		// This is because waitRetryTime is set to 10ms.
		time.Sleep(50 * time.Millisecond)
		// Do this no more than once.
		vstreamHook = nil
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst",
			Filter: "select * from src",
		}},
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}

	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectNontxQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		// Copy mode.
		"insert into dst(idc,val) values ('a',1)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"idc\\" type:VARCHAR} rows:{lengths:1 values:\\"a\\"}'.*`,
		// Copy-catchup mode.
		`/insert into dst\(idc,val\) select 'B', 3 from dual where \( .* 'B' COLLATE .* \) <= \( .* 'a' COLLATE .* \)`,
	).Then(func(expect qh.ExpectationSequencer) qh.ExpectationSequencer {
		// Back to copy mode.
		// Inserts can happen out of order.
		// Updates must happen in order.
		//upd1 := expect.
		upd1 := expect.Then(qh.Eventually(
			"insert into dst(idc,val) values ('B',3)",
			`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"idc\\" type:VARCHAR} rows:{lengths:1 values:\\"B\\"}'.*`,
		))
		upd2 := expect.Then(qh.Eventually(
			"insert into dst(idc,val) values ('c',2)",
			`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"idc\\" type:VARCHAR} rows:{lengths:1 values:\\"c\\"}'.*`,
		))
		upd1.Then(upd2.Eventually())
		return upd2
	}).Then(qh.Immediately(
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst",
		"/update _vt.vreplication set state='Running'",
	)))

	expectData(t, "dst", [][]string{
		{"a", "1"},
		{"B", "3"},
		{"c", "2"},
	})
}

// TestPlayerCopyVarcharPKCaseSensitiveCollation tests the copy/catchup phase for a table with varbinary columns
// (which has a case sensitive collation with upper case alphabets below lower case in sort order)
func TestPlayerCopyVarcharCompositePKCaseSensitiveCollation(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyVarcharCompositePKCaseSensitiveCollation, commonVcopierTestCases())
}

func testPlayerCopyVarcharCompositePKCaseSensitiveCollation(t *testing.T) {
	defer deleteTablet(addTablet(100))

	reset := vstreamer.AdjustPacketSize(1)
	defer reset()

	savedCopyPhaseDuration := copyPhaseDuration
	// copyPhaseDuration should be low enough to have time to send one row.
	copyPhaseDuration = 500 * time.Millisecond
	defer func() { copyPhaseDuration = savedCopyPhaseDuration }()

	savedWaitRetryTime := waitRetryTime
	// waitRetry time should be very low to cause the wait loop to execute multipel times.
	waitRetryTime = 10 * time.Millisecond
	defer func() { waitRetryTime = savedWaitRetryTime }()

	execStatements(t, []string{
		"create table src(id int, idc varbinary(20), idc2 varbinary(20), val int, primary key(id,idc,idc2))",
		"insert into src values(1, 'a', 'a', 1), (1, 'c', 'c', 2)",
		fmt.Sprintf("create table %s.dst(id int, idc varbinary(20), idc2 varbinary(20), val int, primary key(id,idc,idc2))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src",
		fmt.Sprintf("drop table %s.dst", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	count := 0
	vstreamRowsSendHook = func(ctx context.Context) {
		defer func() { count++ }()
		// Allow the first two calls to go through: field info and one row.
		if count <= 1 {
			return
		}
		// Insert a row with PK which is < the lastPK till now because of the utf8mb4 collation
		execStatements(t, []string{
			"insert into src values(1, 'B', 'B', 3)",
		})
		// Wait for context to expire and then send the row.
		// This will cause the copier to abort and go back to catchup mode.
		<-ctx.Done()
		// Do this no more than once.
		vstreamRowsSendHook = nil
	}

	vstreamHook = func(context.Context) {
		// Sleeping 50ms guarantees that the catchup wait loop executes multiple times.
		// This is because waitRetryTime is set to 10ms.
		time.Sleep(50 * time.Millisecond)
		// Do this no more than once.
		vstreamHook = nil
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst",
			Filter: "select * from src",
		}},
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}

	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectNontxQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		// Copy mode.
		"insert into dst(id,idc,idc2,val) values (1,'a','a',1)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} fields:{name:\\"idc\\" type:VARBINARY} fields:{name:\\"idc2\\" type:VARBINARY} rows:{lengths:1 lengths:1 lengths:1 values:\\"1aa\\"}'.*`,
		// Copy-catchup mode.
		`insert into dst(id,idc,idc2,val) select 1, 'B', 'B', 3 from dual where (1,'B','B') <= (1,'a','a')`,
		// Copy mode.
		"insert into dst(id,idc,idc2,val) values (1,'c','c',2)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} fields:{name:\\"idc\\" type:VARBINARY} fields:{name:\\"idc2\\" type:VARBINARY} rows:{lengths:1 lengths:1 lengths:1 values:\\"1cc\\"}'.*`,
		// Wrap-up.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst",
		"/update _vt.vreplication set state='Running'",
	))

	expectData(t, "dst", [][]string{
		{"1", "B", "B", "3"},
		{"1", "a", "a", "1"},
		{"1", "c", "c", "2"},
	})
}

// TestPlayerCopyTablesWithFK validates that vreplication disables foreign keys during the copy phase
func TestPlayerCopyTablesWithFK(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyTablesWithFK, commonVcopierTestCases())
}

func testPlayerCopyTablesWithFK(t *testing.T) {
	testForeignKeyQueries = true
	defer func() {
		testForeignKeyQueries = false
	}()

	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src2(id int, id2 int, primary key(id))",
		"create table src1(id int, id2 int, primary key(id), foreign key (id2) references src2(id) on delete cascade)",
		"insert into src2 values(1, 21), (2, 22)",
		"insert into src1 values(1, 1), (2, 2)",
		fmt.Sprintf("create table %s.dst2(id int, id2 int, primary key(id))", vrepldb),
		fmt.Sprintf("create table %s.dst1(id int, id2 int, primary key(id), foreign key (id2) references dst2(id) on delete cascade)", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.dst1", vrepldb),
		"drop table src2",
		fmt.Sprintf("drop table %s.dst2", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select * from src1",
		}, {
			Match:  "dst2",
			Filter: "select * from src2",
		}},
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	require.NoError(t, err)

	expectDBClientQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"select @@foreign_key_checks;",
		// Create the list of tables to copy and transition to Copying state.
		"begin",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"commit",
		"set foreign_key_checks=0;",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"/update _vt.vreplication set pos=",
	).Then(func(expect qh.ExpectationSequencer) qh.ExpectationSequencer {
		// With parallel inserts, new db client connects are created on-the-fly.
		if vreplicationParallelInsertWorkers > 1 {
			return expect.Then(qh.Eventually("set foreign_key_checks=0;"))
		}
		return expect
	}).Then(qh.Eventually(
		// Copy.
		// Inserts may happen out-of-order. Update happen in-order.
		"begin",
		"insert into dst1(id,id2) values (1,1), (2,2)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		"commit",
	)).Then(qh.Immediately(
		"set foreign_key_checks=0;",
		// copy of dst1 is done: delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst1",
		// The next FF executes and updates the position before copying.
		"set foreign_key_checks=0;",
		"begin",
		"/update _vt.vreplication set pos=",
		"commit",
	)).Then(func(expect qh.ExpectationSequencer) qh.ExpectationSequencer {
		// With parallel inserts, new db client connects are created on-the-fly.
		if vreplicationParallelInsertWorkers > 1 {
			return expect.Then(qh.Eventually("set foreign_key_checks=0;"))
		}
		return expect
	}).Then(qh.Eventually(
		// copy dst2
		"begin",
		"insert into dst2(id,id2) values (1,21), (2,22)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		"commit",
	)).Then(qh.Immediately(
		"set foreign_key_checks=0;",
		// copy of dst1 is done: delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst2",
		// All tables copied. Final catch up followed by Running state.
		"set foreign_key_checks=1;",
		"/update _vt.vreplication set state='Running'",
	)))

	expectData(t, "dst1", [][]string{
		{"1", "1"},
		{"2", "2"},
	})
	expectData(t, "dst2", [][]string{
		{"1", "21"},
		{"2", "22"},
	})

	validateCopyRowCountStat(t, 4)

	query = fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
	if _, err := playerEngine.Exec(query); err != nil {
		t.Fatal(err)
	}
	expectDBClientQueries(t, qh.Expect(
		"set foreign_key_checks=1;",
		"begin",
		"/delete from _vt.vreplication",
		"/delete from _vt.copy_state",
		"/delete from _vt.post_copy_action",
		"commit",
	))
}

func TestPlayerCopyTables(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyTables, commonVcopierTestCases())
}

func testPlayerCopyTables(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), d decimal(8,0), j json, primary key(id))",
		"insert into src1 values(2, 'bbb', 1, '{\"foo\": \"bar\"}'), (1, 'aaa', 0, JSON_ARRAY(123456789012345678901234567890, \"abcd\"))",
		fmt.Sprintf("create table %s.dst1(id int, val varbinary(128), val2 varbinary(128), d decimal(8,0), j json, primary key(id))", vrepldb),
		"create table yes(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.yes(id int, val varbinary(128), primary key(id))", vrepldb),
		"create table no(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.dst1", vrepldb),
		"drop table yes",
		fmt.Sprintf("drop table %s.yes", vrepldb),
		"drop table no",
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select id, val, val as val2, d, j from src1",
		}, {
			Match: "/yes",
		}},
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectDBClientQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		// Create the list of tables to copy and transition to Copying state.
		"begin",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"commit",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"/update _vt.vreplication set pos=",
		"begin",
		"insert into dst1(id,val,val2,d,j) values (1,'aaa','aaa',0,JSON_ARRAY(123456789012345678901234567890, _utf8mb4'abcd')), (2,'bbb','bbb',1,JSON_OBJECT(_utf8mb4'foo', _utf8mb4'bar'))",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		"commit",
		// copy of dst1 is done: delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst1",
		// The next FF executes and updates the position before copying.
		"begin",
		"/update _vt.vreplication set pos=",
		"commit",
		// Nothing to copy from yes. Delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*yes",
		// All tables copied. Final catch up followed by Running state.
		"/update _vt.vreplication set state='Running'",
	))
	expectData(t, "dst1", [][]string{
		{"1", "aaa", "aaa", "0", "[123456789012345678901234567890, \"abcd\"]"},
		{"2", "bbb", "bbb", "1", "{\"foo\": \"bar\"}"},
	})
	expectData(t, "yes", [][]string{})
	validateCopyRowCountStat(t, 2)
	ctx, cancel := context.WithCancel(context.Background())

	type logTestCase struct {
		name string
		typ  string
	}
	testCases := []logTestCase{
		{name: "Check log for start of copy", typ: "LogCopyStarted"},
		{name: "Check log for end of copy", typ: "LogCopyEnded"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query = fmt.Sprintf("select count(*) from _vt.vreplication_log where type = '%s'", testCase.typ)
			qr, err := env.Mysqld.FetchSuperQuery(ctx, query)
			require.NoError(t, err)
			require.NotNil(t, qr)
			require.Equal(t, 1, len(qr.Rows))
		})
	}
	cancel()

}

// TestPlayerCopyBigTable ensures the copy-catchup back-and-forth loop works correctly.
func TestPlayerCopyBigTable(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyBigTable, commonVcopierTestCases())
}

func testPlayerCopyBigTable(t *testing.T) {
	defer deleteTablet(addTablet(100))

	reset := vstreamer.AdjustPacketSize(1)
	defer reset()

	savedCopyPhaseDuration := copyPhaseDuration
	// copyPhaseDuration should be low enough to have time to send one row.
	copyPhaseDuration = 500 * time.Millisecond
	defer func() { copyPhaseDuration = savedCopyPhaseDuration }()

	savedWaitRetryTime := waitRetryTime
	// waitRetry time should be very low to cause the wait loop to execute multiple times.
	waitRetryTime = 10 * time.Millisecond
	defer func() { waitRetryTime = savedWaitRetryTime }()

	execStatements(t, []string{
		"create table src(id int, val varbinary(128), primary key(id))",
		"insert into src values(1, 'aaa'), (2, 'bbb')",
		fmt.Sprintf("create table %s.dst(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src",
		fmt.Sprintf("drop table %s.dst", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	count := 0
	vstreamRowsSendHook = func(ctx context.Context) {
		defer func() { count++ }()
		// Allow the first two calls to go through: field info and one row.
		if count <= 1 {
			return
		}
		// Insert a statement to test that catchup gets new events.
		execStatements(t, []string{
			"insert into src values(3, 'ccc')",
		})
		// Wait for context to expire and then send the row.
		// This will cause the copier to abort and go back to catchup mode.
		<-ctx.Done()
		// Do this at most once.
		vstreamRowsSendHook = nil
	}

	vstreamHook = func(context.Context) {
		// Sleeping 50ms guarantees that the catchup wait loop executes multiple times.
		// This is because waitRetryTime is set to 10ms.
		time.Sleep(50 * time.Millisecond)
		// Do this no more than once.
		vstreamHook = nil
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst",
			Filter: "select * from src",
		}},
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}

	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectNontxQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"/update _vt.vreplication set state='Copying'",
		"insert into dst(id,val) values (1,'aaa')",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"1\\"}'.*`,
		// The next catchup executes the new row insert, but will be a no-op.
		"insert into dst(id,val) select 3, 'ccc' from dual where (3) <= (1)",
		// fastForward has nothing to add. Just saves position.
		// Back to copy mode.
		// Inserts can happen out-of-order.
		// Updates happen in-order.
	).Then(func(expect qh.ExpectationSequencer) qh.ExpectationSequencer {
		ins1 := expect.Then(qh.Eventually("insert into dst(id,val) values (2,'bbb')"))
		upd1 := ins1.Then(qh.Eventually(
			`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		))
		// Third row copied without going back to catchup state.
		ins3 := expect.Then(qh.Eventually("insert into dst(id,val) values (3,'ccc')"))
		upd3 := ins3.Then(qh.Eventually(
			`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"3\\"}'.*`,
		))
		upd1.Then(upd3.Eventually())
		return upd3
	}).Then(qh.Eventually(
		// Wrap-up.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst",
		// Copy is done. Go into running state.
		// All tables copied. Final catch up followed by Running state.
		"/update _vt.vreplication set state='Running'",
	)))

	expectData(t, "dst", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
		{"3", "ccc"},
	})
	validateCopyRowCountStat(t, 3)

	// this check is very flaky in CI and should be done manually while testing catchup locally
	// validateQueryCountStat(t, "catchup", 1)
}

// TestPlayerCopyWildcardRule ensures the copy-catchup back-and-forth loop works correctly
// when the filter uses a wildcard rule
func TestPlayerCopyWildcardRule(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyWildcardRule, commonVcopierTestCases())
}

func testPlayerCopyWildcardRule(t *testing.T) {
	defer deleteTablet(addTablet(100))

	reset := vstreamer.AdjustPacketSize(1)
	defer reset()

	savedCopyPhaseDuration := copyPhaseDuration
	// copyPhaseDuration should be low enough to have time to send one row.
	copyPhaseDuration = 500 * time.Millisecond
	defer func() { copyPhaseDuration = savedCopyPhaseDuration }()

	savedWaitRetryTime := waitRetryTime
	// waitRetry time should be very low to cause the wait loop to execute multipel times.
	waitRetryTime = 10 * time.Millisecond
	defer func() { waitRetryTime = savedWaitRetryTime }()

	execStatements(t, []string{
		"create table src(id int, val varbinary(128), primary key(id))",
		"insert into src values(1, 'aaa'), (2, 'bbb')",
		fmt.Sprintf("create table %s.src(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src",
		fmt.Sprintf("drop table %s.src", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	count := 0
	vstreamRowsSendHook = func(ctx context.Context) {
		defer func() { count++ }()
		// Allow the first two calls to go through: field info and one row.
		if count <= 1 {
			return
		}
		// Insert a statement to test that catchup gets new events.
		execStatements(t, []string{
			"insert into src values(3, 'ccc')",
		})
		// Wait for context to expire and then send the row.
		// This will cause the copier to abort and go back to catchup mode.
		<-ctx.Done()
		// Do this no more than once.
		vstreamRowsSendHook = nil
	}

	vstreamHook = func(context.Context) {
		// Sleeping 50ms guarantees that the catchup wait loop executes multiple times.
		// This is because waitRetryTime is set to 10ms.
		time.Sleep(50 * time.Millisecond)
		// Do this no more than once.
		vstreamHook = nil
	}

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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectNontxQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"insert into src(id,val) values (1,'aaa')",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"1\\"}'.*`,
		// The next catchup executes the new row insert, but will be a no-op.
		"insert into src(id,val) select 3, 'ccc' from dual where (3) <= (1)",
		// fastForward has nothing to add. Just saves position.
		// Return to copy mode.
		// Inserts can happen out-of-order.
		// Updates happen in-order.
	).Then(func(expect qh.ExpectationSequencer) qh.ExpectationSequencer {
		ins1 := expect.Then(qh.Eventually("insert into src(id,val) values (2,'bbb')"))
		upd1 := ins1.Then(qh.Eventually(
			`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		))
		// Third row copied without going back to catchup state.
		ins3 := expect.Then(qh.Eventually("insert into src(id,val) values (3,'ccc')"))
		upd3 := ins3.Then(qh.Eventually(
			`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"3\\"}'.*`,
		))
		upd1.Then(upd3.Eventually())
		return upd3
	}).Then(qh.Immediately(
		// Wrap-up.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*src",
		// Copy is done. Go into running state.
		"/update _vt.vreplication set state='Running'",
	)))

	expectData(t, "src", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
		{"3", "ccc"},
	})
}

// TestPlayerCopyTableContinuation tests the copy workflow where tables have been partially copied.
// TODO(maxenglander): this test isn't repeatable, even with the same flags.
func TestPlayerCopyTableContinuation(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyTableContinuation, []vcopierTestCase{
		{
			vreplicationExperimentalFlags: 0,
		},
	})
}

func testPlayerCopyTableContinuation(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		// src1 is initialized as partially copied.
		// lastpk will be initialized at (6,6) later below.
		// dst1 only copies id1 and val. This will allow us to test for correctness if id2 changes in the source.
		"create table src1(id1 int, id2 int, val varbinary(128), primary key(id1, id2))",
		"insert into src1 values(2,2,'no change'), (3,3,'update'), (4,4,'delete'), (5,5,'move within'), (6,6,'move out'), (8,8,'no change'), (9,9,'delete'), (10,10,'update'), (11,11,'move in')",
		fmt.Sprintf("create table %s.dst1(id int, val varbinary(128), primary key(id))", vrepldb),
		fmt.Sprintf("insert into %s.dst1 values(2,'no change'), (3,'update'), (4,'delete'), (5,'move within'), (6,'move out')", vrepldb),
		// copied is initialized as fully copied
		"create table copied(id int, val varbinary(128), primary key(id))",
		"insert into copied values(1,'aaa')",
		fmt.Sprintf("create table %s.copied(id int, val varbinary(128), primary key(id))", vrepldb),
		fmt.Sprintf("insert into %s.copied values(1,'aaa')", vrepldb),
		// not_copied yet to be copied.
		"create table not_copied(id int, val varbinary(128), primary key(id))",
		"insert into not_copied values(1,'aaa')",
		fmt.Sprintf("create table %s.not_copied(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.dst1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select id1 as id, val from src1",
		}, {
			Match:  "copied",
			Filter: "select * from copied",
		}, {
			Match:  "not_copied",
			Filter: "select * from not_copied",
		}},
	}
	pos := primaryPosition(t)
	execStatements(t, []string{
		// insert inside and outside current range.
		"insert into src1 values(1,1,'insert in'), (7,7,'insert out')",
		// update inside and outside current range.
		"update src1 set val='updated' where id1 in (3,10)",
		// delete inside and outside current range.
		"delete from src1 where id1 in (4,9)",
		// move row within range by changing id2.
		"update src1 set id2=10 where id1=5",
		// move row from within to outside range.
		"update src1 set id1=12 where id1=6",
		// move row from outside to witihn range.
		"update src1 set id1=4 where id1=11",
		// modify the copied table.
		"update copied set val='bbb' where id=1",
		// modify the uncopied table.
		"update not_copied set val='bbb' where id=1",
	})

	// Set a hook to execute statements just before the copy begins from src1.
	vstreamRowsHook = func(context.Context) {
		execStatements(t, []string{
			"update src1 set val='updated again' where id1 = 3",
		})
		// Set it back to nil. Otherwise, this will get executed again when copying not_copied.
		vstreamRowsHook = nil
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.BlpStopped, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	// As mentioned above. lastpk cut-off is set at (6,6)
	lastpk := sqltypes.ResultToProto3(sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id1|id2",
			"int32|int32",
		),
		"6|6",
	))
	lastpk.RowsAffected = 0
	execStatements(t, []string{
		fmt.Sprintf("insert into _vt.copy_state (vrepl_id, table_name, lastpk) values(%d, '%s', %s)", qr.InsertID, "dst1", encodeString(fmt.Sprintf("%v", lastpk))),
		fmt.Sprintf("insert into _vt.copy_state (vrepl_id, table_name, lastpk) values(%d, '%s', null)", qr.InsertID, "not_copied"),
	})
	id := qr.InsertID
	_, err = playerEngine.Exec(fmt.Sprintf("update _vt.vreplication set state='Copying', pos=%s where id=%d", encodeString(pos), id))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", id)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	for q := range globalDBQueries {
		if strings.HasPrefix(q, "update") {
			break
		}
	}

	expectNontxQueries(t, qh.Expect(
		// Catchup
		"/update _vt.vreplication set message='Picked source tablet.*",
		"insert into dst1(id,val) select 1, 'insert in' from dual where (1,1) <= (6,6)",
		"insert into dst1(id,val) select 7, 'insert out' from dual where (7,7) <= (6,6)",
		"update dst1 set val='updated' where id=3 and (3,3) <= (6,6)",
		"update dst1 set val='updated' where id=10 and (10,10) <= (6,6)",
		"delete from dst1 where id=4 and (4,4) <= (6,6)",
		"delete from dst1 where id=9 and (9,9) <= (6,6)",
		"delete from dst1 where id=5 and (5,5) <= (6,6)",
		"insert into dst1(id,val) select 5, 'move within' from dual where (5,10) <= (6,6)",
		"delete from dst1 where id=6 and (6,6) <= (6,6)",
		"insert into dst1(id,val) select 12, 'move out' from dual where (12,6) <= (6,6)",
		"delete from dst1 where id=11 and (11,11) <= (6,6)",
		"insert into dst1(id,val) select 4, 'move in' from dual where (4,11) <= (6,6)",
		"update copied set val='bbb' where id=1",
		// Fast-forward
		"update dst1 set val='updated again' where id=3 and (3,3) <= (6,6)",
	).Then(qh.Immediately(
		"insert into dst1(id,val) values (7,'insert out'), (8,'no change'), (10,'updated'), (12,'move out')",
	)).Then(qh.Eventually(
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id1\\" type:INT32} fields:{name:\\"id2\\" type:INT32} rows:{lengths:2 lengths:1 values:\\"126\\"}'.*`,
	)).Then(qh.Immediately(
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst1",
		"insert into not_copied(id,val) values (1,'bbb')",
	)).Then(qh.Eventually(
		// Copy again. There should be no events for catchup.
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\\"id\\\" type:INT32} rows:{lengths:1 values:\\\"1\\\"}'.*`,
	)).Then(qh.Immediately(
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*not_copied",
		"/update _vt.vreplication set state='Running'",
	)))

	expectData(t, "dst1", [][]string{
		{"1", "insert in"},
		{"2", "no change"},
		{"3", "updated again"},
		{"4", "move in"},
		{"5", "move within"},
		{"7", "insert out"},
		{"8", "no change"},
		{"10", "updated"},
		{"12", "move out"},
	})
	expectData(t, "copied", [][]string{
		{"1", "bbb"},
	})
	expectData(t, "not_copied", [][]string{
		{"1", "bbb"},
	})
}

// TestPlayerCopyWildcardTableContinuation.
func TestPlayerCopyWildcardTableContinuation(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyWildcardTableContinuation, commonVcopierTestCases())
	testVcopierTestCases(t, testPlayerCopyWildcardTableContinuation, []vcopierTestCase{
		// Optimize inserts without parallel inserts.
		{
			vreplicationExperimentalFlags: vttablet.VReplicationExperimentalFlagOptimizeInserts,
		},
		// Optimize inserts with parallel inserts.
		{
			vreplicationExperimentalFlags:     vttablet.VReplicationExperimentalFlagOptimizeInserts,
			vreplicationParallelInsertWorkers: 4,
		},
	})
}

func testPlayerCopyWildcardTableContinuation(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src(id int, val varbinary(128), primary key(id))",
		"insert into src values(2,'copied'), (3,'uncopied')",
		fmt.Sprintf("create table %s.dst(id int, val varbinary(128), primary key(id))", vrepldb),
		fmt.Sprintf("insert into %s.dst values(2,'copied')", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src",
		fmt.Sprintf("drop table %s.dst", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst",
			Filter: "select * from src",
		}},
	}
	pos := primaryPosition(t)
	execStatements(t, []string{
		"insert into src values(4,'new')",
	})

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.BlpStopped, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	lastpk := sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id",
		"int32"),
		"2",
	))
	lastpk.RowsAffected = 0
	execStatements(t, []string{
		fmt.Sprintf("insert into _vt.copy_state (vrepl_id, table_name, lastpk) values(%d, '%s', %s)", qr.InsertID, "dst", encodeString(fmt.Sprintf("%v", lastpk))),
	})
	id := qr.InsertID
	_, err = playerEngine.Exec(fmt.Sprintf("update _vt.vreplication set state='Copying', pos=%s where id=%d", encodeString(pos), id))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", id)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	optimizeInsertsEnabled := vttablet.VReplicationExperimentalFlags /**/ & /**/ vttablet.VReplicationExperimentalFlagOptimizeInserts != 0

	expectNontxQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set state = 'Copying'",
		"/update _vt.vreplication set message='Picked source tablet.*",
	).Then(func(expect qh.ExpectationSequencer) qh.ExpectationSequencer {
		if !optimizeInsertsEnabled {
			expect = expect.Then(qh.Immediately("insert into dst(id,val) select 4, 'new' from dual where (4) <= (2)"))
		}
		return expect.Then(qh.Immediately("insert into dst(id,val) values (3,'uncopied'), (4,'new')"))
	}).Then(qh.Immediately(
		`/insert into _vt.copy_state .*`,
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst",
		"/update _vt.vreplication set state='Running'",
	)))

	expectData(t, "dst", [][]string{
		{"2", "copied"},
		{"3", "uncopied"},
		{"4", "new"},
	})
	if optimizeInsertsEnabled {
		for _, ct := range playerEngine.controllers {
			require.Equal(t, int64(1), ct.blpStats.NoopQueryCount.Counts()["insert"])
			break
		}
	}
}

// TestPlayerCopyWildcardTableContinuationWithOptimizeInserts tests the copy workflow where tables have been partially copied
// enabling the optimize inserts functionality
func TestPlayerCopyWildcardTableContinuationWithOptimizeInserts(t *testing.T) {
	oldVreplicationExperimentalFlags := vttablet.VReplicationExperimentalFlags
	vttablet.VReplicationExperimentalFlags = vttablet.VReplicationExperimentalFlagOptimizeInserts
	defer func() {
		vttablet.VReplicationExperimentalFlags = oldVreplicationExperimentalFlags
	}()

	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src(id int, val varbinary(128), primary key(id))",
		"insert into src values(2,'copied'), (3,'uncopied')",
		fmt.Sprintf("create table %s.dst(id int, val varbinary(128), primary key(id))", vrepldb),
		fmt.Sprintf("insert into %s.dst values(2,'copied')", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src",
		fmt.Sprintf("drop table %s.dst", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst",
			Filter: "select * from src",
		}},
	}
	pos := primaryPosition(t)
	execStatements(t, []string{
		"insert into src values(4,'new')",
	})

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.BlpStopped, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	lastpk := sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id",
		"int32"),
		"2",
	))
	lastpk.RowsAffected = 0
	execStatements(t, []string{
		fmt.Sprintf("insert into _vt.copy_state (vrepl_id, table_name, lastpk) values(%d, '%s', %s)", qr.InsertID, "dst", encodeString(fmt.Sprintf("%v", lastpk))),
	})
	id := qr.InsertID
	_, err = playerEngine.Exec(fmt.Sprintf("update _vt.vreplication set state='Copying', pos=%s where id=%d", encodeString(pos), id))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", id)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectNontxQueries(t, qh.Expect(
		// Catchup
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set state = 'Copying'",
		"/update _vt.vreplication set message='Picked source tablet.*",
		// Copy
		"insert into dst(id,val) values (3,'uncopied'), (4,'new')",
		`/insert into _vt.copy_state .*`,
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst",
		"/update _vt.vreplication set state='Running'",
	))
	expectData(t, "dst", [][]string{
		{"2", "copied"},
		{"3", "uncopied"},
		{"4", "new"},
	})
	for _, ct := range playerEngine.controllers {
		require.Equal(t, int64(1), ct.blpStats.NoopQueryCount.Counts()["insert"])
		break
	}
}

func TestPlayerCopyTablesNone(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyTablesNone, commonVcopierTestCases())
}

func testPlayerCopyTablesNone(t *testing.T) {
	defer deleteTablet(addTablet(100))

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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectDBClientQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"begin",
		"/update _vt.vreplication set state='Stopped'",
		"commit",
	))
}

func TestPlayerCopyTablesStopAfterCopy(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyTablesStopAfterCopy, commonVcopierTestCases())
}

func testPlayerCopyTablesStopAfterCopy(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), primary key(id))",
		"insert into src1 values(2, 'bbb'), (1, 'aaa')",
		fmt.Sprintf("create table %s.dst1(id int, val varbinary(128), primary key(id))", vrepldb),
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
		Keyspace:      env.KeyspaceName,
		Shard:         env.ShardName,
		Filter:        filter,
		OnDdl:         binlogdatapb.OnDDLAction_IGNORE,
		StopAfterCopy: true,
	}
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectDBClientQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		// Create the list of tables to copy and transition to Copying state.
		"begin",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"commit",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"/update _vt.vreplication set pos=",
	).Then(qh.Eventually(
		"begin",
		"insert into dst1(id,val) values (1,'aaa'), (2,'bbb')",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		"commit",
	)).Then(qh.Immediately(
		// copy of dst1 is done: delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst1",
		// All tables copied. Stop vreplication because we requested it.
		"/update _vt.vreplication set state='Stopped'",
	)))

	expectData(t, "dst1", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
	})
}

func TestPlayerCopyTableCancel(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyTableCancel, commonVcopierTestCases())
}

func testPlayerCopyTableCancel(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), primary key(id))",
		"insert into src1 values(2, 'bbb'), (1, 'aaa')",
		fmt.Sprintf("create table %s.dst1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.dst1", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	saveTimeout := copyPhaseDuration
	copyPhaseDuration = 1 * time.Millisecond
	defer func() { copyPhaseDuration = saveTimeout }()

	// Set a hook to reset the copy timeout after first call.
	vstreamRowsHook = func(ctx context.Context) {
		<-ctx.Done()
		copyPhaseDuration = saveTimeout
		vstreamRowsHook = nil
	}

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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	// Make sure rows get copied in spite of the early context cancel.
	expectDBClientQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		// Create the list of tables to copy and transition to Copying state.
		"begin",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"commit",
		// The first copy will do nothing because we set the timeout to be too low.
		// The next copy should proceed as planned because we've made the timeout high again.
		// The first fast-forward has no starting point. So, it just saves the current position.
		"/update _vt.vreplication set pos=",
	).Then(qh.Eventually(
		"begin",
		"insert into dst1(id,val) values (1,'aaa'), (2,'bbb')",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		"commit",
	)).Then(qh.Immediately(
		// copy of dst1 is done: delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst1",
		// All tables copied. Go into running state.
		"/update _vt.vreplication set state='Running'",
	)))

	expectData(t, "dst1", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
	})
}

func TestPlayerCopyTablesWithGeneratedColumn(t *testing.T) {
	testVcopierTestCases(t, testPlayerCopyTablesWithGeneratedColumn, commonVcopierTestCases())
}

func testPlayerCopyTablesWithGeneratedColumn(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), val2 varbinary(128) as (concat(id, val)), val3 varbinary(128) as (concat(val, id)), id2 int, primary key(id))",
		"insert into src1(id, val, id2) values(2, 'bbb', 20), (1, 'aaa', 10)",
		fmt.Sprintf("create table %s.dst1(id int, val varbinary(128), val2 varbinary(128) as (concat(id, val)), val3 varbinary(128), id2 int, primary key(id))", vrepldb),
		"create table src2(id int, val varbinary(128), val2 varbinary(128) as (concat(id, val)), val3 varbinary(128) as (concat(val, id)), id2 int, primary key(id))",
		"insert into src2(id, val, id2) values(2, 'bbb', 20), (1, 'aaa', 10)",
		fmt.Sprintf("create table %s.dst2(val3 varbinary(128), val varbinary(128), id2 int)", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table src1",
		fmt.Sprintf("drop table %s.dst1", vrepldb),
		"drop table src2",
		fmt.Sprintf("drop table %s.dst2", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select * from src1",
		}, {
			Match:  "dst2",
			Filter: "select val3, val, id2 from src2",
		}},
	}

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectNontxQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message=",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"insert into dst1(id,val,val3,id2) values (1,'aaa','aaa1',10), (2,'bbb','bbb2',20)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		// copy of dst1 is done: delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst1",
		"insert into dst2(val3,val,id2) values ('aaa1','aaa',10), ('bbb2','bbb',20)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		// copy of dst2 is done: delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst2",
		"/update _vt.vreplication set state",
	))

	expectData(t, "dst1", [][]string{
		{"1", "aaa", "1aaa", "aaa1", "10"},
		{"2", "bbb", "2bbb", "bbb2", "20"},
	})
	expectData(t, "dst2", [][]string{
		{"aaa1", "aaa", "10"},
		{"bbb2", "bbb", "20"},
	})
}

func TestCopyTablesWithInvalidDates(t *testing.T) {
	testVcopierTestCases(t, testCopyTablesWithInvalidDates, commonVcopierTestCases())
}

func testCopyTablesWithInvalidDates(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, dt date, primary key(id))",
		fmt.Sprintf("create table %s.dst1(id int, dt date, primary key(id))", vrepldb),
		"insert into src1 values(1, '2020-01-12'), (2, '0000-00-00');",
	})

	// default mysql flavor allows invalid dates: so disallow explicitly for this test
	if err := env.Mysqld.ExecuteSuperQuery(context.Background(), "SET @@global.sql_mode=REPLACE(REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')"); err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
	}
	defer func() {
		if err := env.Mysqld.ExecuteSuperQuery(context.Background(), "SET @@global.sql_mode=REPLACE(@@global.sql_mode, ',NO_ZERO_DATE,NO_ZERO_IN_DATE','')"); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
		}
	}()
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	require.NoError(t, err)

	expectDBClientQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		// Create the list of tables to copy and transition to Copying state.
		"begin",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"commit",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"/update _vt.vreplication set pos=",
	).Then(qh.Eventually(
		"begin",
		"insert into dst1(id,dt) values (1,'2020-01-12'), (2,'0000-00-00')",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} rows:{lengths:1 values:\\"2\\"}'.*`,
		"commit",
	)).Then(qh.Immediately(
		// copy of dst1 is done: delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst1",
		// All tables copied. Final catch up followed by Running state.
		"/update _vt.vreplication set state='Running'",
	)))

	expectData(t, "dst1", [][]string{
		{"1", "2020-01-12"},
		{"2", "0000-00-00"},
	})

	query = fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
	if _, err := playerEngine.Exec(query); err != nil {
		t.Fatal(err)
	}
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"/delete from _vt.vreplication",
		"/delete from _vt.copy_state",
		"/delete from _vt.post_copy_action",
		"commit",
	))
}

func supportsInvisibleColumns() bool {
	if env.DBType == string(mysqlctl.FlavorMySQL) && env.DBMajorVersion >= 8 &&
		(env.DBMinorVersion > 0 || env.DBPatchVersion >= 23) {
		return true
	}
	log.Infof("invisible columns not supported in %d.%d.%d", env.DBMajorVersion, env.DBMinorVersion, env.DBPatchVersion)
	return false
}

func TestCopyInvisibleColumns(t *testing.T) {
	testVcopierTestCases(t, testCopyInvisibleColumns, commonVcopierTestCases())
}

func testCopyInvisibleColumns(t *testing.T) {
	if !supportsInvisibleColumns() {
		t.Skip()
	}

	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, id2 int, inv1 int invisible, inv2 int invisible, primary key(id, inv1))",
		"insert into src1(id, id2, inv1, inv2) values(2, 20, 200, 2000), (1, 10, 100, 1000)",
		fmt.Sprintf("create table %s.dst1(id int, id2 int, inv1 int invisible, inv2 int invisible, primary key(id, inv1))", vrepldb),
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName, 0, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}()

	expectNontxQueries(t, qh.Expect(
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message=",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"insert into dst1(id,id2,inv1,inv2) values (1,10,100,1000), (2,20,200,2000)",
		`/insert into _vt.copy_state \(lastpk, vrepl_id, table_name\) values \('fields:{name:\\"id\\" type:INT32} fields:{name:\\"inv1\\" type:INT32} rows:{lengths:1 lengths:3 values:\\"2200\\"}'.*`,
		// copy of dst1 is done: delete from copy_state.
		"/delete cs, pca from _vt.copy_state as cs left join _vt.post_copy_action as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name.*dst1",
		"/update _vt.vreplication set state='Running'",
	))

	expectData(t, "dst1", [][]string{
		{"1", "10"},
		{"2", "20"},
	})
	expectQueryResult(t, "select id,id2,inv1,inv2 from vrepl.dst1", [][]string{
		{"1", "10", "100", "1000"},
		{"2", "20", "200", "2000"},
	})
}
