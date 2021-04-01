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
	"strings"
	"testing"
	"time"

	"context"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
)

func TestPlayerCopyCharPK(t *testing.T) {
	defer deleteTablet(addTablet(100))

	savedPacketSize := *vstreamer.PacketSize
	// PacketSize of 1 byte will send at most one row at a time.
	*vstreamer.PacketSize = 1
	defer func() { *vstreamer.PacketSize = savedPacketSize }()

	savedCopyTimeout := copyTimeout
	// copyTimeout should be low enough to have time to send one row.
	copyTimeout = 500 * time.Millisecond
	defer func() { copyTimeout = savedCopyTimeout }()

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

	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
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

	expectNontxQueries(t, []string{
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"insert into dst(idc,val) values ('a\\0',1)",
		`/update _vt.copy_state set lastpk='fields:<name:\\"idc\\" type:BINARY > rows:<lengths:2 values:\\"a\\\\000\\" > ' where vrepl_id=.*`,
		`update dst set val=3 where idc=cast('a' as binary(2)) and ('a') <= ('a\0')`,
		"insert into dst(idc,val) values ('c\\0',2)",
		`/update _vt.copy_state set lastpk='fields:<name:\\"idc\\" type:BINARY > rows:<lengths:2 values:\\"c\\\\000\\" > ' where vrepl_id=.*`,
		"/delete from _vt.copy_state.*dst",
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst", [][]string{
		{"a\000", "3"},
		{"c\000", "2"},
	})
}

// TestPlayerCopyVarcharPKCaseInsensitive tests the copy/catchup phase for a table with a varchar primary key
// which is case insensitive.
func TestPlayerCopyVarcharPKCaseInsensitive(t *testing.T) {
	defer deleteTablet(addTablet(100))

	savedPacketSize := *vstreamer.PacketSize
	// PacketSize of 1 byte will send at most one row at a time.
	*vstreamer.PacketSize = 1
	defer func() { *vstreamer.PacketSize = savedPacketSize }()

	savedCopyTimeout := copyTimeout
	// copyTimeout should be low enough to have time to send one row.
	copyTimeout = 500 * time.Millisecond
	defer func() { copyTimeout = savedCopyTimeout }()

	savedWaitRetryTime := waitRetryTime
	// waitRetry time should be very low to cause the wait loop to execute multipel times.
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

	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
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

	expectNontxQueries(t, []string{
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"insert into dst(idc,val) values ('a',1)",
		`/update _vt.copy_state set lastpk='fields:<name:\\"idc\\" type:VARCHAR > rows:<lengths:1 values:\\"a\\" > ' where vrepl_id=.*`,
		`/insert into dst\(idc,val\) select 'B', 3 from dual where \( .* 'B' COLLATE .* \) <= \( .* 'a' COLLATE .* \)`,
		"insert into dst(idc,val) values ('B',3)",
		`/update _vt.copy_state set lastpk='fields:<name:\\"idc\\" type:VARCHAR > rows:<lengths:1 values:\\"B\\" > ' where vrepl_id=.*`,
		"insert into dst(idc,val) values ('c',2)",
		`/update _vt.copy_state set lastpk='fields:<name:\\"idc\\" type:VARCHAR > rows:<lengths:1 values:\\"c\\" > ' where vrepl_id=.*`,
		"/delete from _vt.copy_state.*dst",
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst", [][]string{
		{"a", "1"},
		{"B", "3"},
		{"c", "2"},
	})
}

// TestPlayerCopyVarcharPKCaseSensitiveCollation tests the copy/catchup phase for a table with varbinary columns
// (which has a case sensitive collation with upper case alphabets below lower case in sort order)
func TestPlayerCopyVarcharCompositePKCaseSensitiveCollation(t *testing.T) {
	defer deleteTablet(addTablet(100))

	savedPacketSize := *vstreamer.PacketSize
	// PacketSize of 1 byte will send at most one row at a time.
	*vstreamer.PacketSize = 1
	defer func() { *vstreamer.PacketSize = savedPacketSize }()

	savedCopyTimeout := copyTimeout
	// copyTimeout should be low enough to have time to send one row.
	copyTimeout = 500 * time.Millisecond
	defer func() { copyTimeout = savedCopyTimeout }()

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

	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
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

	expectNontxQueries(t, []string{
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"insert into dst(id,idc,idc2,val) values (1,'a','a',1)",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > fields:<name:\\"idc\\" type:VARBINARY > fields:<name:\\"idc2\\" type:VARBINARY > rows:<lengths:1 lengths:1 lengths:1 values:\\"1aa\\" > ' where vrepl_id=.*`,
		`insert into dst(id,idc,idc2,val) select 1, 'B', 'B', 3 from dual where (1,'B','B') <= (1,'a','a')`,
		"insert into dst(id,idc,idc2,val) values (1,'c','c',2)",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > fields:<name:\\"idc\\" type:VARBINARY > fields:<name:\\"idc2\\" type:VARBINARY > rows:<lengths:1 lengths:1 lengths:1 values:\\"1cc\\" > ' where vrepl_id=.*`,
		"/delete from _vt.copy_state.*dst",
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst", [][]string{
		{"1", "B", "B", "3"},
		{"1", "a", "a", "1"},
		{"1", "c", "c", "2"},
	})
}

// TestPlayerCopyTablesWithFK validates that vreplication disables foreign keys during the copy phase
func TestPlayerCopyTablesWithFK(t *testing.T) {
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
	qr, err := playerEngine.Exec(query)
	require.NoError(t, err)

	expectDBClientQueries(t, []string{
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
		"begin",
		"insert into dst1(id,id2) values (1,1), (2,2)",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"2\\" > ' where vrepl_id=.*`,
		"commit",
		// copy of dst1 is done: delete from copy_state.
		"/delete from _vt.copy_state.*dst1",
		// The next FF executes and updates the position before copying.
		"set foreign_key_checks=0;",
		"begin",
		"/update _vt.vreplication set pos=",
		"commit",
		// copy dst2
		"begin",
		"insert into dst2(id,id2) values (1,21), (2,22)",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"2\\" > ' where vrepl_id=.*`,
		"commit",
		// copy of dst1 is done: delete from copy_state.
		"/delete from _vt.copy_state.*dst2",
		// All tables copied. Final catch up followed by Running state.
		"set foreign_key_checks=1;",
		"/update _vt.vreplication set state='Running'",
	})

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
	expectDBClientQueries(t, []string{
		"set foreign_key_checks=1;",
		"begin",
		"/delete from _vt.vreplication",
		"/delete from _vt.copy_state",
		"commit",
	})
}

func TestPlayerCopyTables(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), primary key(id))",
		"insert into src1 values(2, 'bbb'), (1, 'aaa')",
		fmt.Sprintf("create table %s.dst1(id int, val varbinary(128), primary key(id))", vrepldb),
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
			Filter: "select * from src1",
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
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

	expectDBClientQueries(t, []string{
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
		"insert into dst1(id,val) values (1,'aaa'), (2,'bbb')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"2\\" > ' where vrepl_id=.*`,
		"commit",
		// copy of dst1 is done: delete from copy_state.
		"/delete from _vt.copy_state.*dst1",
		// The next FF executes and updates the position before copying.
		"begin",
		"/update _vt.vreplication set pos=",
		"commit",
		// Nothing to copy from yes. Delete from copy_state.
		"/delete from _vt.copy_state.*yes",
		// All tables copied. Final catch up followed by Running state.
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst1", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
	})
	expectData(t, "yes", [][]string{})
	validateCopyRowCountStat(t, 2)
}

// TestPlayerCopyBigTable ensures the copy-catchup back-and-forth loop works correctly.
func TestPlayerCopyBigTable(t *testing.T) {
	defer deleteTablet(addTablet(100))

	savedPacketSize := *vstreamer.PacketSize
	// PacketSize of 1 byte will send at most one row at a time.
	*vstreamer.PacketSize = 1
	defer func() { *vstreamer.PacketSize = savedPacketSize }()

	savedCopyTimeout := copyTimeout
	// copyTimeout should be low enough to have time to send one row.
	copyTimeout = 500 * time.Millisecond
	defer func() { copyTimeout = savedCopyTimeout }()

	savedWaitRetryTime := waitRetryTime
	// waitRetry time shoulw be very low to cause the wait loop to execute multipel times.
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

	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
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

	expectNontxQueries(t, []string{
		// Create the list of tables to copy and transition to Copying state.
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"/update _vt.vreplication set state='Copying'",
		"insert into dst(id,val) values (1,'aaa')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"1\\" > ' where vrepl_id=.*`,
		// The next catchup executes the new row insert, but will be a no-op.
		"insert into dst(id,val) select 3, 'ccc' from dual where (3) <= (1)",
		// fastForward has nothing to add. Just saves position.
		// Second row gets copied.
		"insert into dst(id,val) values (2,'bbb')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"2\\" > ' where vrepl_id=.*`,
		// Third row copied without going back to catchup state.
		"insert into dst(id,val) values (3,'ccc')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"3\\" > ' where vrepl_id=.*`,
		"/delete from _vt.copy_state.*dst",
		// Copy is done. Go into running state.
		// All tables copied. Final catch up followed by Running state.
		"/update _vt.vreplication set state='Running'",
	})
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
	defer deleteTablet(addTablet(100))

	savedPacketSize := *vstreamer.PacketSize
	// PacketSize of 1 byte will send at most one row at a time.
	*vstreamer.PacketSize = 1
	defer func() { *vstreamer.PacketSize = savedPacketSize }()

	savedCopyTimeout := copyTimeout
	// copyTimeout should be low enough to have time to send one row.
	copyTimeout = 500 * time.Millisecond
	defer func() { copyTimeout = savedCopyTimeout }()

	savedWaitRetryTime := waitRetryTime
	// waitRetry time shoulw be very low to cause the wait loop to execute multipel times.
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
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

	expectNontxQueries(t, []string{
		// Create the list of tables to copy and transition to Copying state.
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"insert into src(id,val) values (1,'aaa')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"1\\" > ' where vrepl_id=.*`,
		// The next catchup executes the new row insert, but will be a no-op.
		"insert into src(id,val) select 3, 'ccc' from dual where (3) <= (1)",
		// fastForward has nothing to add. Just saves position.
		// Second row gets copied.
		"insert into src(id,val) values (2,'bbb')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"2\\" > ' where vrepl_id=.*`,
		// Third row copied without going back to catchup state.
		"insert into src(id,val) values (3,'ccc')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"3\\" > ' where vrepl_id=.*`,
		"/delete from _vt.copy_state.*src",
		// Copy is done. Go into running state.
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "src", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
		{"3", "ccc"},
	})
}

// TestPlayerCopyTableContinuation tests the copy workflow where tables have been partially copied.
func TestPlayerCopyTableContinuation(t *testing.T) {
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
	pos := masterPosition(t)
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.BlpStopped, playerEngine.dbName)
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
		fmt.Sprintf("insert into _vt.copy_state values(%d, '%s', %s)", qr.InsertID, "dst1", encodeString(fmt.Sprintf("%v", lastpk))),
		fmt.Sprintf("insert into _vt.copy_state values(%d, '%s', null)", qr.InsertID, "not_copied"),
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

	expectNontxQueries(t, []string{
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
		// Copy
		"insert into dst1(id,val) values (7,'insert out'), (8,'no change'), (10,'updated'), (12,'move out')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id1\\" type:INT32 > fields:<name:\\"id2\\" type:INT32 > rows:<lengths:2 lengths:1 values:\\"126\\" > ' where vrepl_id=.*`,
		"/delete from _vt.copy_state.*dst1",
		// Copy again. There should be no events for catchup.
		"insert into not_copied(id,val) values (1,'bbb')",
		`/update _vt.copy_state set lastpk='fields:<name:\\\"id\\\" type:INT32 > rows:<lengths:1 values:\\\"1\\\" > ' where vrepl_id=.*`,
		"/delete from _vt.copy_state.*not_copied",
		"/update _vt.vreplication set state='Running'",
	})
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

// TestPlayerCopyWildcardTableContinuation tests the copy workflow where tables have been partially copied.
func TestPlayerCopyWildcardTableContinuation(t *testing.T) {
	defer deleteTablet(addTablet(100))

	execStatements(t, []string{
		// src is initialized as partially copied.
		// lastpk will be initialized at (6,6) later below.
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
	pos := masterPosition(t)
	execStatements(t, []string{
		"insert into src values(4,'new')",
	})

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.BlpStopped, playerEngine.dbName)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	// As mentioned above. lastpk cut-off is set at (6,6)
	lastpk := sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id",
		"int32"),
		"2",
	))
	lastpk.RowsAffected = 0
	execStatements(t, []string{
		fmt.Sprintf("insert into _vt.copy_state values(%d, '%s', %s)", qr.InsertID, "dst", encodeString(fmt.Sprintf("%v", lastpk))),
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

	expectNontxQueries(t, []string{
		// Catchup
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set state = 'Copying'",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"insert into dst(id,val) select 4, 'new' from dual where (4) <= (2)",
		// Copy
		"insert into dst(id,val) values (3,'uncopied'), (4,'new')",
		`/update _vt.copy_state set lastpk.*`,
		"/delete from _vt.copy_state.*dst",
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst", [][]string{
		{"2", "copied"},
		{"3", "uncopied"},
		{"4", "new"},
	})
}

func TestPlayerCopyTablesNone(t *testing.T) {
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
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

	expectDBClientQueries(t, []string{
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"begin",
		"/update _vt.vreplication set state='Stopped'",
		"commit",
	})
}

func TestPlayerCopyTablesStopAfterCopy(t *testing.T) {
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
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

	expectDBClientQueries(t, []string{
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
		"insert into dst1(id,val) values (1,'aaa'), (2,'bbb')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"2\\" > ' where vrepl_id=.*`,
		"commit",
		// copy of dst1 is done: delete from copy_state.
		"/delete from _vt.copy_state.*dst1",
		// All tables copied. Stop vreplication because we requested it.
		"/update _vt.vreplication set state='Stopped'",
	})
	expectData(t, "dst1", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
	})
}

func TestPlayerCopyTableCancel(t *testing.T) {
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

	saveTimeout := copyTimeout
	copyTimeout = 1 * time.Millisecond
	defer func() { copyTimeout = saveTimeout }()

	// Set a hook to reset the copy timeout after first call.
	vstreamRowsHook = func(ctx context.Context) {
		<-ctx.Done()
		copyTimeout = saveTimeout
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.VReplicationInit, playerEngine.dbName)
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
	expectDBClientQueries(t, []string{
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
		"begin",
		"insert into dst1(id,val) values (1,'aaa'), (2,'bbb')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"2\\" > ' where vrepl_id=.*`,
		"commit",
		// copy of dst1 is done: delete from copy_state.
		"/delete from _vt.copy_state.*dst1",
		// All tables copied. Go into running state.
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst1", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
	})
}
