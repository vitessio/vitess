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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestPlayerCopyTables(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
		expectDBClientQueries(t, []string{
			"/delete",
		})
	}()

	expectDBClientQueries(t, []string{
		"/insert into _vt.vreplication",
		// Create the list of tables to copy and transition to Copying state.
		"begin",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"commit",
		"rollback",
		// The first fast-forward has no starting point. So, it just saves the current position.
		"/update _vt.vreplication set pos=",
		"begin",
		"insert into dst1(id,val) values (1,'aaa'), (2,'bbb')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"2\\" > ' where vrepl_id=.*`,
		"commit",
		// copy of dst1 is done: delete from copy_state.
		"/delete from _vt.copy_state.*dst1",
		"rollback",
		// The next FF executes and updates the position before copying.
		"begin",
		"/update _vt.vreplication set pos=",
		"commit",
		// Nothing to copy from yes. Delete from copy_state.
		"/delete from _vt.copy_state.*yes",
		"rollback",
		// All tables copied. Final catch up followed by Running state.
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst1", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
	})
	expectData(t, "yes", [][]string{})
}

// TestPlayerCopyTableContinuation tests the copy workflow where tables have been partially copied.
func TestPlayerCopyTableContinuation(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
	streamRowsHook = func(context.Context) {
		execStatements(t, []string{
			"update src1 set val='updated again' where id1 = 3",
		})
		// Set it back to nil. Otherwise, this will get executed again when copying not_copied.
		streamRowsHook = nil
	}
	defer func() { streamRowsHook = nil }()

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
		for q := range globalDBQueries {
			if strings.HasPrefix(q, "delete from _vt.vreplication") {
				break
			}
		}
	}()

	for q := range globalDBQueries {
		if strings.HasPrefix(q, "update") {
			break
		}
	}

	expectNontxQueries(t, []string{
		// Catchup
		"insert into dst1(id,val) select 1, 'insert in' where (1,1) <= (6,6)",
		"insert into dst1(id,val) select 7, 'insert out' where (7,7) <= (6,6)",
		"update dst1 set val='updated' where id=3 and (3,3) <= (6,6)",
		"update dst1 set val='updated' where id=10 and (10,10) <= (6,6)",
		"delete from dst1 where id=4 and (4,4) <= (6,6)",
		"delete from dst1 where id=9 and (9,9) <= (6,6)",
		"delete from dst1 where id=5 and (5,5) <= (6,6)",
		"insert into dst1(id,val) select 5, 'move within' where (5,10) <= (6,6)",
		"delete from dst1 where id=6 and (6,6) <= (6,6)",
		"insert into dst1(id,val) select 12, 'move out' where (12,6) <= (6,6)",
		"delete from dst1 where id=11 and (11,11) <= (6,6)",
		"insert into dst1(id,val) select 4, 'move in' where (4,11) <= (6,6)",
		"update copied set val='bbb' where id=1",
		// Fast-forward
		"update dst1 set val='updated again' where id=3 and (3,3) <= (6,6)",
		// Copy
		"insert into dst1(id,val) values (7,'insert out'), (8,'no change'), (10,'updated'), (12,'move out')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id1\\" type:INT32 > fields:<name:\\"id2\\" type:INT32 > rows:<lengths:2 lengths:1 values:\\"126\\" > ' where vrepl_id=.*`,
		"/delete from _vt.copy_state.*dst1",
		"rollback",
		// Copy again. There should be no events for catchup.
		"insert into not_copied(id,val) values (1,'bbb')",
		`/update _vt.copy_state set lastpk='fields:<name:\\\"id\\\" type:INT32 > rows:<lengths:1 values:\\\"1\\\" > ' where vrepl_id=.*`,
		"/delete from _vt.copy_state.*not_copied",
		"rollback",
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

func TestPlayerCopyTablesNone(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
		expectDBClientQueries(t, []string{
			"/delete",
		})
	}()

	expectDBClientQueries(t, []string{
		"/insert into _vt.vreplication",
		"begin",
		"/update _vt.vreplication set state='Stopped'",
		"commit",
		"rollback",
	})
}

func TestPlayerCopyTableCancel(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
	streamRowsHook = func(ctx context.Context) {
		<-ctx.Done()
		copyTimeout = saveTimeout
		streamRowsHook = nil
	}
	defer func() { streamRowsHook = nil }()

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
		expectDBClientQueries(t, []string{
			"/delete",
		})
	}()

	// Make sure rows get copied in spite of the early context cancel.
	expectDBClientQueries(t, []string{
		"/insert into _vt.vreplication",
		// Create the list of tables to copy and transition to Copying state.
		"begin",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"commit",
		"rollback",
		// The first copy will do nothing because we set the timeout to be too low.
		// We should expect it to do an empty rollback.
		"rollback",
		// The next copy should proceed as planned because we've made the timeout high again.
		// The first fast-forward has no starting point. So, it just saves the current position.
		"/update _vt.vreplication set pos=",
		"begin",
		"insert into dst1(id,val) values (1,'aaa'), (2,'bbb')",
		`/update _vt.copy_state set lastpk='fields:<name:\\"id\\" type:INT32 > rows:<lengths:1 values:\\"2\\" > ' where vrepl_id=.*`,
		"commit",
		// copy of dst1 is done: delete from copy_state.
		"/delete from _vt.copy_state.*dst1",
		"rollback",
		// All tables copied. Final catch up followed by Running state.
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst1", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
	})
}
