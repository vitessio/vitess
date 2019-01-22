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
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestPlayerFilters(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
		}},
	}
	cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_IGNORE, "")
	defer cancel()

	testcases := []struct {
		input  string
		output []string
	}{{
		// insert with insertNormal
		input: "insert into src1 values(1, 'aaa')",
		output: []string{
			"begin",
			"insert into dst1 set id=1, val='aaa'",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// update with insertNormal
		input: "update src1 set val='bbb'",
		output: []string{
			"begin",
			"update dst1 set id=1, val='bbb' where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// delete with insertNormal
		input: "delete from src1 where id=1",
		output: []string{
			"begin",
			"delete from dst1 where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// insert with insertOnDup
		input: "insert into src2 values(1, 2, 3)",
		output: []string{
			"begin",
			"insert into dst2 set id=1, val1=2, sval2=3, rcount=1 on duplicate key update val1=2, sval2=sval2+3, rcount=rcount+1",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// update with insertOnDup
		input: "update src2 set val1=5, val2=1 where id=1",
		output: []string{
			"begin",
			"update dst2 set val1=5, sval2=sval2-3+1, rcount=rcount-1+1 where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// delete with insertOnDup
		input: "delete from src2 where id=1",
		output: []string{
			"begin",
			"update dst2 set val1=NULL, sval2=sval2-1, rcount=rcount-1 where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// insert with insertIgnore
		input: "insert into src3 values(1, 'aaa')",
		output: []string{
			"begin",
			"insert ignore into dst3 set id=1, val='aaa'",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// update with insertIgnore
		input: "update src3 set val='bbb'",
		output: []string{
			"begin",
			"insert ignore into dst3 set id=1, val='bbb'",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// delete with insertIgnore
		input: "delete from src3 where id=1",
		output: []string{
			"begin",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// insert: regular expression filter
		input: "insert into yes values(1, 'aaa')",
		output: []string{
			"begin",
			"insert into yes set id=1, val='aaa'",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// update: regular expression filter
		input: "update yes set val='bbb'",
		output: []string{
			"begin",
			"update yes set id=1, val='bbb' where id=1",
			"/update _vt.vreplication set pos=",
			"commit",
		},
	}, {
		// table should not match a rule
		input:  "insert into no values(1, 'aaa')",
		output: []string{},
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{tcases.input})
		expectDBClientQueries(t, tcases.output)
	}
}

func TestPlayerDDL(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))
	execStatements(t, []string{
		"create table dummy(id int, primary key(id))",
		fmt.Sprintf("create table %s.dummy(id int, primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table dummy",
		fmt.Sprintf("drop table %s.dummy", vrepldb),
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_IGNORE, "")
	// Issue a dummy change to ensure vreplication is initialized. Otherwise there
	// is a race between the DDLs and the schema loader of vstreamer.
	// Root cause seems to be with MySQL where t1 shows up in information_schema before
	// the actual table is created.
	execStatements(t, []string{"insert into dummy values(1)"})
	expectDBClientQueries(t, []string{
		"begin",
		"insert into dummy set id=1",
		"/update _vt.vreplication set pos=",
		"commit",
	})

	execStatements(t, []string{"create table t1(id int, primary key(id))"})
	execStatements(t, []string{"drop table t1"})
	expectDBClientQueries(t, []string{})
	cancel()

	cancel, id := startVReplication(t, filter, binlogdatapb.OnDDLAction_STOP, "")
	execStatements(t, []string{"create table t1(id int, primary key(id))"})
	pos1 := masterPosition(t)
	execStatements(t, []string{"drop table t1"})
	pos2 := masterPosition(t)
	// The stop position must be the GTID of the first DDL
	expectDBClientQueries(t, []string{
		"begin",
		fmt.Sprintf("/update _vt.vreplication set pos='%s'", pos1),
		"/update _vt.vreplication set state='Stopped'",
		"commit",
	})
	// Restart vreplication
	if _, err := playerEngine.Exec(fmt.Sprintf(`update _vt.vreplication set state = 'Running', message='' where id=%d`, id)); err != nil {
		t.Fatal(err)
	}
	// It should stop at the next DDL
	expectDBClientQueries(t, []string{
		"/update.*'Running'",
		"/update.*'Running'",
		"begin",
		fmt.Sprintf("/update.*'%s'", pos2),
		"/update _vt.vreplication set state='Stopped'",
		"commit",
	})
	cancel()

	execStatements(t, []string{fmt.Sprintf("create table %s.t2(id int, primary key(id))", vrepldb)})
	cancel, _ = startVReplication(t, filter, binlogdatapb.OnDDLAction_EXEC, "")
	execStatements(t, []string{"create table t1(id int, primary key(id))"})
	expectDBClientQueries(t, []string{
		"create table t1(id int, primary key(id))",
		"/update _vt.vreplication set pos=",
	})
	execStatements(t, []string{"create table t2(id int, primary key(id))"})
	expectDBClientQueries(t, []string{
		"create table t2(id int, primary key(id))",
		"/update _vt.vreplication set state='Error'",
	})
	cancel()

	// Don't test drop.
	// MySQL rewrites them by uppercasing, which may be version specific.
	execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
		"drop table t2",
		fmt.Sprintf("drop table %s.t2", vrepldb),
	})

	execStatements(t, []string{fmt.Sprintf("create table %s.t2(id int, primary key(id))", vrepldb)})
	cancel, _ = startVReplication(t, filter, binlogdatapb.OnDDLAction_EXEC_IGNORE, "")
	execStatements(t, []string{"create table t1(id int, primary key(id))"})
	expectDBClientQueries(t, []string{
		"create table t1(id int, primary key(id))",
		"/update _vt.vreplication set pos=",
	})
	execStatements(t, []string{"create table t2(id int, primary key(id))"})
	expectDBClientQueries(t, []string{
		"create table t2(id int, primary key(id))",
		"/update _vt.vreplication set pos=",
	})
	cancel()

	execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
		"drop table t2",
		fmt.Sprintf("drop table %s.t2", vrepldb),
	})
}

func TestPlayerStopPos(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
	startPos := masterPosition(t)
	query := binlogplayer.CreateVReplicationStopped("test", bls, startPos)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	id := uint32(qr.InsertID)
	for q := range globalDBQueries {
		if strings.HasPrefix(q, "insert into _vt.vreplication") {
			break
		}
	}

	// Test normal stop.
	execStatements(t, []string{
		"insert into yes values(1, 'aaa')",
	})
	stopPos := masterPosition(t)
	query = binlogplayer.StartVReplicationUntil(id, stopPos)
	if _, err := playerEngine.Exec(query); err != nil {
		t.Fatal(err)
	}
	expectDBClientQueries(t, []string{
		"/update.*'Running'", // done by Engine
		"/update.*'Running'", // done by vplayer on start
		"begin",
		"insert into yes set id=1, val='aaa'",
		fmt.Sprintf("/update.*'%s'", stopPos),
		"/update.*'Stopped'",
		"commit",
	})

	// Test stopping at empty transaction.
	execStatements(t, []string{
		"insert into no values(2, 'aaa')",
		"insert into no values(3, 'aaa')",
	})
	stopPos = masterPosition(t)
	execStatements(t, []string{
		"insert into no values(4, 'aaa')",
	})
	query = binlogplayer.StartVReplicationUntil(id, stopPos)
	if _, err := playerEngine.Exec(query); err != nil {
		t.Fatal(err)
	}
	expectDBClientQueries(t, []string{
		"/update.*'Running'", // done by Engine
		"/update.*'Running'", // done by vplayer on start
		"begin",
		// Since 'no' generates empty transactions that are skipped by
		// vplayer, a commit is done only for the stop position event.
		fmt.Sprintf("/update.*'%s'", stopPos),
		"/update.*'Stopped'",
		"commit",
	})

	// Test stopping when position is already reached.
	query = binlogplayer.StartVReplicationUntil(id, stopPos)
	if _, err := playerEngine.Exec(query); err != nil {
		t.Fatal(err)
	}
	expectDBClientQueries(t, []string{
		"/update.*'Running'", // done by Engine
		"/update.*'Running'", // done by vplayer on start
		"/update.*'Stopped'.*already reached",
	})
}

func TestPlayerIdleUpdate(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
	cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_IGNORE, "")
	defer cancel()

	execStatements(t, []string{
		"insert into t1 values(1, 'aaa')",
	})
	start := time.Now()
	expectDBClientQueries(t, []string{
		"begin",
		"insert into t1 set id=1, val='aaa'",
		"/update _vt.vreplication set pos=",
		"commit",
	})
	// The above write will generate a new binlog event, and
	// that event will loopback into player as an empty event.
	// But it must not get saved until idleTimeout has passed.
	// The exact positions are hard to verify because of this
	// loopback mechanism.
	expectDBClientQueries(t, []string{
		"/update _vt.vreplication set pos=",
	})
	if duration := time.Now().Sub(start); duration < idleTimeout {
		t.Errorf("duration: %v, must be at least %v", duration, idleTimeout)
	}
}

func TestPlayerSplitTransaction(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))
	flag.Set("vstream_packet_size", "10")
	defer flag.Set("vstream_packet_size", "10000")

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
	cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_IGNORE, "")
	defer cancel()

	execStatements(t, []string{
		"begin",
		"insert into t1 values(1, '123456')",
		"insert into t1 values(2, '789012')",
		"commit",
	})
	// Because the packet size is 10, this is received as two events,
	// but still combined as one transaction.
	expectDBClientQueries(t, []string{
		"begin",
		"insert into t1 set id=1, val='123456'",
		"insert into t1 set id=2, val='789012'",
		"/update _vt.vreplication set pos=",
		"commit",
	})
}

func TestPlayerLockErrors(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
	cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_IGNORE, "")
	defer cancel()

	execStatements(t, []string{
		"begin",
		"insert into t1 values(1, 'aaa')",
		"insert into t1 values(2, 'bbb')",
		"commit",
	})
	expectDBClientQueries(t, []string{
		"begin",
		"insert into t1 set id=1, val='aaa'",
		"insert into t1 set id=2, val='bbb'",
		"/update _vt.vreplication set pos=",
		"commit",
	})

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
	expectDBClientQueries(t, []string{
		"begin",
		"update t1 set id=1, val='ccc' where id=1",
		"update t1 set id=2, val='ccc' where id=2",
		"rollback",
	})

	// Release the lock, and watch the retry go through.
	_, _ = vconn.ExecuteFetch("rollback", 1)
	expectDBClientQueries(t, []string{
		"begin",
		"update t1 set id=1, val='ccc' where id=1",
		"update t1 set id=2, val='ccc' where id=2",
		"/update _vt.vreplication set pos=",
		"commit",
	})
}

func TestPlayerCancelOnLock(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
	cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_IGNORE, "")
	defer cancel()

	execStatements(t, []string{
		"begin",
		"insert into t1 values(1, 'aaa')",
		"commit",
	})
	expectDBClientQueries(t, []string{
		"begin",
		"insert into t1 set id=1, val='aaa'",
		"/update _vt.vreplication set pos=",
		"commit",
	})

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
	expectDBClientQueries(t, []string{
		"begin",
		"update t1 set id=1, val='ccc' where id=1",
		"rollback",
	})

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
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
	cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_EXEC, "")
	defer cancel()

	execStatements(t, []string{
		"insert into t1 values(1, 'aaa')",
	})
	expectDBClientQueries(t, []string{
		"begin",
		"insert into t1 set id=1, val='aaa'",
		"/update _vt.vreplication set pos=",
		"commit",
	})

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
	expectDBClientQueries(t, []string{
		"begin",
	})

	// Create two more transactions. They will go and wait in the relayLog.
	execStatements(t, []string{
		"insert into t1 values(2, 'aaa')",
		"insert into t1 values(3, 'aaa')",
		"create table t2(id int, val varbinary(128), primary key(id))",
		"drop table t2",
	})

	// Release the lock.
	_, _ = vconn.ExecuteFetch("rollback", 1)
	// First transaction will complete. The other two
	// transactions must be batched into one. But the
	// DDLs should be on their own.
	expectDBClientQueries(t, []string{
		"update t1 set id=1, val='ccc' where id=1",
		"/update _vt.vreplication set pos=",
		"commit",
		"begin",
		"insert into t1 set id=2, val='aaa'",
		"insert into t1 set id=3, val='aaa'",
		"/update _vt.vreplication set pos=",
		"commit",
		"create table t2(id int, val varbinary(128), primary key(id))",
		"/update _vt.vreplication set pos=",
		"/", // drop table is rewritten by mysql. Don't check.
		"/update _vt.vreplication set pos=",
	})
}

func TestPlayerRelayLogMaxSize(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
			cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_IGNORE, "")
			defer cancel()

			execStatements(t, []string{
				"insert into t1 values(1, '123456')",
			})
			expectDBClientQueries(t, []string{
				"begin",
				"insert into t1 set id=1, val='123456'",
				"/update _vt.vreplication set pos=",
				"commit",
			})

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
			expectDBClientQueries(t, []string{
				"begin",
			})

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
			expectDBClientQueries(t, []string{
				"update t1 set id=1, val='ccc' where id=1",
				"/update _vt.vreplication set pos=",
				"commit",
				"begin",
				"insert into t1 set id=2, val='789012'",
				"insert into t1 set id=3, val='345678'",
				"/update _vt.vreplication set pos=",
				"commit",
				"begin",
				"insert into t1 set id=4, val='901234'",
				"/update _vt.vreplication set pos=",
				"commit",
			})
		}()
	}
}

func TestRestartOnVStreamEnd(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

	savedDelay := *retryDelay
	defer func() { *retryDelay = savedDelay }()
	*retryDelay = 1 * time.Millisecond

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
	cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_IGNORE, "")
	defer cancel()

	execStatements(t, []string{
		"insert into t1 values(1, 'aaa')",
	})
	expectDBClientQueries(t, []string{
		"begin",
		"insert into t1 set id=1, val='aaa'",
		"/update _vt.vreplication set pos=",
		"commit",
	})

	streamerEngine.Close()
	expectDBClientQueries(t, []string{
		"/update.*'Error'.*vstream ended",
	})
	if err := streamerEngine.Open(env.KeyspaceName, env.ShardName); err != nil {
		t.Fatal(err)
	}

	execStatements(t, []string{
		"insert into t1 values(2, 'aaa')",
	})
	expectDBClientQueries(t, []string{
		"/update.*'Running'",
		"begin",
		"insert into t1 set id=2, val='aaa'",
		"/update _vt.vreplication set pos=",
		"commit",
	})
}

func TestTimestamp(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

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
	cancel, _ := startVReplication(t, filter, binlogdatapb.OnDDLAction_IGNORE, "")
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
	expectDBClientQueries(t, []string{
		"begin",
		// The insert value for ts will be in UTC.
		// We'll check the row instead.
		"/insert into t1 set id=",
		"/update _vt.vreplication set pos=",
		"commit",
	})

	qr, err = env.Mysqld.FetchSuperQuery(context.Background(), fmt.Sprintf("select ts, dt from %s.t1 where id=1", vrepldb))
	if err != nil {
		t.Fatal(err)
	}
	// The value for dt should come back in the local timezone.
	if got := qr.Rows[0][0].ToString(); got != want {
		t.Errorf("ts: %s, want %s", got, want)
	}
	// The value for dt should be as is.
	if got := qr.Rows[0][1].ToString(); got != want {
		t.Errorf("ts: %s, want %s", got, want)
	}
}

func execStatements(t *testing.T, queries []string) {
	t.Helper()
	if err := env.Mysqld.ExecuteSuperQueryList(context.Background(), queries); err != nil {
		t.Error(err)
	}
}

func startVReplication(t *testing.T, filter *binlogdatapb.Filter, onddl binlogdatapb.OnDDLAction, pos string) (cancelFunc func(), id int) {
	t.Helper()

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    onddl,
	}
	if pos == "" {
		pos = masterPosition(t)
	}
	query := binlogplayer.CreateVReplication("test", bls, pos, 9223372036854775807, 9223372036854775807, 0)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	// Eat all the initialization queries
	for q := range globalDBQueries {
		if strings.HasPrefix(q, "update") {
			break
		}
	}
	return func() {
		t.Helper()
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDBClientQueries(t, []string{
			"/delete",
		})
	}, int(qr.InsertID)
}

func masterPosition(t *testing.T) string {
	t.Helper()
	pos, err := env.Mysqld.MasterPosition()
	if err != nil {
		t.Fatal(err)
	}
	return mysql.EncodePosition(pos)
}
