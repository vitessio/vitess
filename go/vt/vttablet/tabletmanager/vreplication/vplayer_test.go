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

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestFilters(t *testing.T) {
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
	cancel, _ := startVReplication(t, playerEngine, filter, binlogdatapb.OnDDLAction_IGNORE, "")
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

func TestDDL(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	cancel, _ := startVReplication(t, playerEngine, filter, binlogdatapb.OnDDLAction_IGNORE, "")
	execStatements(t, []string{"create table t1(id int, primary key(id))"})
	execStatements(t, []string{"drop table t1"})
	expectDBClientQueries(t, []string{})
	cancel()

	cancel, id := startVReplication(t, playerEngine, filter, binlogdatapb.OnDDLAction_STOP, "")
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
	cancel, _ = startVReplication(t, playerEngine, filter, binlogdatapb.OnDDLAction_EXEC, "")
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
	cancel, _ = startVReplication(t, playerEngine, filter, binlogdatapb.OnDDLAction_EXEC_IGNORE, "")
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

	// Don't test drop.
	// MySQL rewrites them by uppercasing, which may be version specific.
	execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
		"drop table t2",
		fmt.Sprintf("drop table %s.t2", vrepldb),
	})
}

func TestStopPos(t *testing.T) {
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

func execStatements(t *testing.T, queries []string) {
	t.Helper()
	if err := env.Mysqld.ExecuteSuperQueryList(context.Background(), queries); err != nil {
		t.Fatal(err)
	}
}

func startVReplication(t *testing.T, pe *Engine, filter *binlogdatapb.Filter, onddl binlogdatapb.OnDDLAction, pos string) (cancelFunc func(), id int) {
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
	qr, err := pe.Exec(query)
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
		if _, err := pe.Exec(query); err != nil {
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
