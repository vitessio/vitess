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
		"/insert",
		"begin",
		"/insert into _vt.copy_state",
		"/update _vt.vreplication set state='Copying'",
		"commit",
		"rollback",
		"begin",
		"/insert into dst1",
		"/update _vt.copy_state set lastpk",
		"commit",
		"/delete from _vt.copy_state.*dst1",
		"rollback",
		"/delete from _vt.copy_state.*yes",
		"rollback",
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst1", [][]string{
		{"1", "aaa"},
		{"2", "bbb"},
	})
	expectData(t, "yes", [][]string{})
}

func TestPlayerCopyTablePartial(t *testing.T) {
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
	query := binlogplayer.CreateVReplicationState("test", bls, "", binlogplayer.BlpStopped)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	execStatements(t, []string{
		fmt.Sprintf("insert into _vt.copy_state values(%d, '%s', '%s')", qr.InsertID, "dst1", `fields:<name:"id" type:INT32 > rows:<lengths:1 values:"1" > `),
	})
	qr, err = playerEngine.Exec(fmt.Sprintf("update _vt.vreplication set state='Copying' where id=%d", qr.InsertID))
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

	for q := range globalDBQueries {
		if strings.HasPrefix(q, "update") {
			break
		}
	}

	expectDBClientQueries(t, []string{
		"begin",
		"/insert into dst1",
		"/update _vt.copy_state set lastpk",
		"commit",
		"/delete from _vt.copy_state.*dst1",
		"rollback",
		"/update _vt.vreplication set state='Running'",
	})
	expectData(t, "dst1", [][]string{
		{"2", "bbb"},
	})
}
