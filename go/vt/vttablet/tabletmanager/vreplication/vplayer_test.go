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
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestSimple(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		"create table t2(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
		"drop table t2",
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t2",
			Filter: "select * from t1",
		}},
	}
	cancel := startVReplication(t, playerEngine, filter, "")
	defer cancel()

	execStatements(t, []string{
		"insert into t1 values(1, 'aaa')",
		"insert into t1 values(2, 'aaa')",
		"insert into t1 values(3, 'aaa')",
		"insert into t1 values(4, 'aaa')",
	})
	time.Sleep(1 * time.Second)
	printQueries(t)
	/*
		expectDBClientQueries(t, []string{
			"update _vt.vreplication set state='Running'.*",
			"begin",
			"update _vt.vreplication set pos=.*",
			"commit",
			"begin",
			"insert into t2 set id=1, val='aaa'",
			"update _vt.vreplication set pos=.*",
			"commit",
		})
	*/
}

func execStatements(t *testing.T, queries []string) {
	t.Helper()
	if err := env.Mysqld.ExecuteSuperQueryList(context.Background(), queries); err != nil {
		t.Fatal(err)
	}
}

func startVReplication(t *testing.T, pe *Engine, filter *binlogdatapb.Filter, pos string) (cancelFunc func()) {
	t.Helper()

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
	}
	if pos == "" {
		pos = masterPosition(t)
	}
	query := fmt.Sprintf(`insert into _vt.vreplication`+
		`(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state)`+
		`values('test', '%v', '%s', 9223372036854775807, 9223372036854775807, 481823, 0, 'Running')`,
		bls, pos,
	)
	qr, err := pe.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	resetDBClient()
	return func() {
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := pe.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
}

func masterPosition(t *testing.T) string {
	t.Helper()
	pos, err := env.Mysqld.MasterPosition()
	if err != nil {
		t.Fatal(err)
	}
	return mysql.EncodePosition(pos)
}
