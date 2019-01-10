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
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"

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
	vre := NewEngine(env.TopoServ, env.Cells[0], env.Mysqld, realDBClientFactory)
	vre.Open(context.Background())
	defer vre.Close()

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t2",
				Filter: "select * from t1",
			}},
		},
	}
	pos := masterPosition(t)
	query := fmt.Sprintf(`insert into _vt.vreplication`+
		`(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state)`+
		`values('test', '%v', '%s', 9223372036854775807, 9223372036854775807, 481823, 0, 'Running')`,
		bls, pos,
	)
	if _, err := vre.Exec(query); err != nil {
		t.Fatal(err)
	}
	execStatements(t, []string{"insert into t1 values(1, 'aaa')"})
	time.Sleep(1 * time.Second)
}

func execStatements(t *testing.T, queries []string) {
	t.Helper()
	if err := env.Mysqld.ExecuteSuperQueryList(context.Background(), queries); err != nil {
		t.Fatal(err)
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

func realDBClientFactory() binlogplayer.DBClient {
	return realDBClient{}
}

type realDBClient struct{}

func (dbc realDBClient) DBName() string {
	return env.KeyspaceName
}

func (dbc realDBClient) Connect() error {
	return nil
}

func (dbc realDBClient) Begin() error {
	return env.Mysqld.ExecuteSuperQueryList(context.Background(), []string{"begin"})
}

func (dbc realDBClient) Commit() error {
	return env.Mysqld.ExecuteSuperQueryList(context.Background(), []string{"commit"})
}

func (dbc realDBClient) Rollback() error {
	return env.Mysqld.ExecuteSuperQueryList(context.Background(), []string{"rollback"})
}

func (dbc realDBClient) Close() {
}

func (dbc realDBClient) ExecuteFetch(query string, maxrows int) (qr *sqltypes.Result, err error) {
	fmt.Printf("executing: %v\n", query)
	if strings.HasPrefix(query, "use") {
		return nil, nil
	}
	return env.Mysqld.FetchSuperQuery(context.Background(), query)
}
