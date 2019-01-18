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

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestSimple(t *testing.T) {
	defer deleteTablet(addTablet(100, "0", topodatapb.TabletType_REPLICA, true, true))

	execStatements(t, []string{
		"create table src1(id int, val varbinary(128), primary key(id))",
		"create table dst1(id int, val varbinary(128), primary key(id))",
		"create table src2(id int, val1 int, val2 int, primary key(id))",
		"create table dst2(id int, val1 int, sval2 int, rcount int, primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table src1",
		"drop table dst1",
		"drop table src2",
		//"drop table dst2",
	})
	env.SchemaEngine.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "dst1",
			Filter: "select * from src1",
		}, {
			Match:  "dst2",
			Filter: "select id, val1, sum(val2) as sval2, count(*) as rcount from src2 group by id",
		}},
	}
	cancel := startVReplication(t, playerEngine, filter, "")
	defer cancel()

	testcases := []struct {
		input  string
		output string
	}{{
		input:  "insert into src1 values(1, 'aaa')",
		output: "insert into dst1 set id=1, val='aaa'",
	}, {
		input:  "update src1 set val='bbb'",
		output: "update dst1 set id=1, val='bbb' where id=1",
	}, {
		input:  "delete from src1 where id=1",
		output: "delete from dst1 where id=1",
	}, {
		input:  "insert into src2 values(1, 2, 3)",
		output: "insert into dst2 set id=1, val1=2, sval2=3, rcount=1 on duplicate key update val1=2, sval2=sval2+3, rcount=rcount+1",
	}, {
		input:  "update src2 set val1=5, val2=1 where id=1",
		output: "update dst2 set val1=5, sval2=sval2-3+1, rcount=rcount-1+1 where id=1",
	}, {
		input:  "delete from src2 where id=1",
		output: "update dst2 set val1=NULL, sval2=sval2-1, rcount=rcount-1 where id=1",
	}}

	for _, tcases := range testcases {
		execStatements(t, []string{
			tcases.input,
		})
		expectDBClientQueries(t, []string{
			"begin",
			tcases.output,
			"/update _vt.vreplication set pos=.*",
			"commit",
		})
	}
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
	// Eat all the initialization queries
	for q := range globalDBQueries {
		if strings.HasPrefix(q, "update") {
			break
		}
	}
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
