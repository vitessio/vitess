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

package vstreamer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestStreamRowsScan(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		// Single PK
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, 'aaa'), (2, 'bbb')",
		// Composite PK
		"create table t2(id1 int, id2 int, val varbinary(128), primary key(id1, id2))",
		"insert into t2 values (1, 2, 'aaa'), (1, 3, 'bbb')",
		// No PK
		"create table t3(id int, val varbinary(128))",
		"insert into t3 values (1, 'aaa'), (2, 'bbb')",
	})
	defer execStatements(t, []string{
		"drop table t1",
		"drop table t2",
		"drop table t3",
	})
	engine.se.Reload(context.Background())

	// t1: all rows
	wantStream := []string{
		`fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > pkfields:<name:"id" type:INT32 > `,
		`rows:<lengths:1 lengths:3 values:"1aaa" > rows:<lengths:1 lengths:3 values:"2bbb" > lastpk:<lengths:1 values:"2" > `,
	}
	wantQuery := "select id, val from t1 order by id"
	checkStream(t, "select * from t1", nil, wantQuery, wantStream)

	// t1: lastpk=1
	wantStream = []string{
		`fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > pkfields:<name:"id" type:INT32 > `,
		`rows:<lengths:1 lengths:3 values:"2bbb" > lastpk:<lengths:1 values:"2" > `,
	}
	wantQuery = "select id, val from t1 where (id) > (1) order by id"
	checkStream(t, "select * from t1", []sqltypes.Value{sqltypes.NewInt64(1)}, wantQuery, wantStream)

	// t1: different column ordering
	wantStream = []string{
		`fields:<name:"val" type:VARBINARY > fields:<name:"id" type:INT32 > pkfields:<name:"id" type:INT32 > `,
		`rows:<lengths:3 lengths:1 values:"aaa1" > rows:<lengths:3 lengths:1 values:"bbb2" > lastpk:<lengths:1 values:"2" > `,
	}
	wantQuery = "select id, val from t1 order by id"
	checkStream(t, "select val, id from t1", nil, wantQuery, wantStream)

	// t2: all rows
	wantStream = []string{
		`fields:<name:"id1" type:INT32 > fields:<name:"id2" type:INT32 > fields:<name:"val" type:VARBINARY > pkfields:<name:"id1" type:INT32 > pkfields:<name:"id2" type:INT32 > `,
		`rows:<lengths:1 lengths:1 lengths:3 values:"12aaa" > rows:<lengths:1 lengths:1 lengths:3 values:"13bbb" > lastpk:<lengths:1 lengths:1 values:"13" > `,
	}
	wantQuery = "select id1, id2, val from t2 order by id1, id2"
	checkStream(t, "select * from t2", nil, wantQuery, wantStream)

	// t2: lastpk=1,2
	wantStream = []string{
		`fields:<name:"id1" type:INT32 > fields:<name:"id2" type:INT32 > fields:<name:"val" type:VARBINARY > pkfields:<name:"id1" type:INT32 > pkfields:<name:"id2" type:INT32 > `,
		`rows:<lengths:1 lengths:1 lengths:3 values:"13bbb" > lastpk:<lengths:1 lengths:1 values:"13" > `,
	}
	wantQuery = "select id1, id2, val from t2 where (id1,id2) > (1,2) order by id1, id2"
	checkStream(t, "select * from t2", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, wantQuery, wantStream)

	// t3: all rows
	wantStream = []string{
		`fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > pkfields:<name:"id" type:INT32 > pkfields:<name:"val" type:VARBINARY > `,
		`rows:<lengths:1 lengths:3 values:"1aaa" > rows:<lengths:1 lengths:3 values:"2bbb" > lastpk:<lengths:1 lengths:3 values:"2bbb" > `,
	}
	wantQuery = "select id, val from t3 order by id, val"
	checkStream(t, "select * from t3", nil, wantQuery, wantStream)

	// t3: lastpk: 1,'aaa'
	wantStream = []string{
		`fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > pkfields:<name:"id" type:INT32 > pkfields:<name:"val" type:VARBINARY > `,
		`rows:<lengths:1 lengths:3 values:"2bbb" > lastpk:<lengths:1 lengths:3 values:"2bbb" > `,
	}
	wantQuery = "select id, val from t3 where (id,val) > (1,'aaa') order by id, val"
	checkStream(t, "select * from t3", []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarBinary("aaa")}, wantQuery, wantStream)
}

func TestStreamRowsKeyRange(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	defer env.SetVSchema("{}")

	execStatements(t, []string{
		"create table t1(id1 int, val varbinary(128), primary key(id1))",
		"insert into t1 values (1, 'aaa'), (6, 'bbb')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	time.Sleep(1 * time.Second)

	// Only the first row should be returned, but lastpk should be 6.
	wantStream := []string{
		`fields:<name:"id1" type:INT32 > fields:<name:"val" type:VARBINARY > pkfields:<name:"id1" type:INT32 > `,
		`rows:<lengths:1 lengths:3 values:"1aaa" > lastpk:<lengths:1 values:"6" > `,
	}
	wantQuery := "select id1, val from t1 order by id1"
	checkStream(t, "select * from t1 where in_keyrange('-80')", nil, wantQuery, wantStream)
}

func TestStreamRowsMultiPacket(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	savedSize := *packetSize
	*packetSize = 10
	defer func() { *packetSize = savedSize }()

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, '234'), (2, '6789'), (3, '1'), (4, '2345678901'), (5, '2')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	wantStream := []string{
		`fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > pkfields:<name:"id" type:INT32 > `,
		`rows:<lengths:1 lengths:3 values:"1234" > rows:<lengths:1 lengths:4 values:"26789" > rows:<lengths:1 lengths:1 values:"31" > lastpk:<lengths:1 values:"3" > `,
		`rows:<lengths:1 lengths:10 values:"42345678901" > lastpk:<lengths:1 values:"4" > `,
		`rows:<lengths:1 lengths:1 values:"52" > lastpk:<lengths:1 values:"5" > `,
	}
	wantQuery := "select id, val from t1 order by id"
	checkStream(t, "select * from t1", nil, wantQuery, wantStream)
}

func TestStreamRowsCancel(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	savedSize := *packetSize
	*packetSize = 10
	defer func() { *packetSize = savedSize }()

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, '234567890'), (2, '234')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := engine.StreamRows(ctx, "select * from t1", nil, func(rows *binlogdatapb.VStreamRowsResponse) error {
		cancel()
		return nil
	})
	if got, want := err.Error(), "stream ended: context canceled"; got != want {
		t.Errorf("err: %v, want %s", err, want)
	}
}

func checkStream(t *testing.T, query string, lastpk []sqltypes.Value, wantQuery string, wantStream []string) {
	t.Helper()

	i := 0
	ch := make(chan error)
	// We don't want to report errors inside callback functions because
	// line numbers come out wrong.
	go func() {
		first := true
		defer close(ch)
		err := engine.StreamRows(context.Background(), query, lastpk, func(rows *binlogdatapb.VStreamRowsResponse) error {
			if first {
				if rows.Gtid == "" {
					ch <- fmt.Errorf("stream gtid is empty")
				}
				if got := engine.rowStreamers[engine.streamIdx-1].sendQuery; got != wantQuery {
					ch <- fmt.Errorf("query sent:\n%v, want\n%v", got, wantQuery)
				}
			}
			first = false
			rows.Gtid = ""
			if i >= len(wantStream) {
				ch <- fmt.Errorf("unexpected stream rows: %v", rows)
				return nil
			}
			srows := fmt.Sprintf("%v", rows)
			if srows != wantStream[i] {
				ch <- fmt.Errorf("stream %d:\n%s, want\n%s", i, srows, wantStream[i])
			}
			i++
			return nil
		})
		if err != nil {
			ch <- err
		}
	}()
	for err := range ch {
		t.Error(err)
	}
}
