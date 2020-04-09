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
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type testcase struct {
	input  interface{}
	output [][]string
}

func TestFilteredVarBinary(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id1 int, val varbinary(128), primary key(id1))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id1, val from t1 where val = 'newton'",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into t1 values (1, 'kepler')",
			"insert into t1 values (2, 'newton')",
			"insert into t1 values (3, 'newton')",
			"insert into t1 values (4, 'kepler')",
			"insert into t1 values (5, 'newton')",
			"update t1 set val = 'newton' where id1 = 1",
			"update t1 set val = 'kepler' where id1 = 2",
			"update t1 set val = 'newton' where id1 = 2",
			"update t1 set val = 'kepler' where id1 = 1",
			"delete from t1 where id1 in (2,3)",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"t1" fields:<name:"id1" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:6 values:"2newton" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:6 values:"3newton" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:6 values:"5newton" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:6 values:"1newton" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:6 values:"2newton" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:6 values:"2newton" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:6 values:"1newton" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:6 values:"2newton" > > row_changes:<before:<lengths:1 lengths:6 values:"3newton" > > > `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, "")
}

func TestFilteredInt(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id1, val from t1 where id2 = 200",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into t1 values (1, 100, 'aaa')",
			"insert into t1 values (2, 200, 'bbb')",
			"insert into t1 values (3, 100, 'ccc')",
			"insert into t1 values (4, 200, 'ddd')",
			"insert into t1 values (5, 200, 'eee')",
			"update t1 set val = 'newddd' where id1 = 4",
			"update t1 set id2 = 200 where id1 = 1",
			"update t1 set id2 = 100 where id1 = 2",
			"update t1 set id2 = 100 where id1 = 1",
			"update t1 set id2 = 200 where id1 = 2",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"t1" fields:<name:"id1" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:3 values:"2bbb" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:3 values:"4ddd" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:3 values:"5eee" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:3 values:"4ddd" > after:<lengths:1 lengths:6 values:"4newddd" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:3 values:"2bbb" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:3 values:"2bbb" > > > `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, "")
}

func TestStatements(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
		"create table stream2(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table stream1",
		"drop table stream2",
	})
	engine.se.Reload(context.Background())

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into stream1 values (1, 'aaa')",
			"update stream1 set val='bbb' where id = 1",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"stream1" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"stream1" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"stream1" row_changes:<before:<lengths:1 lengths:3 values:"1aaa" > after:<lengths:1 lengths:3 values:"1bbb" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		// Normal DDL.
		input: "alter table stream1 change column val val varbinary(128)",
		output: [][]string{{
			`gtid`,
			`type:DDL ddl:"alter table stream1 change column val val varbinary(128)" `,
		}},
	}, {
		// DDL padded with comments.
		input: " /* prefix */ alter table stream1 change column val val varbinary(256) /* suffix */ ",
		output: [][]string{{
			`gtid`,
			`type:DDL ddl:"/* prefix */ alter table stream1 change column val val varbinary(256) /* suffix */" `,
		}},
	}, {
		// Multiple tables, and multiple rows changed per statement.
		input: []string{
			"begin",
			"insert into stream1 values (2, 'bbb')",
			"insert into stream2 values (1, 'aaa')",
			"update stream1 set val='ccc'",
			"delete from stream1",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"stream1" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"stream1" row_changes:<after:<lengths:1 lengths:3 values:"2bbb" > > > `,
			`type:FIELD field_event:<table_name:"stream2" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"stream2" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"stream1" ` +
				`row_changes:<before:<lengths:1 lengths:3 values:"1bbb" > after:<lengths:1 lengths:3 values:"1ccc" > > ` +
				`row_changes:<before:<lengths:1 lengths:3 values:"2bbb" > after:<lengths:1 lengths:3 values:"2ccc" > > > `,
			`type:ROW row_event:<table_name:"stream1" ` +
				`row_changes:<before:<lengths:1 lengths:3 values:"1ccc" > > ` +
				`row_changes:<before:<lengths:1 lengths:3 values:"2ccc" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		// truncate is a DDL
		input: "truncate table stream2",
		output: [][]string{{
			`gtid`,
			`type:DDL ddl:"truncate table stream2" `,
		}},
	}}
	runCases(t, nil, testcases, "current")

	// Test FilePos flavor
	savedEngine := engine
	defer func() { engine = savedEngine }()
	engine = customEngine(t, func(in mysql.ConnParams) mysql.ConnParams {
		in.Flavor = "FilePos"
		return in
	})
	defer engine.Close()
	runCases(t, nil, testcases, "current")
}

// TestOther tests "other" and "priv" statements. These statements can
// produce very different events depending on the version of mysql or
// mariadb. So, we just show that vreplication transmits "OTHER" events
// if the binlog is affected by the statement.
func TestOther(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
		"create table stream2(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table stream1",
		"drop table stream2",
	})
	engine.se.Reload(context.Background())

	testcases := []string{
		"repair table stream2",
		"optimize table stream2",
		"analyze table stream2",
		"select * from stream1",
		"set @val=1",
		"show tables",
		"describe stream1",
		"grant select on stream1 to current_user()",
		"revoke select on stream1 from current_user()",
	}

	// customRun is a modified version of runCases.
	customRun := func(mode string) {
		t.Logf("Run mode: %v", mode)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch := startStream(ctx, t, nil, "")

		want := [][]string{{
			`gtid`,
			`type:OTHER `,
		}}

		for _, stmt := range testcases {
			startPosition := masterPosition(t)
			execStatement(t, stmt)
			endPosition := masterPosition(t)
			if startPosition == endPosition {
				t.Logf("statement %s did not affect binlog", stmt)
				continue
			}
			expectLog(ctx, t, stmt, ch, want)
		}
		cancel()
		if evs, ok := <-ch; ok {
			t.Fatalf("unexpected evs: %v", evs)
		}
	}
	customRun("gtid")

	// Test FilePos flavor
	savedEngine := engine
	defer func() { engine = savedEngine }()
	engine = customEngine(t, func(in mysql.ConnParams) mysql.ConnParams {
		in.Flavor = "FilePos"
		return in
	})
	defer engine.Close()
	customRun("filePos")
}

func TestRegexp(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table yes_stream(id int, val varbinary(128), primary key(id))",
		"create table no_stream(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table yes_stream",
		"drop table no_stream",
	})
	engine.se.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/yes.*/",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into yes_stream values (1, 'aaa')",
			"insert into no_stream values (2, 'bbb')",
			"update yes_stream set val='bbb' where id = 1",
			"update no_stream set val='bbb' where id = 2",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"yes_stream" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"yes_stream" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"yes_stream" row_changes:<before:<lengths:1 lengths:3 values:"1aaa" > after:<lengths:1 lengths:3 values:"1bbb" > > > `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, "")
}

func TestREKeyRange(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	setVSchema(t, shardedVSchema)
	defer env.SetVSchema("{}")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "/.*/",
			Filter: "-80",
		}},
	}
	ch := startStream(ctx, t, filter, "")

	// 1, 2, 3 and 5 are in shard -80.
	// 4 and 6 are in shard 80-.
	input := []string{
		"begin",
		"insert into t1 values (1, 4, 'aaa')",
		"insert into t1 values (4, 1, 'bbb')",
		// Stay in shard.
		"update t1 set id1 = 2 where id1 = 1",
		// Move from -80 to 80-.
		"update t1 set id1 = 6 where id1 = 2",
		// Move from 80- to -80.
		"update t1 set id1 = 3 where id1 = 4",
		"commit",
	}
	execStatements(t, input)
	expectLog(ctx, t, input, ch, [][]string{{
		`begin`,
		`type:FIELD field_event:<table_name:"t1" fields:<name:"id1" type:INT32 > fields:<name:"id2" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 values:"14aaa" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:1 lengths:3 values:"14aaa" > after:<lengths:1 lengths:1 lengths:3 values:"24aaa" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:1 lengths:3 values:"24aaa" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 values:"31bbb" > > > `,
		`gtid`,
		`commit`,
	}})

	// Switch the vschema to make id2 the primary vindex.
	altVSchema := `{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "hash"
        }
      ]
    }
  }
}`
	setVSchema(t, altVSchema)

	// Only the first insert should be sent.
	input = []string{
		"begin",
		"insert into t1 values (4, 1, 'aaa')",
		"insert into t1 values (1, 4, 'aaa')",
		"commit",
	}
	execStatements(t, input)
	expectLog(ctx, t, input, ch, [][]string{{
		`begin`,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 values:"41aaa" > > > `,
		`gtid`,
		`commit`,
	}})
}

func TestInKeyRangeMultiColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(region int, id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	setVSchema(t, multicolumnVSchema)
	defer env.SetVSchema("{}")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id, region, val, keyspace_id() from t1 where in_keyrange('-80')",
		}},
	}
	ch := startStream(ctx, t, filter, "")

	// 1, 2, 3 and 5 are in shard -80.
	// 4 and 6 are in shard 80-.
	input := []string{
		"begin",
		"insert into t1 values (1, 1, 'aaa')",
		"insert into t1 values (128, 2, 'bbb')",
		// Stay in shard.
		"update t1 set region = 2 where id = 1",
		// Move from -80 to 80-.
		"update t1 set region = 128 where id = 1",
		// Move from 80- to -80.
		"update t1 set region = 1 where id = 2",
		"commit",
	}
	execStatements(t, input)
	expectLog(ctx, t, input, ch, [][]string{{
		`begin`,
		`type:FIELD field_event:<table_name:"t1" fields:<name:"id" type:INT32 > fields:<name:"region" type:INT32 > fields:<name:"val" type:VARBINARY > fields:<name:"keyspace_id" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 lengths:9 values:"11aaa\001\026k@\264J\272K\326" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:1 lengths:3 lengths:9 values:"11aaa\001\026k@\264J\272K\326" > ` +
			`after:<lengths:1 lengths:1 lengths:3 lengths:9 values:"12aaa\002\026k@\264J\272K\326" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:1 lengths:3 lengths:9 values:"12aaa\002\026k@\264J\272K\326" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 lengths:9 values:"21bbb\001\006\347\352\"\316\222p\217" > > > `,
		`gtid`,
		`commit`,
	}})
}

func TestREMultiColumnVindex(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(region int, id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	setVSchema(t, multicolumnVSchema)
	defer env.SetVSchema("{}")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "/.*/",
			Filter: "-80",
		}},
	}
	ch := startStream(ctx, t, filter, "")

	// 1, 2, 3 and 5 are in shard -80.
	// 4 and 6 are in shard 80-.
	input := []string{
		"begin",
		"insert into t1 values (1, 1, 'aaa')",
		"insert into t1 values (128, 2, 'bbb')",
		// Stay in shard.
		"update t1 set region = 2 where id = 1",
		// Move from -80 to 80-.
		"update t1 set region = 128 where id = 1",
		// Move from 80- to -80.
		"update t1 set region = 1 where id = 2",
		"commit",
	}
	execStatements(t, input)
	expectLog(ctx, t, input, ch, [][]string{{
		`begin`,
		`type:FIELD field_event:<table_name:"t1" fields:<name:"region" type:INT32 > fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 values:"11aaa" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:1 lengths:3 values:"11aaa" > after:<lengths:1 lengths:1 lengths:3 values:"21aaa" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:1 lengths:3 values:"21aaa" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 values:"12bbb" > > > `,
		`gtid`,
		`commit`,
	}})
}

func TestSelectFilter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, val varbinary(128), primary key(id1))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id2, val from t1 where in_keyrange(id2, 'hash', '-80')",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into t1 values (4, 1, 'aaa')",
			"insert into t1 values (2, 4, 'aaa')",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"t1" fields:<name:"id2" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, "")
}

func TestDDLAddColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table ddl_test1(id int, val1 varbinary(128), primary key(id))",
		"create table ddl_test2(id int, val1 varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table ddl_test1",
		"drop table ddl_test2",
	})

	// Record position before the next few statements.
	pos := masterPosition(t)
	execStatements(t, []string{
		"begin",
		"insert into ddl_test1 values(1, 'aaa')",
		"insert into ddl_test2 values(1, 'aaa')",
		"commit",
		// Adding columns is allowed.
		"alter table ddl_test1 add column val2 varbinary(128)",
		"alter table ddl_test2 add column val2 varbinary(128)",
		"begin",
		"insert into ddl_test1 values(2, 'bbb', 'ccc')",
		"insert into ddl_test2 values(2, 'bbb', 'ccc')",
		"commit",
	})
	engine.se.Reload(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test RE as well as select-based filters.
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "ddl_test2",
			Filter: "select * from ddl_test2",
		}, {
			Match: "/.*/",
		}},
	}

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		defer close(ch)
		if err := vstream(ctx, t, pos, filter, ch); err != nil {
			t.Error(err)
		}
	}()
	expectLog(ctx, t, "ddls", ch, [][]string{{
		// Current schema has 3 columns, but they'll be truncated to match the two columns in the event.
		`begin`,
		`type:FIELD field_event:<table_name:"ddl_test1" fields:<name:"id" type:INT32 > fields:<name:"val1" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"ddl_test1" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
		`type:FIELD field_event:<table_name:"ddl_test2" fields:<name:"id" type:INT32 > fields:<name:"val1" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"ddl_test2" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
		`gtid`,
		`commit`,
	}, {
		`gtid`,
		`type:DDL ddl:"alter table ddl_test1 add column val2 varbinary(128)" `,
	}, {
		`gtid`,
		`type:DDL ddl:"alter table ddl_test2 add column val2 varbinary(128)" `,
	}, {
		// The plan will be updated to now include the third column
		// because the new table map will have three columns.
		`begin`,
		`type:FIELD field_event:<table_name:"ddl_test1" fields:<name:"id" type:INT32 > fields:<name:"val1" type:VARBINARY > fields:<name:"val2" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"ddl_test1" row_changes:<after:<lengths:1 lengths:3 lengths:3 values:"2bbbccc" > > > `,
		`type:FIELD field_event:<table_name:"ddl_test2" fields:<name:"id" type:INT32 > fields:<name:"val1" type:VARBINARY > fields:<name:"val2" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"ddl_test2" row_changes:<after:<lengths:1 lengths:3 lengths:3 values:"2bbbccc" > > > `,
		`gtid`,
		`commit`,
	}})
}

func TestDDLDropColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatement(t, "create table ddl_test2(id int, val1 varbinary(128), val2 varbinary(128), primary key(id))")
	defer execStatement(t, "drop table ddl_test2")

	// Record position before the next few statements.
	pos := masterPosition(t)
	execStatements(t, []string{
		"insert into ddl_test2 values(1, 'aaa', 'ccc')",
		// Adding columns is allowed.
		"alter table ddl_test2 drop column val2",
		"insert into ddl_test2 values(2, 'bbb')",
	})
	engine.se.Reload(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		for range ch {
		}
	}()
	defer close(ch)
	err := vstream(ctx, t, pos, nil, ch)
	want := "cannot determine table columns"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}
}

func TestUnsentDDL(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatement(t, "create table unsent(id int, val varbinary(128), primary key(id))")

	testcases := []testcase{{
		input: []string{
			"drop table unsent",
		},
		// An unsent DDL is sent as an empty transaction.
		output: [][]string{{
			`gtid`,
			`type:OTHER `,
		}},
	}}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/none/",
		}},
	}
	runCases(t, filter, testcases, "")
}

func TestBuffering(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	savedSize := *PacketSize
	*PacketSize = 10
	defer func() { *PacketSize = savedSize }()

	execStatement(t, "create table packet_test(id int, val varbinary(128), primary key(id))")
	defer execStatement(t, "drop table packet_test")
	engine.se.Reload(context.Background())

	testcases := []testcase{{
		// All rows in one packet.
		input: []string{
			"begin",
			"insert into packet_test values (1, '123')",
			"insert into packet_test values (2, '456')",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"packet_test" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:3 values:"1123" > > > `,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:3 values:"2456" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		// A new row causes packet size to be exceeded.
		// Also test deletes
		input: []string{
			"begin",
			"insert into packet_test values (3, '123456')",
			"insert into packet_test values (4, '789012')",
			"delete from packet_test where id=3",
			"delete from packet_test where id=4",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:6 values:"3123456" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:6 values:"4789012" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<before:<lengths:1 lengths:6 values:"3123456" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<before:<lengths:1 lengths:6 values:"4789012" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		// A single row is itself bigger than the packet size.
		input: []string{
			"begin",
			"insert into packet_test values (5, '123456')",
			"insert into packet_test values (6, '12345678901')",
			"insert into packet_test values (7, '23456')",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:6 values:"5123456" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:11 values:"612345678901" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:5 values:"723456" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		// An update packet is bigger because it has a before and after image.
		input: []string{
			"begin",
			"insert into packet_test values (8, '123')",
			"update packet_test set val='456' where id=8",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:3 values:"8123" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<before:<lengths:1 lengths:3 values:"8123" > after:<lengths:1 lengths:3 values:"8456" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		// DDL is in its own packet
		input: []string{
			"alter table packet_test change val val varchar(128)",
		},
		output: [][]string{{
			`gtid`,
			`type:DDL ddl:"alter table packet_test change val val varchar(128)" `,
		}},
	}}
	runCases(t, nil, testcases, "")
}

func TestBestEffortNameInFieldEvent(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	filter := &binlogdatapb.Filter{
		FieldEventMode: binlogdatapb.Filter_BEST_EFFORT,
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	// Modeled after vttablet endtoend compatibility tests.
	execStatements(t, []string{
		"create table vitess_test(id int, val varbinary(128), primary key(id))",
	})
	position := masterPosition(t)
	execStatements(t, []string{
		"insert into vitess_test values(1, 'abc')",
		"rename table vitess_test to vitess_test_new",
	})

	defer execStatements(t, []string{
		"drop table vitess_test_new",
	})
	engine.se.Reload(context.Background())
	testcases := []testcase{{
		input: []string{
			"insert into vitess_test_new values(2, 'abc')",
		},
		// In this case, we don't have information about vitess_test since it was renamed to vitess_test_test.
		// information returned by binlog for val column == varchar (rather than varbinary).
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"vitess_test" fields:<name:"@1" type:INT32 > fields:<name:"@2" type:VARCHAR > > `,
			`type:ROW row_event:<table_name:"vitess_test" row_changes:<after:<lengths:1 lengths:3 values:"1abc" > > > `,
			`gtid`,
			`commit`,
		}, {
			`gtid`,
			`type:DDL ddl:"rename table vitess_test to vitess_test_new" `,
		}, {
			`begin`,
			`type:FIELD field_event:<table_name:"vitess_test_new" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"vitess_test_new" row_changes:<after:<lengths:1 lengths:3 values:"2abc" > > > `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, position)
}

func TestTypes(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Modeled after vttablet endtoend compatibility tests.
	execStatements(t, []string{
		"create table vitess_ints(tiny tinyint, tinyu tinyint unsigned, small smallint, smallu smallint unsigned, medium mediumint, mediumu mediumint unsigned, normal int, normalu int unsigned, big bigint, bigu bigint unsigned, y year, primary key(tiny))",
		"create table vitess_fracts(id int, deci decimal(5,2), num numeric(5,2), f float, d double, primary key(id))",
		"create table vitess_strings(vb varbinary(16), c char(16), vc varchar(16), b binary(4), tb tinyblob, bl blob, ttx tinytext, tx text, en enum('a','b'), s set('a','b'), primary key(vb))",
		"create table vitess_misc(id int, b bit(8), d date, dt datetime, t time, g geometry, primary key(id))",
		"create table vitess_null(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table vitess_ints",
		"drop table vitess_fracts",
		"drop table vitess_strings",
		"drop table vitess_misc",
		"drop table vitess_null",
	})
	engine.se.Reload(context.Background())

	testcases := []testcase{{
		input: []string{
			"insert into vitess_ints values(-128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 2012)",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"vitess_ints" ` +
				`fields:<name:"tiny" type:INT8 > ` +
				`fields:<name:"tinyu" type:UINT8 > ` +
				`fields:<name:"small" type:INT16 > ` +
				`fields:<name:"smallu" type:UINT16 > ` +
				`fields:<name:"medium" type:INT24 > ` +
				`fields:<name:"mediumu" type:UINT24 > ` +
				`fields:<name:"normal" type:INT32 > ` +
				`fields:<name:"normalu" type:UINT32 > ` +
				`fields:<name:"big" type:INT64 > ` +
				`fields:<name:"bigu" type:UINT64 > ` +
				`fields:<name:"y" type:YEAR > > `,
			`type:ROW row_event:<table_name:"vitess_ints" row_changes:<after:<lengths:4 lengths:3 lengths:6 lengths:5 lengths:8 lengths:8 lengths:11 lengths:10 lengths:20 lengths:20 lengths:4 values:"` +
				`-128` +
				`255` +
				`-32768` +
				`65535` +
				`-8388608` +
				`16777215` +
				`-2147483648` +
				`4294967295` +
				`-9223372036854775808` +
				`18446744073709551615` +
				`2012` +
				`" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		input: []string{
			"insert into vitess_fracts values(1, 1.99, 2.99, 3.99, 4.99)",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"vitess_fracts" ` +
				`fields:<name:"id" type:INT32 > ` +
				`fields:<name:"deci" type:DECIMAL > ` +
				`fields:<name:"num" type:DECIMAL > ` +
				`fields:<name:"f" type:FLOAT32 > ` +
				`fields:<name:"d" type:FLOAT64 > > `,
			`type:ROW row_event:<table_name:"vitess_fracts" row_changes:<after:<lengths:1 lengths:4 lengths:4 lengths:8 lengths:8 values:"` +
				`1` +
				`1.99` +
				`2.99` +
				`3.99E+00` +
				`4.99E+00` +
				`" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		// TODO(sougou): validate that binary and char data generate correct DMLs on the other end.
		input: []string{
			"insert into vitess_strings values('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'a', 'a,b')",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"vitess_strings" ` +
				`fields:<name:"vb" type:VARBINARY > ` +
				`fields:<name:"c" type:CHAR > ` +
				`fields:<name:"vc" type:VARCHAR > ` +
				`fields:<name:"b" type:BINARY > ` +
				`fields:<name:"tb" type:BLOB > ` +
				`fields:<name:"bl" type:BLOB > ` +
				`fields:<name:"ttx" type:TEXT > ` +
				`fields:<name:"tx" type:TEXT > ` +
				`fields:<name:"en" type:ENUM > ` +
				`fields:<name:"s" type:SET > > `,
			`type:ROW row_event:<table_name:"vitess_strings" row_changes:<after:<lengths:1 lengths:1 lengths:1 lengths:4 lengths:1 lengths:1 lengths:1 lengths:1 lengths:1 lengths:1 ` +
				`values:"abcd\000\000\000efgh13" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		// TODO(sougou): validate that the geometry value generates the correct DMLs on the other end.
		input: []string{
			"insert into vitess_misc values(1, '\x01', '2012-01-01', '2012-01-01 15:45:45', '15:45:45', point(1, 2))",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"vitess_misc" ` +
				`fields:<name:"id" type:INT32 > ` +
				`fields:<name:"b" type:BIT > ` +
				`fields:<name:"d" type:DATE > ` +
				`fields:<name:"dt" type:DATETIME > ` +
				`fields:<name:"t" type:TIME > ` +
				`fields:<name:"g" type:GEOMETRY > > `,
			`type:ROW row_event:<table_name:"vitess_misc" row_changes:<after:<lengths:1 lengths:1 lengths:10 lengths:19 lengths:8 lengths:25 values:"` +
				`1` +
				`\001` +
				`2012-01-01` +
				`2012-01-01 15:45:45` +
				`15:45:45` +
				`\000\000\000\000\001\001\000\000\000\000\000\000\000\000\000\360?\000\000\000\000\000\000\000@` +
				`" > > > `,
			`gtid`,
			`commit`,
		}},
	}, {
		input: []string{
			"insert into vitess_null values(1, null)",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"vitess_null" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"vitess_null" row_changes:<after:<lengths:1 lengths:-1 values:"1" > > > `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "")
}

func TestJSON(t *testing.T) {
	t.Skip("This test is disabled because every flavor of mysql has a different behavior.")

	// JSON is supported only after mysql57.
	if err := env.Mysqld.ExecuteSuperQuery(context.Background(), "create table vitess_json(id int default 1, val json, primary key(id))"); err != nil {
		// If it's a syntax error, MySQL is an older version. Skip this test.
		if strings.Contains(err.Error(), "syntax") {
			return
		}
		t.Fatal(err)
	}
	defer execStatement(t, "drop table vitess_json")
	engine.se.Reload(context.Background())

	testcases := []testcase{{
		input: []string{
			`insert into vitess_json values(1, '{"foo": "bar"}')`,
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"vitess_json" fields:<name:"id" type:INT32 > fields:<name:"val" type:JSON > > `,
			`type:ROW row_event:<table_name:"vitess_json" row_changes:<after:<lengths:1 lengths:24 values:"1JSON_OBJECT('foo','bar')" > > > `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "")
}

func TestExternalTable(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create database external",
		"create table external.ext(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop database external",
	})
	engine.se.Reload(context.Background())

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into external.ext values (1, 'aaa')",
			"commit",
		},
		// External table events don't get sent.
		output: [][]string{{
			`begin`,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "")
}

func TestJournal(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table _vt.resharding_journal(id int, db_name varchar(128), val blob, primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table _vt.resharding_journal",
	})
	engine.se.Reload(context.Background())

	journal1 := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_SHARDS,
	}
	journal2 := &binlogdatapb.Journal{
		Id:            2,
		MigrationType: binlogdatapb.MigrationType_SHARDS,
	}
	testcases := []testcase{{
		input: []string{
			"begin",
			fmt.Sprintf("insert into _vt.resharding_journal values(1, 'vttest', '%v')", journal1.String()),
			fmt.Sprintf("insert into _vt.resharding_journal values(2, 'nosend', '%v')", journal2.String()),
			"commit",
		},
		// External table events don't get sent.
		output: [][]string{{
			`begin`,
			`type:JOURNAL journal:<id:1 migration_type:SHARDS > `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "")
}

func TestMinimalMode(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id int, val1 varbinary(128), val2 varbinary(128), primary key(id))",
		"insert into t1 values(1, 'aaa', 'bbb')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	// Record position before the next few statements.
	pos := masterPosition(t)
	execStatements(t, []string{
		"set @@session.binlog_row_image='minimal'",
		"update t1 set val1='bbb' where id=1",
		"set @@session.binlog_row_image='full'",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		for evs := range ch {
			t.Errorf("received: %v", evs)
		}
	}()
	defer close(ch)
	err := vstream(ctx, t, pos, nil, ch)
	want := "partial row image encountered"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain '%s'", err, want)
	}
}

func TestStatementMode(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
		"create table stream2(id int, val varbinary(128), primary key(id))",
	})

	engine.se.Reload(context.Background())

	defer execStatements(t, []string{
		"drop table stream1",
		"drop table stream2",
	})

	testcases := []testcase{{
		input: []string{
			"set @@session.binlog_format='STATEMENT'",
			"begin",
			"insert into stream1 values (1, 'aaa')",
			"update stream1 set val='bbb' where id = 1",
			"delete from stream1 where id = 1",
			"commit",
			"set @@session.binlog_format='ROW'",
		},
		output: [][]string{{
			`begin`,
			`type:INSERT dml:"insert into stream1 values (1, 'aaa')" `,
			`type:UPDATE dml:"update stream1 set val='bbb' where id = 1" `,
			`type:DELETE dml:"delete from stream1 where id = 1" `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases, "")
}

func TestHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := startStream(ctx, t, nil, "")
	evs := <-ch
	require.Equal(t, 1, len(evs))
	assert.Equal(t, binlogdatapb.VEventType_HEARTBEAT, evs[0].Type)
}

func TestNoFutureGTID(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Execute something to make sure we have ranges in GTIDs.
	execStatements(t, []string{
		"create table stream1(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table stream1",
	})
	engine.se.Reload(context.Background())

	pos := masterPosition(t)
	t.Logf("current position: %v", pos)
	// Both mysql and mariadb have '-' in their gtids.
	// Invent a GTID in the future.
	index := strings.LastIndexByte(pos, '-')
	num, err := strconv.Atoi(pos[index+1:])
	require.NoError(t, err)
	future := pos[:index+1] + fmt.Sprintf("%d", num+1)
	t.Logf("future position: %v", future)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		for range ch {
		}
	}()
	defer close(ch)
	err = vstream(ctx, t, future, nil, ch)
	want := "is ahead of current position"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}
}

func TestFilteredMultipleWhere(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id1 int, id2 int, id3 int, val varbinary(128), primary key(id1))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	engine.se.Reload(context.Background())

	setVSchema(t, shardedVSchema)
	defer env.SetVSchema("{}")

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id1, val from t1 where in_keyrange('-80') and id2 = 200 and id3 = 1000 and val = 'newton'",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into t1 values (1, 100, 1000, 'kepler')",
			"insert into t1 values (2, 200, 1000, 'newton')",
			"insert into t1 values (3, 100, 2000, 'kepler')",
			"insert into t1 values (128, 200, 1000, 'newton')",
			"insert into t1 values (5, 200, 2000, 'kepler')",
			"insert into t1 values (129, 200, 1000, 'kepler')",
			"commit",
		},
		output: [][]string{{
			`begin`,
			`type:FIELD field_event:<table_name:"t1" fields:<name:"id1" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:6 values:"2newton" > > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:3 lengths:6 values:"128newton" > > > `,
			`gtid`,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases, "")
}

func runCases(t *testing.T, filter *binlogdatapb.Filter, testcases []testcase, position string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := startStream(ctx, t, filter, position)

	// If position is 'current', we wait for a heartbeat to be
	// sure the vstreamer has started.
	if position == "current" {
		expectLog(ctx, t, "current pos", ch, [][]string{{`gtid`, `type:OTHER `}})
	}

	for _, tcase := range testcases {
		switch input := tcase.input.(type) {
		case []string:
			execStatements(t, input)
		case string:
			execStatement(t, input)
		default:
			t.Fatalf("unexpected input: %#v", input)
		}
		expectLog(ctx, t, tcase.input, ch, tcase.output)
	}
	cancel()
	if evs, ok := <-ch; ok {
		t.Fatalf("unexpected evs: %v", evs)
	}
}

func expectLog(ctx context.Context, t *testing.T, input interface{}, ch <-chan []*binlogdatapb.VEvent, output [][]string) {
	t.Helper()
	timer := time.NewTimer(1 * time.Minute)
	defer timer.Stop()
	for _, wantset := range output {
		var evs []*binlogdatapb.VEvent
		for {
			select {
			case allevs, ok := <-ch:
				if !ok {
					t.Fatal("stream ended early")
				}
				for _, ev := range allevs {
					// Ignore spurious heartbeats that can happen on slow machines.
					if ev.Type == binlogdatapb.VEventType_HEARTBEAT {
						continue
					}
					evs = append(evs, ev)
				}
			case <-ctx.Done():
				t.Fatal("stream ended early")
			case <-timer.C:
				t.Fatalf("timed out waiting for events: %v", wantset)
			}
			if len(evs) != 0 {
				break
			}
		}
		if len(wantset) != len(evs) {
			t.Fatalf("%v: evs\n%v, want\n%v", input, evs, wantset)
		}
		for i, want := range wantset {
			// CurrentTime is not testable.
			evs[i].CurrentTime = 0
			switch want {
			case "begin":
				if evs[i].Type != binlogdatapb.VEventType_BEGIN {
					t.Fatalf("%v (%d): event: %v, want gtid or begin", input, i, evs[i])
				}
			case "gtid":
				if evs[i].Type != binlogdatapb.VEventType_GTID {
					t.Fatalf("%v (%d): event: %v, want gtid", input, i, evs[i])
				}
			case "commit":
				if evs[i].Type != binlogdatapb.VEventType_COMMIT {
					t.Fatalf("%v (%d): event: %v, want commit", input, i, evs[i])
				}
			default:
				evs[i].Timestamp = 0
				if got := fmt.Sprintf("%v", evs[i]); got != want {
					t.Fatalf("%v (%d): event:\n%q, want\n%q", input, i, got, want)
				}
			}
		}
	}
}

func startStream(ctx context.Context, t *testing.T, filter *binlogdatapb.Filter, position string) <-chan []*binlogdatapb.VEvent {
	if position == "" {
		position = masterPosition(t)
	}

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		defer close(ch)
		_ = vstream(ctx, t, position, filter, ch)
	}()
	return ch
}

func vstream(ctx context.Context, t *testing.T, pos string, filter *binlogdatapb.Filter, ch chan []*binlogdatapb.VEvent) error {
	if filter == nil {
		filter = &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "/.*/",
			}},
		}
	}
	return engine.Stream(ctx, pos, filter, func(evs []*binlogdatapb.VEvent) error {
		t.Logf("Received events: %v", evs)
		select {
		case ch <- evs:
		case <-ctx.Done():
			return fmt.Errorf("stream ended early")
		}
		return nil
	})
}

func execStatement(t *testing.T, query string) {
	t.Helper()
	if err := env.Mysqld.ExecuteSuperQuery(context.Background(), query); err != nil {
		t.Fatal(err)
	}
}

func execStatements(t *testing.T, queries []string) {
	t.Helper()
	if err := env.Mysqld.ExecuteSuperQueryList(context.Background(), queries); err != nil {
		t.Fatal(err)
	}
}

func masterPosition(t *testing.T) string {
	t.Helper()
	// We use the engine's cp because there is one test that overrides
	// the flavor to FilePos. If so, we have to obtain the position
	// in that flavor format.
	connParam, err := engine.env.DBConfigs().DbaWithDB().MysqlParams()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := mysql.Connect(context.Background(), connParam)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pos, err := conn.MasterPosition()
	if err != nil {
		t.Fatal(err)
	}
	return mysql.EncodePosition(pos)
}

func setVSchema(t *testing.T, vschema string) {
	t.Helper()

	curCount := vschemaUpdates.Get()

	if err := env.SetVSchema(vschema); err != nil {
		t.Fatal(err)
	}

	// Wait for curCount to go up.
	updated := false
	for i := 0; i < 10; i++ {
		if vschemaUpdates.Get() != curCount {
			updated = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !updated {
		t.Error("vschema did not get updated")
	}
}
