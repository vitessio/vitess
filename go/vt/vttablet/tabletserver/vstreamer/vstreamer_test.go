/*
Copyright 2018 The Vitess Authors.

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
	"strings"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type testcase struct {
	input  interface{}
	output [][]string
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
		// MySQL issues GTID->BEGIN.
		// MariaDB issues BEGIN->GTID.
		output: [][]string{{
			`gtid|begin`,
			`gtid|begin`,
			`type:FIELD field_event:<table_name:"stream1" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"stream1" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"stream1" row_changes:<before:<lengths:1 lengths:3 values:"1aaa" > after:<lengths:1 lengths:3 values:"1bbb" > > > `,
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
			`gtid|begin`,
			`gtid|begin`,
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
			`commit`,
		}},
	}, {
		// truncate is a DDL
		input: "truncate table stream2",
		output: [][]string{{
			`gtid`,
			`type:DDL ddl:"truncate table stream2" `,
		}},
	}, {
		// repair, optimize and analyze show up in binlog stream, but ignored by vitess.
		input: "repair table stream2",
	}, {
		input: "optimize table stream2",
	}, {
		input: "analyze table stream2",
	}, {
		// select, set, show, analyze and describe don't get logged.
		input: "select * from stream1",
	}, {
		input: "set @val=1",
	}, {
		input: "show tables",
	}, {
		input: "analyze table stream1",
	}, {
		input: "describe stream1",
	}}
	runCases(t, nil, testcases)
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
			`gtid|begin`,
			`gtid|begin`,
			`type:FIELD field_event:<table_name:"yes_stream" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"yes_stream" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`type:ROW row_event:<table_name:"yes_stream" row_changes:<before:<lengths:1 lengths:3 values:"1aaa" > after:<lengths:1 lengths:3 values:"1bbb" > > > `,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases)
}

func TestREKeyrange(t *testing.T) {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "/.*/",
			Filter: "-80",
		}},
	}
	ch := startStream(ctx, t, filter)

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	defer env.SetVSchema("{}")

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
		`gtid|begin`,
		`gtid|begin`,
		`type:FIELD field_event:<table_name:"t1" fields:<name:"id1" type:INT32 > fields:<name:"id2" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 values:"14aaa" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:1 lengths:3 values:"14aaa" > after:<lengths:1 lengths:1 lengths:3 values:"24aaa" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<before:<lengths:1 lengths:1 lengths:3 values:"24aaa" > > > `,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 values:"31bbb" > > > `,
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
	if err := env.SetVSchema(altVSchema); err != nil {
		t.Fatal(err)
	}

	// Only the first insert should be sent.
	input = []string{
		"begin",
		"insert into t1 values (4, 1, 'aaa')",
		"insert into t1 values (1, 4, 'aaa')",
		"commit",
	}
	execStatements(t, input)
	expectLog(ctx, t, input, ch, [][]string{{
		`gtid|begin`,
		`gtid|begin`,
		`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:1 lengths:3 values:"41aaa" > > > `,
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
		// MySQL issues GTID->BEGIN.
		// MariaDB issues BEGIN->GTID.
		output: [][]string{{
			`gtid|begin`,
			`gtid|begin`,
			`type:FIELD field_event:<table_name:"t1" fields:<name:"id2" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"t1" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases)
}

func TestSelectExpressions(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table expr_test(id int, val bigint, primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table expr_test",
	})
	engine.se.Reload(context.Background())

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "expr_test",
			Filter: "select id, val, month(val), day(val), hour(val) from expr_test",
		}},
	}

	testcases := []testcase{{
		input: []string{
			"begin",
			"insert into expr_test values (1, 1546392881)",
			"commit",
		},
		// MySQL issues GTID->BEGIN.
		// MariaDB issues BEGIN->GTID.
		output: [][]string{{
			`gtid|begin`,
			`gtid|begin`,
			`type:FIELD field_event:<table_name:"expr_test" ` +
				`fields:<name:"id" type:INT32 > ` +
				`fields:<name:"val" type:INT64 > ` +
				`fields:<name:"month(val)" type:VARBINARY > ` +
				`fields:<name:"day(val)" type:VARBINARY > ` +
				`fields:<name:"hour(val)" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"expr_test" row_changes:<after:<lengths:1 lengths:10 lengths:6 lengths:8 lengths:10 values:"` +
				`1` +
				`1546392881` +
				`201901` +
				`20190102` +
				`2019010201` +
				`" > > > `,
			`commit`,
		}},
	}}
	runCases(t, filter, testcases)
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
			t.Fatal(err)
		}
	}()
	expectLog(ctx, t, "ddls", ch, [][]string{{
		// Current schema has 3 columns, but they'll be truncated to match the two columns in the event.
		`gtid|begin`,
		`gtid|begin`,
		`type:FIELD field_event:<table_name:"ddl_test1" fields:<name:"id" type:INT32 > fields:<name:"val1" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"ddl_test1" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
		`type:FIELD field_event:<table_name:"ddl_test2" fields:<name:"id" type:INT32 > fields:<name:"val1" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"ddl_test2" row_changes:<after:<lengths:1 lengths:3 values:"1aaa" > > > `,
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
		`gtid|begin`,
		`gtid|begin`,
		`type:FIELD field_event:<table_name:"ddl_test1" fields:<name:"id" type:INT32 > fields:<name:"val1" type:VARBINARY > fields:<name:"val2" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"ddl_test1" row_changes:<after:<lengths:1 lengths:3 lengths:3 values:"2bbbccc" > > > `,
		`type:FIELD field_event:<table_name:"ddl_test2" fields:<name:"id" type:INT32 > fields:<name:"val1" type:VARBINARY > fields:<name:"val2" type:VARBINARY > > `,
		`type:ROW row_event:<table_name:"ddl_test2" row_changes:<after:<lengths:1 lengths:3 lengths:3 values:"2bbbccc" > > > `,
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
			`gtid|begin`,
			`gtid|begin`,
			`commit`,
		}},
	}}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/none/",
		}},
	}
	runCases(t, filter, testcases)
}

func TestBuffering(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	savedSize := *packetSize
	*packetSize = 10
	defer func() { *packetSize = savedSize }()

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
			`gtid|begin`,
			`gtid|begin`,
			`type:FIELD field_event:<table_name:"packet_test" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:3 values:"1123" > > > `,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:3 values:"2456" > > > `,
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
			`gtid|begin`,
			`gtid|begin`,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:6 values:"3123456" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:6 values:"4789012" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<before:<lengths:1 lengths:6 values:"3123456" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<before:<lengths:1 lengths:6 values:"4789012" > > > `,
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
			`gtid|begin`,
			`gtid|begin`,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:6 values:"5123456" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:11 values:"612345678901" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:5 values:"723456" > > > `,
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
			`gtid|begin`,
			`gtid|begin`,
			`type:ROW row_event:<table_name:"packet_test" row_changes:<after:<lengths:1 lengths:3 values:"8123" > > > `,
		}, {
			`type:ROW row_event:<table_name:"packet_test" row_changes:<before:<lengths:1 lengths:3 values:"8123" > after:<lengths:1 lengths:3 values:"8456" > > > `,
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
	runCases(t, nil, testcases)
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
			`gtid|begin`,
			`gtid|begin`,
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
			`commit`,
		}},
	}, {
		input: []string{
			"insert into vitess_fracts values(1, 1.99, 2.99, 3.99, 4.99)",
		},
		output: [][]string{{
			`gtid|begin`,
			`gtid|begin`,
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
			`commit`,
		}},
	}, {
		// TODO(sougou): validate that binary and char data generate correct DMLs on the other end.
		input: []string{
			"insert into vitess_strings values('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'a', 'a,b')",
		},
		output: [][]string{{
			`gtid|begin`,
			`gtid|begin`,
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
			`commit`,
		}},
	}, {
		// TODO(sougou): validate that the geometry value generates the correct DMLs on the other end.
		input: []string{
			"insert into vitess_misc values(1, '\x01', '2012-01-01', '2012-01-01 15:45:45', '15:45:45', point(1, 2))",
		},
		output: [][]string{{
			`gtid|begin`,
			`gtid|begin`,
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
			`commit`,
		}},
	}, {
		input: []string{
			"insert into vitess_null values(1, null)",
		},
		output: [][]string{{
			`gtid|begin`,
			`gtid|begin`,
			`type:FIELD field_event:<table_name:"vitess_null" fields:<name:"id" type:INT32 > fields:<name:"val" type:VARBINARY > > `,
			`type:ROW row_event:<table_name:"vitess_null" row_changes:<after:<lengths:1 lengths:-1 values:"1" > > > `,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases)
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
			`gtid|begin`,
			`gtid|begin`,
			`type:FIELD field_event:<table_name:"vitess_json" fields:<name:"id" type:INT32 > fields:<name:"val" type:JSON > > `,
			`type:ROW row_event:<table_name:"vitess_json" row_changes:<after:<lengths:1 lengths:24 values:"1JSON_OBJECT('foo','bar')" > > > `,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases)
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
			`gtid|begin`,
			`gtid|begin`,
			`commit`,
		}},
	}}
	runCases(t, nil, testcases)
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
		"set @@session.binlog_format='statement'",
		"update t1 set val1='bbb' where id=1",
		"set @@session.binlog_format='row'",
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
	want := "unexpected statement type"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain '%s'", err, want)
	}
}

func runCases(t *testing.T, filter *binlogdatapb.Filter, testcases []testcase) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := startStream(ctx, t, filter)

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

	for _, wantset := range output {
		var evs []*binlogdatapb.VEvent
		var ok bool
		select {
		case evs, ok = <-ch:
			if !ok {
				t.Fatal("stream ended early")
			}
		case <-ctx.Done():
			t.Fatal("stream ended early")
		}
		if len(wantset) != len(evs) {
			t.Fatalf("%v: evs\n%v, want\n%v", input, evs, wantset)
		}
		for i, want := range wantset {
			// CurrentTime is not testable.
			evs[i].CurrentTime = 0
			switch want {
			case "gtid|begin":
				if evs[i].Type != binlogdatapb.VEventType_GTID && evs[i].Type != binlogdatapb.VEventType_BEGIN {
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
				if evs[i].Timestamp == 0 {
					t.Fatalf("evs[%d].Timestamp: 0, want non-zero", i)
				}
				evs[i].Timestamp = 0
				if got := fmt.Sprintf("%v", evs[i]); got != want {
					t.Fatalf("%v (%d): event:\n%q, want\n%q", input, i, got, want)
				}
			}
		}
	}
}

func startStream(ctx context.Context, t *testing.T, filter *binlogdatapb.Filter) <-chan []*binlogdatapb.VEvent {
	pos := masterPosition(t)

	ch := make(chan []*binlogdatapb.VEvent)
	go func() {
		defer close(ch)
		if err := vstream(ctx, t, pos, filter, ch); err != nil {
			t.Error(err)
		}
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
	pos, err := env.Mysqld.MasterPosition()
	if err != nil {
		t.Fatal(err)
	}
	return mysql.EncodePosition(pos)
}
