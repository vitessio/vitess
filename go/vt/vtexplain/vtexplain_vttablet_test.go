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

package vtexplain

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestRun(t *testing.T) {
	testVSchema := `
{
	"test_keyspace": {
		"sharded": false,
		"tables": {
			"t1": {
				"columns": [
					{ "name": "id", "type": "INT64" }
				],
				"column_list_authoritative": true
			},
			"t2": {
				"columns": [
					{ "name": "id", "type": "INT32" }
				],
				"column_list_authoritative": true
			}
		}
	}
}
`

	testSchema := `
create table t1 (
	id bigint unsigned not null
);

create table t2 (
	id int unsigned not null
);
`

	opts := &Options{
		ExecutionMode:   "multi",
		ReplicationMode: "ROW",
		NumShards:       2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, Cell)
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	vte, err := Init(ctx, vtenv.NewTestEnv(), ts, testVSchema, testSchema, "", opts, srvTopoCounts)
	require.NoError(t, err)
	defer vte.Stop()

	// Check if the correct schema query is registered.
	_, found := vte.globalTabletEnv.schemaQueries["SELECT COLUMN_NAME as column_name\n\t\tFROM INFORMATION_SCHEMA.COLUMNS\n\t\tWHERE TABLE_SCHEMA = database() AND TABLE_NAME = 't1'\n\t\tORDER BY ORDINAL_POSITION"]
	assert.True(t, found)

	sql := "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id"

	_, err = vte.Run(sql)
	require.NoError(t, err)
}

func TestParseSchema(t *testing.T) {
	testSchema := `
create table t1 (
	id bigint(20) unsigned not null default 123,
	val varchar default "default",
	primary key (id)
);

create table t2 (
	val text default "default2"
);

create table t3 (
    b bit(1) default B'0'
);

create table t4 like t3;

create table t5 (like t2);

create table t1_seq(
  id int,
  next_id bigint,
  cache bigint,
  primary key(id)
) comment 'vitess_sequence';

create table test_partitioned (
	id bigint,
	date_create int,
	primary key(id)
) Engine=InnoDB	/*!50100 PARTITION BY RANGE (date_create)
	(PARTITION p2018_06_14 VALUES LESS THAN (1528959600) ENGINE = InnoDB,
	PARTITION p2018_06_15 VALUES LESS THAN (1529046000) ENGINE = InnoDB,
	PARTITION p2018_06_16 VALUES LESS THAN (1529132400) ENGINE = InnoDB,
	PARTITION p2018_06_17 VALUES LESS THAN (1529218800) ENGINE = InnoDB)*/;
`
	env := vtenv.NewTestEnv()
	ddls, err := parseSchema(testSchema, &Options{StrictDDL: false}, env.Parser())
	if err != nil {
		t.Fatalf("parseSchema: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts := memorytopo.NewServer(ctx, Cell)
	vte := initTest(ctx, ts, ModeMulti, defaultTestOpts(), &testopts{}, t)
	defer vte.Stop()

	tabletEnv, _ := newTabletEnvironment(ddls, defaultTestOpts(), env.CollationEnv())
	vte.setGlobalTabletEnv(tabletEnv)
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	tablet := vte.newTablet(ctx, env, defaultTestOpts(), &topodatapb.Tablet{
		Keyspace: "ks_sharded",
		Shard:    "-80",
		Alias: &topodatapb.TabletAlias{
			Cell: Cell,
		},
	}, ts, srvTopoCounts)

	time.Sleep(10 * time.Millisecond)
	se := tablet.tsv.SchemaEngine()
	tables := se.GetSchema()

	t1 := tables["t1"]
	require.NotNil(t, t1, "table t1 wasn't parsed properly")

	wantCols := `[{"name":"id","type":778,"charset":33,"flags":32800},{"name":"val","type":6165,"charset":33}]`
	got, _ := json.Marshal(t1.Fields)
	assert.Equal(t, wantCols, string(got))

	if !t1.HasPrimary() || len(t1.PKColumns) != 1 || t1.PKColumns[0] != 0 {
		t.Errorf("expected HasPrimary && t1.PKColumns == [0] got %v", t1.PKColumns)
	}
	pkCol := t1.GetPKColumn(0)
	if pkCol == nil || pkCol.String() != `name:"id" type:UINT64 charset:33 flags:32800` {
		t.Errorf("expected pkCol[0] == id, got %v", pkCol)
	}

	t2 := tables["t2"]
	require.NotNil(t, t2, "table t2 wasn't parsed properly")

	wantCols = `[{"name":"val","type":6163,"charset":33}]`
	got, _ = json.Marshal(t2.Fields)
	assert.Equal(t, wantCols, string(got))

	if t2.HasPrimary() || len(t2.PKColumns) != 0 {
		t.Errorf("expected !HasPrimary && t2.PKColumns == [] got %v", t2.PKColumns)
	}

	t5 := tables["t5"]
	require.NotNil(t, t5, "table t5 wasn't parsed properly")
	got, _ = json.Marshal(t5.Fields)
	assert.Equal(t, wantCols, string(got))

	if t5.HasPrimary() || len(t5.PKColumns) != 0 {
		t.Errorf("expected !HasPrimary && t5.PKColumns == [] got %v", t5.PKColumns)
	}

	seq := tables["t1_seq"]
	require.NotNil(t, seq)
	assert.Equal(t, schema.Sequence, seq.Type)
}

func TestErrParseSchema(t *testing.T) {
	testSchema := `create table t1 like t2`
	ddl, err := parseSchema(testSchema, &Options{StrictDDL: true}, sqlparser.NewTestParser())
	require.NoError(t, err)

	_, err = newTabletEnvironment(ddl, defaultTestOpts(), collations.MySQL8())
	require.Error(t, err, "check your schema, table[t2] doesn't exist")
}
