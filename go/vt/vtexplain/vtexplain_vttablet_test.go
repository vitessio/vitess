package vtexplain

import (
	"encoding/json"
	"testing"

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

	defer Stop()

	err := Init(testVSchema, testSchema, "", opts)

	if err != nil {
		t.Error(err)
	}

	sql := "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id"

	_, err = Run(sql)
	if err != nil {
		t.Error(err)
	}
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

	ddls, err := parseSchema(testSchema, &Options{StrictDDL: false})
	if err != nil {
		t.Fatalf("parseSchema: %v", err)
	}
	{
		tabletEnv, _ := newTabletEnvironment(ddls, defaultTestOpts())
		setGlobalTabletEnv(tabletEnv)
	}
	tablet := newTablet(defaultTestOpts(), &topodatapb.Tablet{
		Keyspace: "test_keyspace",
		Shard:    "-80",
		Alias:    &topodatapb.TabletAlias{},
	})
	se := tablet.tsv.SchemaEngine()
	tables := se.GetSchema()

	t1 := tables["t1"]
	if t1 == nil {
		t.Fatalf("table t1 wasn't parsed properly")
	}

	wantCols := `[{"name":"id","type":778},{"name":"val","type":6165}]`
	got, _ := json.Marshal(t1.Fields)
	if wantCols != string(got) {
		t.Errorf("expected %s got %s", wantCols, string(got))
	}

	if !t1.HasPrimary() || len(t1.PKColumns) != 1 || t1.PKColumns[0] != 0 {
		t.Errorf("expected HasPrimary && t1.PKColumns == [0] got %v", t1.PKColumns)
	}
	pkCol := t1.GetPKColumn(0)
	if pkCol == nil || pkCol.String() != `name:"id" type:UINT64` {
		t.Errorf("expected pkCol[0] == id, got %v", pkCol)
	}

	t2 := tables["t2"]
	if t2 == nil {
		t.Fatalf("table t2 wasn't parsed properly")
	}

	wantCols = `[{"name":"val","type":6163}]`
	got, _ = json.Marshal(t2.Fields)
	if wantCols != string(got) {
		t.Errorf("expected %s got %s", wantCols, string(got))
	}

	if t2.HasPrimary() || len(t2.PKColumns) != 0 {
		t.Errorf("expected !HasPrimary && t2.PKColumns == [] got %v", t2.PKColumns)
	}

	t5 := tables["t5"]
	if t5 == nil {
		t.Fatalf("table t5 wasn't parsed properly")
	}
	got, _ = json.Marshal(t5.Fields)
	if wantCols != string(got) {
		t.Errorf("expected %s got %s", wantCols, string(got))
	}

	if t5.HasPrimary() || len(t5.PKColumns) != 0 {
		t.Errorf("expected !HasPrimary && t5.PKColumns == [] got %v", t5.PKColumns)
	}

	seq := tables["t1_seq"]
	if seq.Type != schema.Sequence {
		t.Errorf("expected t1_seq to be a sequence table but is type %v", seq.Type)
	}
}

func TestErrParseSchema(t *testing.T) {
	testSchema := `
create table t1 like t2;
`
	expected := "check your schema, table[t2] doesn't exist"
	ddl, err := parseSchema(testSchema, &Options{StrictDDL: true})
	if err != nil {
		t.Fatalf("parseSchema: %v", err)
	}

	_, err = newTabletEnvironment(ddl, defaultTestOpts())
	if err.Error() != expected {
		t.Errorf("want: %s, got %s", expected, err.Error())
	}
}
