/*
Copyright 2021 The Vitess Authors.

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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const smSchema = `
	CREATE TABLE tx (
	id bigint NOT NULL,
	val varbinary(10) NOT NULL,
	ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	typ tinyint NOT NULL,
	PRIMARY KEY (id),
	KEY ts (ts),
	KEY typ (typ)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

const smVSchema = `
{
  "sharded": true,
  "tables": {
      "tx": {
          "column_vindexes": [
              {
                  "column": "id",
                  "name": "hash"
              }
          ]
      }
  },
  "vindexes": {
      "hash": {
          "type": "hash"
      }
  }
}
`

const smMaterializeSpec = `{"workflow": "wf1", "source_keyspace": "ks1", "target_keyspace": "ks2", "table_settings": [ {"target_table": "tx", "source_expression": "select * from tx where typ>=2 and val > 'abc'"  }] }`

const initDataQuery = `insert into ks1.tx(id, typ, val) values (1, 1, 'abc'), (2, 1, 'def'), (3, 2, 'def'), (4, 2, 'abc'), (5, 3, 'def'), (6, 3, 'abc')`

// testShardedMaterialize tests a materialize workflow for a sharded cluster (single shard) using comparison filters
func testShardedMaterialize(t *testing.T) {
	var err error
	vc = NewVitessCluster(t, nil)
	ks1 := "ks1"
	ks2 := "ks2"
	require.NotNil(t, vc)
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with primary tablets
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown()
	defaultCell := vc.Cells[vc.CellNames[0]]
	vc.AddKeyspace(t, []*Cell{defaultCell}, ks1, "0", smVSchema, smSchema, defaultReplicas, defaultRdonly, 100, nil)

	vc.AddKeyspace(t, []*Cell{defaultCell}, ks2, "0", smVSchema, smSchema, defaultReplicas, defaultRdonly, 200, nil)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	_, err = vtgateConn.ExecuteFetch(initDataQuery, 0, false)
	require.NoError(t, err)
	materialize(t, smMaterializeSpec)
	tab := vc.getPrimaryTablet(t, ks2, "0")
	catchup(t, tab, "wf1", "Materialize")

	waitForRowCount(t, vtgateConn, ks2, "tx", 2)
	waitForQueryResult(t, vtgateConn, "ks2:0", "select id, val from tx",
		`[[INT64(3) VARBINARY("def")] [INT64(5) VARBINARY("def")]]`)
}

/*
 * The following sections are related to testMaterialize, which is intended to test these edge cases:
 * it tests
 * - the case where the same column is referred to multiple times
 * - use of mysql functions in the filter
 * - use of a custom function in the filter
 */

const smMaterializeSchemaSource = `
	CREATE TABLE mat (
	id bigint NOT NULL,
	val varbinary(10) NOT NULL,
	ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

const smMaterializeVSchemaSource = `
{
  "tables": {
      "mat": {
          "column_vindexes": [
              {
                  "column": "id",
                  "name": "hash"
              }
          ]
      }
  },
  "vindexes": {
      "hash": {
          "type": "hash"
      }
  }
}
`
const smMaterializeSchemaTarget = `
	CREATE TABLE mat2 (
	id bigint NOT NULL,
	val varbinary(10) NOT NULL,
	ts timestamp NOT NULL,
	day int NOT NULL,
	month int NOT NULL,
    x int not null,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

const smMaterializeVSchemaTarget = `
{
  "sharded": true,
  "tables": {
      "mat2": {
          "column_vindexes": [
              {
                  "column": "id",
                  "name": "hash"
              }
          ]
      }
  },
  "vindexes": {
      "hash": {
          "type": "hash"
      }
  }
}
`
const smMaterializeSpec2 = `{"workflow": "wf1", "source_keyspace": "source", "target_keyspace": "target", "table_settings": [ {"target_table": "mat2", "source_expression": "select id, val, ts, dayofmonth(ts) as day, month(ts) as month, custom1(id, val) as x from mat"  }] }`

const materializeInitDataQuery = `insert into mat(id, val, ts) values (1, 'abc', '2021-10-9 16:17:36'), (2, 'def', '2021-11-10 16:17:36')`

const customFunc = `
CREATE FUNCTION custom1(id int, val varbinary(10))
RETURNS int
DETERMINISTIC
RETURN id * length(val);
`

func testMaterialize(t *testing.T) {
	var err error
	vc = NewVitessCluster(t, nil)
	sourceKs := "source"
	targetKs := "target"
	shard := "0"
	require.NotNil(t, vc)
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with primary tablets
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown()

	defaultCell := vc.Cells[vc.CellNames[0]]
	vc.AddKeyspace(t, []*Cell{defaultCell}, sourceKs, "0", smMaterializeVSchemaSource, smMaterializeSchemaSource, defaultReplicas, defaultRdonly, 300, nil)

	vc.AddKeyspace(t, []*Cell{defaultCell}, targetKs, "0", smMaterializeVSchemaTarget, smMaterializeSchemaTarget, defaultReplicas, defaultRdonly, 400, nil)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	_, err = vtgateConn.ExecuteFetch(materializeInitDataQuery, 0, false)
	require.NoError(t, err)

	ks2Primary := vc.getPrimaryTablet(t, targetKs, shard)
	_, err = ks2Primary.QueryTablet(customFunc, targetKs, true)
	require.NoError(t, err)

	testMaterializeWithNonExistentTable(t)

	materialize(t, smMaterializeSpec2)
	catchup(t, ks2Primary, "wf1", "Materialize")

	// validate data after the copy phase
	waitForRowCount(t, vtgateConn, targetKs, "mat2", 2)
	want := `[[INT64(1) VARBINARY("abc") TIMESTAMP("2021-10-09 16:17:36") INT32(9) INT32(10) INT32(3)] [INT64(2) VARBINARY("def") TIMESTAMP("2021-11-10 16:17:36") INT32(10) INT32(11) INT32(6)]]`
	waitForQueryResult(t, vtgateConn, targetKs, "select id, val, ts, day, month, x from mat2", want)

	// insert data to test the replication phase
	execVtgateQuery(t, vtgateConn, sourceKs, "insert into mat(id, val, ts) values (3, 'ghi', '2021-12-11 16:17:36')")

	// validate data after the replication phase
	waitForQueryResult(t, vtgateConn, targetKs, "select count(*) from mat2", "[[INT64(3)]]")
	want = `[[INT64(1) VARBINARY("abc") TIMESTAMP("2021-10-09 16:17:36") INT32(9) INT32(10) INT32(3)] [INT64(2) VARBINARY("def") TIMESTAMP("2021-11-10 16:17:36") INT32(10) INT32(11) INT32(6)] [INT64(3) VARBINARY("ghi") TIMESTAMP("2021-12-11 16:17:36") INT32(11) INT32(12) INT32(9)]]`
	waitForQueryResult(t, vtgateConn, targetKs, "select id, val, ts, day, month, x from mat2", want)
}

// TestMaterialize runs all the individual materialize tests defined above.
func TestMaterialize(t *testing.T) {
	t.Run("Materialize", func(t *testing.T) {
		testMaterialize(t)
	})
	t.Run("ShardedMaterialize", func(t *testing.T) {
		testShardedMaterialize(t)
	})
}

const (
	refSchema = `
  	create table ref1 (
		id bigint not null,
		val varbinary(10) not null,
		primary key (id)
	) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;
	create table ref2 (
		id bigint not null,
		id2 bigint not null,
		primary key (id)
	) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;
	create table ref3 (
		id bigint not null,
		id2 bigint not null,
		primary key (id)
	) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;
	create table ref4 (
		id bigint not null,
		id2 bigint not null,
		primary key (id)
	) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;
`
	refSourceVSchema = `
{
  "tables": {
    "ref1": {
      "type": "reference"
    },
	"ref2": {
      "type": "reference"
	},
	"ref3": {
      "type": "reference"
	},
	"ref4": {
      "type": "reference"
	}
  }
}
`
	refTargetVSchema = `
{
  "tables": {
	"ref1": {
		  "type": "reference",
		  "source": "ks1.ref1"
	},
	"ref2": {
		  "type": "reference",
		  "source": "ks1.ref2"
	},
	"ref3": {
		  "type": "reference",
		  "source": "ks1.ref3"
	},
	"ref4": {
		  "type": "reference",
		  "source": "ks1.ref4"
	}
  }
}
`
	initRef1DataQuery = `insert into ks1.ref1(id, val) values (1, 'abc'), (2, 'def'), (3, 'ghi')`
	initRef2DataQuery = `insert into ks1.ref2(id, id2) values (1, 1), (2, 2), (3, 3)`
	initRef3DataQuery = `insert into ks1.ref3(id, id2) values (1, 1), (2, 2), (3, 3), (4, 4)`
	initRef4DataQuery = `insert into ks1.ref4(id, id2) values (1, 1), (2, 2), (3, 3)`
)

// TestReferenceTableMaterialize tests materializing reference tables.
func TestReferenceTableMaterialize(t *testing.T) {
	vc = NewVitessCluster(t, nil)
	require.NotNil(t, vc)
	shards := []string{"-80", "80-"}
	defer vc.TearDown()
	defaultCell := vc.Cells[vc.CellNames[0]]
	_, err := vc.AddKeyspace(t, []*Cell{defaultCell}, "ks1", "0", refSourceVSchema, refSchema, 0, 0, 100, nil)
	require.NoError(t, err)
	_, err = vc.AddKeyspace(t, []*Cell{defaultCell}, "ks2", strings.Join(shards, ","), refTargetVSchema, "", 0, 0, 200, nil)
	require.NoError(t, err)
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	_, err = vtgateConn.ExecuteFetch(initRef1DataQuery, 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(initRef2DataQuery, 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(initRef3DataQuery, 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(initRef4DataQuery, 0, false)
	require.NoError(t, err)

	err = vc.VtctldClient.ExecuteCommand("Materialize", "--target-keyspace", "ks2", "--workflow", "wf1", "create",
		"--source-keyspace", "ks1", "--reference-tables", "ref1,ref2")
	require.NoError(t, err, "Materialize")
	for _, shard := range shards {
		tab := vc.getPrimaryTablet(t, "ks2", shard)
		catchup(t, tab, "wf1", "Materialize")
	}

	for _, shard := range shards {
		waitForRowCount(t, vtgateConn, "ks2:"+shard, "ref1", 3)
		waitForQueryResult(t, vtgateConn, "ks2:"+shard, "select id, val from ref1",
			`[[INT64(1) VARBINARY("abc")] [INT64(2) VARBINARY("def")] [INT64(3) VARBINARY("ghi")]]`)
		waitForRowCount(t, vtgateConn, "ks2:"+shard, "ref2", 3)
		waitForQueryResult(t, vtgateConn, "ks2:"+shard, "select id, id2 from ref2",
			`[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)]]`)
	}
	vdiff(t, "ks2", "wf1", defaultCellName, nil)

	queries := []string{
		"update ks1.ref1 set val='xyz'",
		"update ks1.ref2 set id2=3 where id=2",
		"delete from ks1.ref1 where id=1",
		"delete from ks1.ref2 where id=1",
		"insert into ks1.ref1(id, val) values (4, 'jkl'), (5, 'mno')",
		"insert into ks1.ref2(id, id2) values (4, 4), (5, 5)",
	}
	for _, query := range queries {
		execVtgateQuery(t, vtgateConn, "ks1", query)
	}
	for _, shard := range shards {
		waitForRowCount(t, vtgateConn, "ks2:"+shard, "ref1", 4)
		waitForRowCount(t, vtgateConn, "ks2:"+shard, "ref2", 4)
	}
	vdiff(t, "ks2", "wf1", defaultCellName, nil)

	// Testing update with --add-reference-tables.
	err = vc.VtctldClient.ExecuteCommand("Materialize", "--target-keyspace", "ks2", "--workflow", "wf1", "update",
		"--add-reference-tables", "ref3,ref4")
	require.NoError(t, err, "MaterializeAddTables")

	for _, shard := range shards {
		tab := vc.getPrimaryTablet(t, "ks2", shard)
		catchup(t, tab, "wf1", "Materialize")
	}

	for _, shard := range shards {
		waitForRowCount(t, vtgateConn, "ks2:"+shard, "ref3", 4)
		waitForQueryResult(t, vtgateConn, "ks2:"+shard, "select id, id2 from ref3",
			`[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(4) INT64(4)]]`)
		waitForRowCount(t, vtgateConn, "ks2:"+shard, "ref4", 3)
		waitForQueryResult(t, vtgateConn, "ks2:"+shard, "select id, id2 from ref4",
			`[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)]]`)
	}
	vdiff(t, "ks2", "wf1", defaultCellName, nil)

	queries = []string{
		"update ks1.ref3 set id2=3 where id=2",
		"update ks1.ref4 set id2=3 where id=2",
		"delete from ks1.ref3 where id2=3",
		"delete from ks1.ref4 where id2=3",
		"insert into ks1.ref3(id, id2) values (3, 3)",
		"insert into ks1.ref4(id, id2) values (3, 3), (4, 4)",
	}
	for _, query := range queries {
		execVtgateQuery(t, vtgateConn, "ks1", query)
	}
	for _, shard := range shards {
		waitForRowCount(t, vtgateConn, "ks2:"+shard, "ref3", 3)
		waitForRowCount(t, vtgateConn, "ks2:"+shard, "ref4", 3)
	}
	vdiff(t, "ks2", "wf1", defaultCellName, nil)
}
