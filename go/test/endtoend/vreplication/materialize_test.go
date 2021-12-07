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
	"fmt"
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

// TestShardedMaterialize tests a materialize from a sharded (single shard) using comparison filters
func TestShardedMaterialize(t *testing.T) {
	defaultCellName := "zone1"
	allCells := []string{"zone1"}
	allCellNames = "zone1"
	vc = NewVitessCluster(t, "TestShardedMaterialize", allCells, mainClusterConfig)
	ks1 := "ks1"
	ks2 := "ks2"
	require.NotNil(t, vc)
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with primary tablets
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown(t)

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, ks1, "-", smVSchema, smSchema, defaultReplicas, defaultRdonly, 100)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", ks1, "0"), 1)

	vc.AddKeyspace(t, []*Cell{defaultCell}, ks2, "-", smVSchema, smSchema, defaultReplicas, defaultRdonly, 200)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", ks2, "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	_, err := vtgateConn.ExecuteFetch(initDataQuery, 0, false)
	require.NoError(t, err)
	materialize(t, smMaterializeSpec)
	tab := vc.Cells[defaultCell.Name].Keyspaces[ks2].Shards["-"].Tablets["zone1-200"].Vttablet
	catchup(t, tab, "wf1", "Materialize")

	validateCount(t, vtgateConn, ks2, "tx", 2)
	validateQuery(t, vtgateConn, "ks2:-", "select id, val from tx",
		`[[INT64(3) VARBINARY("def")] [INT64(5) VARBINARY("def")]]`)
}

/*
 * The following sections are related to TestMaterialize. TestMaterialize is intended to test some edge cases. Currently
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
  "sharded": true,
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
const smMaterializeSpec2 = `{"workflow": "wf1", "source_keyspace": "ks1", "target_keyspace": "ks2", "table_settings": [ {"target_table": "mat2", "source_expression": "select id, val, ts, dayofmonth(ts) as day, month(ts) as month, custom1(id, val) as x from mat"  }] }`

const materializeInitDataQuery = `insert into ks1.mat(id, val, ts) values (1, 'abc', '2021-10-9 16:17:36'), (2, 'def', '2021-11-10 16:17:36')`

const customFunc = `
CREATE FUNCTION custom1(id int, val varbinary(10))
RETURNS int
DETERMINISTIC
RETURN id * length(val);
`

// TestMaterialize: details mentioned above
func TestMaterialize(t *testing.T) {
	defaultCellName := "zone1"
	allCells := []string{"zone1"}
	allCellNames = "zone1"
	vc = NewVitessCluster(t, "TestMaterialize", allCells, mainClusterConfig)
	ks1 := "ks1"
	ks2 := "ks2"
	require.NotNil(t, vc)
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with primary tablets
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown(t)

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, ks1, "-", smMaterializeVSchemaSource, smMaterializeSchemaSource, defaultReplicas, defaultRdonly, 100)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", ks1, "0"), 1)

	vc.AddKeyspace(t, []*Cell{defaultCell}, ks2, "-", smMaterializeVSchemaTarget, smMaterializeSchemaTarget, defaultReplicas, defaultRdonly, 200)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", ks2, "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	_, err := vtgateConn.ExecuteFetch(materializeInitDataQuery, 0, false)
	require.NoError(t, err)

	ks2Keyspace := vc.Cells[defaultCell.Name].Keyspaces["ks2"]
	ks2Primary := ks2Keyspace.Shards["-"].Tablets["zone1-200"].Vttablet
	_, err = ks2Primary.QueryTablet(customFunc, "ks2", true)
	require.NoError(t, err)

	materialize(t, smMaterializeSpec2)
	tab := vc.Cells[defaultCell.Name].Keyspaces[ks2].Shards["-"].Tablets["zone1-200"].Vttablet
	catchup(t, tab, "wf1", "Materialize")

	// validate data after the copy phase
	validateCount(t, vtgateConn, ks2, "mat2", 2)
	want := `[[INT64(1) VARBINARY("abc") TIMESTAMP("2021-10-09 16:17:36") INT32(9) INT32(10) INT32(3)] [INT64(2) VARBINARY("def") TIMESTAMP("2021-11-10 16:17:36") INT32(10) INT32(11) INT32(6)]]`
	validateQuery(t, vtgateConn, "ks2:-", "select id, val, ts, day, month, x from mat2", want)

	// insert data to test the replication phase
	execVtgateQuery(t, vtgateConn, "ks1", "insert into ks1.mat(id, val, ts) values (3, 'ghi', '2021-12-11 16:17:36')")

	// validate data after the replication phase
	waitForQueryToExecute(t, vtgateConn, "ks2", "select count(*) from mat2", "[[INT64(3)]]")
	want = `[[INT64(1) VARBINARY("abc") TIMESTAMP("2021-10-09 16:17:36") INT32(9) INT32(10) INT32(3)] [INT64(2) VARBINARY("def") TIMESTAMP("2021-11-10 16:17:36") INT32(10) INT32(11) INT32(6)] [INT64(3) VARBINARY("ghi") TIMESTAMP("2021-12-11 16:17:36") INT32(11) INT32(12) INT32(9)]]`
	validateQuery(t, vtgateConn, "ks2:-", "select id, val, ts, day, month, x from mat2", want)
}
