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
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with master tablets
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown(t)

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, ks1, "-", smVSchema, smSchema, defaultReplicas, defaultRdonly, 100)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", ks1, "0"), 1)

	vc.AddKeyspace(t, []*Cell{defaultCell}, ks2, "-", smVSchema, smSchema, defaultReplicas, defaultRdonly, 200)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", ks2, "0"), 1)

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
