/*
Copyright 2024 The Vitess Authors.

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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

/*
* Create unsharded keyspace with two tables, t1,t2,t3, empty vschema. Confirm global routing works. Also try @primary, @replica
* Add another unsharded keyspace  with t2,t4,t5. Check what happens
* Add MoveTables into sharded keyspace moving t2, t4 . Check what happens on Create/SwitchRead/SwitchWrites/Complete
* Check global routing for each with an expectation.
* First BEFORE and then AFTEr the logic change
 */

func getSchema(tables []string) string {
	var createSQL string
	for _, table := range tables {
		createSQL += "CREATE TABLE " + table + " (id int primary key, val varchar(32)) ENGINE=InnoDB;\n"
	}
	return createSQL
}

func insertData(t *testing.T, keyspace string, table string, id int, val string) {
	vtgateConn, cancel := getVTGateConn()
	defer cancel()
	_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("insert into %s.%s(id, val) values(%d, '%s')", keyspace, table, id, val), 1, false)
	require.NoError(t, err)
}

var ksS1VSchema = `
{
  "sharded": true,
  "vindexes": {
    "reverse_bits": {
      "type": "reverse_bits"
    }
  },
  "tables": {
    "t2": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    },
 	"t4": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    }
  }
}
`

func isGlobal(t *testing.T, tables []string, expectedVal string) bool {
	vtgateConn, cancel := getVTGateConn()
	defer cancel()
	var err error
	asExpected := true
	for _, table := range tables {
		for _, target := range []string{"", "@primary", "@replica"} {
			_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("use %s", target), 1, false)
			require.NoError(t, err)
			rs, err := vtgateConn.ExecuteFetch(fmt.Sprintf("select * from %s", table), 1, false)
			require.NoError(t, err)
			gotVal := rs.Rows[0][1].ToString()
			if gotVal != expectedVal {
				asExpected = false
			}
		}
	}
	return asExpected
}

func isNotGlobal(t *testing.T, tables []string) bool {
	vtgateConn, cancel := getVTGateConn()
	defer cancel()
	var err error
	asExpected := true
	for _, table := range tables {
		for _, target := range []string{"", "@primary", "@replica"} {
			_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("use %s", target), 1, false)
			require.NoError(t, err)
			_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("select * from %s", table), 1, false)
			log.Infof("Got error %v, for table %s.%s", err, table, target)
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("table %s not found", table)) {
				asExpected = false
			}
		}
	}
	return asExpected
}

func isAmbiguous(t *testing.T, tables []string) bool {
	vtgateConn, cancel := getVTGateConn()
	defer cancel()
	var err error
	asExpected := true
	for _, table := range tables {
		for _, target := range []string{"", "@primary", "@replica"} {
			_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("use %s", target), 1, false)
			require.NoError(t, err)
			_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("select * from %s", table), 1, false)
			if err == nil || !strings.Contains(err.Error(), "ambiguous") {
				asExpected = false
			}
		}
	}
	return asExpected
}

type tGlobalRoutingTestConfig struct {
	ksU1, ksU2, ksS1                   string
	ksU1Tables, ksU2Tables, ksS1Tables []string
}

var globalRoutingTestConfig tGlobalRoutingTestConfig = tGlobalRoutingTestConfig{
	ksU1:       "unsharded1",
	ksU2:       "unsharded2",
	ksS1:       "sharded1",
	ksU1Tables: []string{"t1", "t2", "t3"},
	ksU2Tables: []string{"t2", "t4", "t5"},
	ksS1Tables: []string{"t2", "t4"},
}

type tGlobalRoutingTestExpectationFuncs struct {
	postKsU1, postKsU2, postKsS1 func(t *testing.T)
}

type globalRoutingTestCase struct {
	markAsGlobal         bool
	unshardedHaveVSchema bool
}

func setExpectations(t *testing.T) *map[globalRoutingTestCase]*tGlobalRoutingTestExpectationFuncs {
	var exp = make(map[globalRoutingTestCase]*tGlobalRoutingTestExpectationFuncs)
	exp[globalRoutingTestCase{unshardedHaveVSchema: false, markAsGlobal: false}] = &tGlobalRoutingTestExpectationFuncs{
		postKsU1: func(t *testing.T) {
			require.True(t, isGlobal(t, []string{"t1", "t2", "t3"}, globalRoutingTestConfig.ksU1))
		},
		postKsU2: func(t *testing.T) {
			require.True(t, isNotGlobal(t, []string{"t1", "t2", "t3"}))
			require.True(t, isNotGlobal(t, []string{"t4", "t5"}))
		},
		postKsS1: func(t *testing.T) {
			require.True(t, isGlobal(t, []string{"t2", "t4"}, globalRoutingTestConfig.ksS1))
			require.True(t, isNotGlobal(t, []string{"t1", "t3"}))
			require.True(t, isNotGlobal(t, []string{"t5"}))
		},
	}
	exp[globalRoutingTestCase{unshardedHaveVSchema: false, markAsGlobal: true}] = &tGlobalRoutingTestExpectationFuncs{
		postKsU1: func(t *testing.T) {
			require.True(t, isGlobal(t, []string{"t1", "t2", "t3"}, globalRoutingTestConfig.ksU1))
		},
		postKsU2: func(t *testing.T) {
			require.True(t, isGlobal(t, []string{"t1", "t3"}, globalRoutingTestConfig.ksU1))
			require.True(t, isGlobal(t, []string{"t4", "t5"}, globalRoutingTestConfig.ksU2))
			require.True(t, isAmbiguous(t, []string{"t2"}))
		},
		postKsS1: func(t *testing.T) {
			require.True(t, isGlobal(t, []string{"t2", "t4"}, globalRoutingTestConfig.ksS1))
			require.True(t, isGlobal(t, []string{"t1", "t3"}, globalRoutingTestConfig.ksU1))
			require.True(t, isGlobal(t, []string{"t5"}, globalRoutingTestConfig.ksU2))
		},
	}
	exp[globalRoutingTestCase{unshardedHaveVSchema: true, markAsGlobal: false}] = &tGlobalRoutingTestExpectationFuncs{
		postKsU1: func(t *testing.T) {
			require.True(t, isGlobal(t, []string{"t1", "t2", "t3"}, globalRoutingTestConfig.ksU1))
		},
		postKsU2: func(t *testing.T) {
			require.True(t, isGlobal(t, []string{"t1", "t3"}, globalRoutingTestConfig.ksU1))
			require.True(t, isGlobal(t, []string{"t4", "t5"}, globalRoutingTestConfig.ksU2))
			require.True(t, isAmbiguous(t, []string{"t2"}))
		},
		postKsS1: func(t *testing.T) {
			require.True(t, isAmbiguous(t, []string{"t2", "t4"}))
			require.True(t, isGlobal(t, []string{"t1", "t3"}, globalRoutingTestConfig.ksU1))
			require.True(t, isGlobal(t, []string{"t5"}, globalRoutingTestConfig.ksU2))
		},
	}
	exp[globalRoutingTestCase{unshardedHaveVSchema: true, markAsGlobal: true}] =
		exp[globalRoutingTestCase{unshardedHaveVSchema: true, markAsGlobal: false}]
	return &exp

}

func TestGlobalRouting(t *testing.T) {
	exp := *setExpectations(t)
	testCases := []globalRoutingTestCase{
		{unshardedHaveVSchema: false, markAsGlobal: true},
		{unshardedHaveVSchema: false, markAsGlobal: false},
		{unshardedHaveVSchema: true, markAsGlobal: true},
		{unshardedHaveVSchema: true, markAsGlobal: false},
	}
	for _, tc := range testCases {
		funcs := exp[tc]
		require.NotNil(t, funcs)
		testGlobalRouting(t, tc.markAsGlobal, tc.unshardedHaveVSchema, funcs)
	}
}

func getUnshardedVschema(unshardedHaveVSchema bool, tables []string) string {
	if !unshardedHaveVSchema {
		return ""
	}
	vschema := `{"tables": {`
	for i, table := range tables {
		if i != 0 {
			vschema += `,`
		}
		vschema += fmt.Sprintf(`"%s": {}`, table)
	}
	vschema += `}}`
	return vschema
}

func testGlobalRouting(t *testing.T, markAsGlobal, unshardedHaveVSchema bool, funcs *tGlobalRoutingTestExpectationFuncs) {
	setSidecarDBName("_vt")
	vttablet.InitVReplicationConfigDefaults()
	extraVTGateArgs = append(extraVTGateArgs, fmt.Sprintf("--mark_unique_unsharded_tables_as_global=%t", markAsGlobal))

	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()
	zone1 := vc.Cells["zone1"]
	config := globalRoutingTestConfig
	vc.AddKeyspace(t, []*Cell{zone1}, config.ksU1, "0", getUnshardedVschema(unshardedHaveVSchema, config.ksU1Tables),
		getSchema(config.ksU1Tables), 1, 0, 100, nil)
	verifyClusterHealth(t, vc)
	for _, table := range config.ksU1Tables {
		insertData(t, config.ksU1, table, 1, config.ksU1)
	}
	time.Sleep(5 * time.Second)
	funcs.postKsU1(t)

	vc.AddKeyspace(t, []*Cell{zone1}, config.ksU2, "0", getUnshardedVschema(unshardedHaveVSchema, config.ksU2Tables),
		getSchema(config.ksU2Tables), 1, 0, 200, nil)
	verifyClusterHealth(t, vc)
	for _, table := range config.ksU2Tables {
		insertData(t, config.ksU2, table, 1, config.ksU2)
	}
	time.Sleep(5 * time.Second) // FIXME: wait for the mysql replication to catch up on the replica
	rebuild(t)
	funcs.postKsU2(t)

	vc.AddKeyspace(t, []*Cell{zone1}, config.ksS1, "-80,80-", ksS1VSchema, getSchema(config.ksS1Tables), 1, 0, 300, nil)
	verifyClusterHealth(t, vc)
	for _, table := range config.ksS1Tables {
		insertData(t, config.ksS1, table, 1, config.ksS1)
	}
	time.Sleep(5 * time.Second)
	rebuild(t)
	funcs.postKsS1(t)
}

func rebuild(t *testing.T) {
	err := vc.VtctldClient.ExecuteCommand("RebuildVSchemaGraph")
	require.NoError(t, err)
	err = vc.VtctldClient.ExecuteCommand("RebuildKeyspaceGraph", globalRoutingTestConfig.ksU1, globalRoutingTestConfig.ksU2)
	require.NoError(t, err)
}
