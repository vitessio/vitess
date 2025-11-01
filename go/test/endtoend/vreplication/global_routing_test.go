/*
Copyright 2025 The Vitess Authors.

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
	"bytes"
	"fmt"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

type tgrTestConfig struct {
	ksU1, ksU2, ksS1                   string
	ksU1Tables, ksU2Tables, ksS1Tables []string
}

var grTestConfig = tgrTestConfig{
	ksU1:       "unsharded1",
	ksU2:       "unsharded2",
	ksS1:       "sharded1",
	ksU1Tables: []string{"t1", "t2", "t3"},
	ksU2Tables: []string{"t2", "t4", "t5"},
	ksS1Tables: []string{"t2", "t4", "t6"},
}

type grTestExpectations struct {
	postKsU1, postKsU2, postKsS1 func(t *testing.T)
}

// Scope helpers to this test file so we don't pollute the global namespace.
type grHelpers struct {
	t *testing.T
}

func (h *grHelpers) getSchema(tables []string) string {
	var createSQL string
	for _, table := range tables {
		createSQL += fmt.Sprintf("CREATE TABLE %s (id int primary key, val varchar(32)) ENGINE=InnoDB;\n", table)
	}
	return createSQL
}

func (h *grHelpers) getShardedVSchema(tables []string) string {
	const vSchemaTmpl = `{
  "sharded": true,
  "vindexes": {
    "reverse_bits": {
      "type": "reverse_bits"
    }
  },
  "tables": {
    {{- range $i, $table := .Tables}}
    {{- if gt $i 0}},{{end}}
    "{{ $table }}": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    }
    {{- end}}
  }
}
`
	type VSchemaData struct {
		Tables []string
	}
	tmpl, err := template.New("vschema").Parse(vSchemaTmpl)
	require.NoError(h.t, err)
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, VSchemaData{tables})
	require.NoError(h.t, err)
	return buf.String()
}

func (h *grHelpers) insertData(t *testing.T, keyspace string, table string, id int, val string) {
	vtgateConn, cancel := getVTGateConn()
	defer cancel()
	_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("insert into %s.%s(id, val) values(%d, '%s')",
		keyspace, table, id, val), 1, false)
	require.NoError(t, err)
}

// There is a race between when a table is created and it is updated in the global table cache in vtgate.
// This function waits for the table to be available in vtgate before proceeding.
func (h *grHelpers) waitForTableAvailability(t *testing.T, vtgateConn *mysql.Conn, table string) {
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	for {
		_, err := vtgateConn.ExecuteFetch("select * from "+table, 1, false)
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("table %s not found", table)) {
			return
		}
		select {
		case <-timer.C:
			require.FailNow(t, "timed out waiting for table availability for %s", table)
		default:
			time.Sleep(defaultTick)
		}
	}
}

// Check for the expected global routing behavior for the given tables. Expected logic is implemented in the callback.
func (h *grHelpers) checkForTable(
	t *testing.T,
	tables []string,
	queryCallback func(rs *sqltypes.Result, err error),
) {
	vtgateConn, cancel := getVTGateConn()
	defer cancel()

	for _, table := range tables {
		for _, target := range []string{"", "@primary"} {
			_, err := vtgateConn.ExecuteFetch("use "+target, 1, false)
			require.NoError(t, err)
			h.waitForTableAvailability(t, vtgateConn, table)
			rs, err := vtgateConn.ExecuteFetch("select * from "+table, 1, false)
			queryCallback(rs, err)
		}
	}
}

func (h *grHelpers) isGlobal(t *testing.T, tables []string, expectedVal string) bool {
	asExpected := true

	h.checkForTable(t, tables, func(rs *sqltypes.Result, err error) {
		require.NoError(t, err)
		gotVal := rs.Rows[0][1].ToString()
		if gotVal != expectedVal {
			asExpected = false
		}
	})

	return asExpected
}

func (h *grHelpers) isAmbiguous(t *testing.T, tables []string) bool {
	asExpected := true

	h.checkForTable(t, tables, func(rs *sqltypes.Result, err error) {
		if err == nil || !strings.Contains(err.Error(), "ambiguous") {
			asExpected = false
		}
	})

	return asExpected
}

// getExpectations returns a map of expectations for global routing tests. The key is a boolean indicating whether
// the unsharded keyspace has a vschema. The value is a struct containing callbacks for verifying the global routing
// behavior after each keyspace is added.
func (h *grHelpers) getExpectations() *map[bool]*grTestExpectations {
	var exp = make(map[bool]*grTestExpectations)
	exp[false] = &grTestExpectations{
		postKsU1: func(t *testing.T) {
			require.True(t, h.isGlobal(t, []string{"t1", "t2", "t3"}, grTestConfig.ksU1))
		},
		postKsU2: func(t *testing.T) {
			require.True(t, h.isGlobal(t, []string{"t1", "t3"}, grTestConfig.ksU1))
			require.True(t, h.isGlobal(t, []string{"t4", "t5"}, grTestConfig.ksU2))
			require.True(t, h.isAmbiguous(t, []string{"t2"}))
		},
		postKsS1: func(t *testing.T) {
			require.True(t, h.isGlobal(t, []string{"t2", "t4"}, grTestConfig.ksS1))
			require.True(t, h.isGlobal(t, []string{"t1", "t3"}, grTestConfig.ksU1))
			require.True(t, h.isGlobal(t, []string{"t5"}, grTestConfig.ksU2))
			require.True(t, h.isGlobal(t, []string{"t6"}, grTestConfig.ksS1))
		},
	}
	exp[true] = &grTestExpectations{
		postKsU1: func(t *testing.T) {
			require.True(t, h.isGlobal(t, []string{"t1", "t2", "t3"}, grTestConfig.ksU1))
		},
		postKsU2: func(t *testing.T) {
			require.True(t, h.isGlobal(t, []string{"t1", "t3"}, grTestConfig.ksU1))
			require.True(t, h.isGlobal(t, []string{"t4", "t5"}, grTestConfig.ksU2))
			require.True(t, h.isAmbiguous(t, []string{"t2"}))
		},
		postKsS1: func(t *testing.T) {
			require.True(t, h.isAmbiguous(t, []string{"t2", "t4"}))
			require.True(t, h.isGlobal(t, []string{"t1", "t3"}, grTestConfig.ksU1))
			require.True(t, h.isGlobal(t, []string{"t5"}, grTestConfig.ksU2))
		},
	}
	return &exp
}

func (h *grHelpers) getUnshardedVschema(unshardedHasVSchema bool, tables []string) string {
	if !unshardedHasVSchema {
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

func (h *grHelpers) rebuildGraphs(t *testing.T, keyspaces []string) {
	var err error
	for _, ks := range keyspaces {
		err = vc.VtctldClient.ExecuteCommand("RebuildKeyspaceGraph", ks)
		require.NoError(t, err)
	}
	require.NoError(t, err)
	err = vc.VtctldClient.ExecuteCommand("RebuildVSchemaGraph")
	require.NoError(t, err)
}

// TestGlobalRouting tests global routing for unsharded and sharded keyspaces by setting up keyspaces
// with different table configurations and verifying that the tables are globally routed
// by querying via vtgate.
func TestGlobalRouting(t *testing.T) {
	h := grHelpers{t}
	exp := *h.getExpectations()
	for unshardedHasVSchema, funcs := range exp {
		require.NotNil(t, funcs)
		testGlobalRouting(t, unshardedHasVSchema, funcs)
	}
}

func testGlobalRouting(t *testing.T, unshardedHasVSchema bool, funcs *grTestExpectations) {
	h := grHelpers{t: t}
	setSidecarDBName("_vt")
	vttablet.InitVReplicationConfigDefaults()

	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()
	zone1 := vc.Cells["zone1"]
	config := grTestConfig
	vc.AddKeyspace(t, []*Cell{zone1}, config.ksU1, "0", h.getUnshardedVschema(unshardedHasVSchema, config.ksU1Tables),
		h.getSchema(config.ksU1Tables), 1, 0, 100, nil)
	verifyClusterHealth(t, vc)
	for _, table := range config.ksU1Tables {
		h.insertData(t, config.ksU1, table, 1, config.ksU1)
		vtgateConn, cancel := getVTGateConn()
		waitForRowCount(t, vtgateConn, config.ksU1+"@replica", table, 1)
		cancel()
	}
	keyspaces := []string{config.ksU1}
	h.rebuildGraphs(t, keyspaces)
	funcs.postKsU1(t)

	vc.AddKeyspace(t, []*Cell{zone1}, config.ksU2, "0", h.getUnshardedVschema(unshardedHasVSchema, config.ksU2Tables),
		h.getSchema(config.ksU2Tables), 1, 0, 200, nil)
	verifyClusterHealth(t, vc)
	for _, table := range config.ksU2Tables {
		h.insertData(t, config.ksU2, table, 1, config.ksU2)
		vtgateConn, cancel := getVTGateConn()
		waitForRowCount(t, vtgateConn, config.ksU2+"@replica", table, 1)
		cancel()
	}
	keyspaces = append(keyspaces, config.ksU2)
	h.rebuildGraphs(t, keyspaces)
	funcs.postKsU2(t)

	vc.AddKeyspace(t, []*Cell{zone1}, config.ksS1, "-80,80-", h.getShardedVSchema(config.ksS1Tables), h.getSchema(config.ksS1Tables),
		1, 0, 300, nil)
	verifyClusterHealth(t, vc)
	for _, table := range config.ksS1Tables {
		h.insertData(t, config.ksS1, table, 1, config.ksS1)
		vtgateConn, cancel := getVTGateConn()
		waitForRowCount(t, vtgateConn, config.ksS1+"@replica", table, 1)
		cancel()
	}
	keyspaces = append(keyspaces, config.ksS1)
	h.rebuildGraphs(t, keyspaces)
	funcs.postKsS1(t)
}
