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
	"bytes"
	"fmt"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"

	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

type tgrTestConfig struct {
	ksU1, ksU2, ksS1                   string
	ksU1Tables, ksU2Tables, ksS1Tables []string
}

var grTestConfig tgrTestConfig = tgrTestConfig{
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
		createSQL += "CREATE TABLE " + table + " (id int primary key, val varchar(32)) ENGINE=InnoDB;\n"
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

func (h *grHelpers) isGlobal(t *testing.T, tables []string, expectedVal string) bool {
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

func (h *grHelpers) isNotGlobal(t *testing.T, tables []string) bool {
	vtgateConn, cancel := getVTGateConn()
	defer cancel()
	var err error
	asExpected := true
	for _, table := range tables {
		for _, target := range []string{"", "@primary", "@replica"} {
			_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("use %s", target), 1, false)
			require.NoError(t, err)
			_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("select * from %s", table), 1, false)
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("table %s not found", table)) {
				asExpected = false
			}
		}
	}
	return asExpected
}

func (h *grHelpers) isAmbiguous(t *testing.T, tables []string) bool {
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
	// FIXME: figure out how to ensure vtgate has processed the updated vschema
	time.Sleep(5 * time.Second)
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
	time.Sleep(5 * time.Second)
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
	time.Sleep(5 * time.Second)
	funcs.postKsS1(t)
}
