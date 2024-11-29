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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/test/endtoend/cluster"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

type TestClusterSpec struct {
	keyspaceName string
	vschema      string
	schema       string
}

var lookupClusterSpec = TestClusterSpec{
	keyspaceName: "lookuptest",
	vschema: `
{
  "sharded": true,
  "vindexes": {
    "reverse_bits": {
      "type": "reverse_bits"
    }
  },
  "tables": {
	"t1": {
	      "column_vindexes": [
	        {
	          "column": "c1",
	          "name": "reverse_bits"
	        }
	      ]
	}
  }
}
`,
	schema: `
create table t1(
	c1 int,
	c2 int,
    val varchar(128),
	primary key(c1)
);
`,
}

func setupLookupIndexKeyspace(t *testing.T) map[string]*cluster.VttabletProcess {
	tablets := make(map[string]*cluster.VttabletProcess)
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, lookupClusterSpec.keyspaceName, "-80,80-",
		lookupClusterSpec.vschema, lookupClusterSpec.schema, defaultReplicas, defaultRdonly, 200, nil); err != nil {
		t.Fatal(err)
	}
	defaultCell := vc.Cells[vc.CellNames[0]]
	ks := vc.Cells[defaultCell.Name].Keyspaces[lookupClusterSpec.keyspaceName]
	targetTab1 = ks.Shards["-80"].Tablets["zone1-200"].Vttablet
	targetTab2 = ks.Shards["80-"].Tablets["zone1-300"].Vttablet
	tablets["-80"] = targetTab1
	tablets["80-"] = targetTab2
	return tablets
}

func TestLookupIndex(t *testing.T) {
	setSidecarDBName("_vt")
	origDefaultReplicas := defaultReplicas
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultReplicas = origDefaultReplicas
		defaultRdonly = origDefaultRdonly
	}()
	defaultReplicas = 1
	defaultRdonly = 0
	vc = setupMinimalCluster(t)
	vttablet.InitVReplicationConfigDefaults()

	tabs := setupLookupIndexKeyspace(t)
	_ = tabs

	vtgateConn, cancel := getVTGateConn()
	defer cancel()
	_, err := vtgateConn.ExecuteFetch("insert into t1 (c1, c2, val) values (1, 1, 'val1'), (2, 2, 'val2'), (3, 3, 'val3')", 1000, false)
	require.NoError(t, err)
	rows := 3

	vindexName := "t1_c2_lookup"
	lks := lookupClusterSpec.keyspaceName
	err = vc.VtctldClient.ExecuteCommand("LookupVindex", "--name", vindexName, "--table-keyspace="+lks, "create", "--keyspace="+lks,
		"--type=lookup", "--table-owner=t1", "--table-owner-columns=c2", "--ignore-nulls", "--tablet-types=PRIMARY")
	require.NoError(t, err, "error executing LookupVindex create: %v", err)
	waitForWorkflowState(t, vc, fmt.Sprintf(lks+".%s", vindexName), binlogdatapb.VReplicationWorkflowState_Running.String())
	waitForRowCount(t, vtgateConn, lks, vindexName, rows)
	vschema, err := vc.VtctldClient.ExecuteCommandWithOutput("GetVSchema", lks)
	require.NoError(t, err, "error executing GetVSchema: %v", err)
	vdx := gjson.Get(vschema, fmt.Sprintf("vindexes.%s", vindexName))
	require.NotNil(t, vdx, "lookup vindex %s not found", vindexName)
	require.Equal(t, "true", vdx.Get("params.write_only").String(), "expected write_only parameter to be true")

	_, err = vtgateConn.ExecuteFetch("insert into t1 (c1, c2, val) values (4, 4, 'val4'), (5, 5, 'val5'), (6, 6, 'val6')", 1000, false)
	require.NoError(t, err)
	rows = 6
	waitForRowCount(t, vtgateConn, lks, vindexName, rows)

}
