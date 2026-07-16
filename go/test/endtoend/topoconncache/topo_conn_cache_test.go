/*
Copyright 2022 The Vitess Authors.

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

package topoconncache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/test/vitesst"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

/*
1. In steady state check the numbers of tablets through 'ListAllTablets'.
2. Now delete 'zone2' and 'ListAllTablets' should return only primary tablets
3. Add the cell back with same name but at different location in topo.
4. 'ListAllTablets' should return all the new tablets.
*/
func TestVtctldListAllTablets(t *testing.T) {
	setupCluster(t)
	testURL(t, "/api/keyspaces/", "keyspace url")
	testURL(t, "/debug/health", "vtctld health check url")

	testListAllTablets(t)
	deleteCell(t)
	addCellback(t)
}

func testListAllTablets(t *testing.T) {
	// first w/o any filters, aside from cell
	result, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetTablets")
	require.NoError(t, err)

	tablets := getAllTablets()
	tabletsFromCMD := strings.Split(result, "\n")
	tabletCountFromCMD := 0
	for _, line := range tabletsFromCMD {
		if len(line) > 0 {
			tabletCountFromCMD = tabletCountFromCMD + 1
			assert.Contains(t, tablets, strings.Split(line, " ")[0])
		}
	}
	assert.Equal(t, len(tablets), tabletCountFromCMD)
}

func deleteCell(t *testing.T) {
	ks := clusterInstance.Keyspace(keyspaceName)
	shard1 := ks.Shard("-80")
	shard2 := ks.Shard("80-")

	// delete all tablets for cell2
	deleteTablet(t, shard1.Replicas()[0])
	deleteTablet(t, shard2.Replicas()[0])
	deleteTablet(t, shard1.RDOnly()[0])
	deleteTablet(t, shard2.RDOnly()[0])

	// Delete cell2 info from topo
	res, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "DeleteCellInfo", "--force", cell2)
	t.Log(res)
	require.NoError(t, err)

	// Now list all tablets
	result, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetTablets")
	require.NoError(t, err)

	tablets := getAllTablets()
	tabletsFromCMD := strings.Split(result, "\n")
	tabletCountFromCMD := 0

	for _, line := range tabletsFromCMD {
		if len(line) > 0 {
			tabletCountFromCMD = tabletCountFromCMD + 1
			assert.Contains(t, tablets, strings.Split(line, " ")[0])
		}
	}
	assert.Equal(t, len(tablets), tabletCountFromCMD)
}

func deleteTablet(t *testing.T, tablet *vitesst.Tablet) {
	alias := tablet.Alias()
	require.NoError(t, tablet.Remove(t.Context()))

	err := clusterInstance.Vtctld().ExecuteCommand(t.Context(), "DeleteTablets", alias)
	require.NoError(t, err)
}

func addCellback(t *testing.T) {
	ctx := t.Context()

	// creating new cell , with same name as previously deleted one but at a different root path.
	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "AddCellInfo",
		"--root", "/org1/obj1/"+cell2,
		"--server-address", cellServerAddress(t, cell1),
		cell2)
	require.NoError(t, err)

	// create new vttablets. They should come up as serving since the primary
	// for each shard already exists.
	for _, shard := range []string{"-80", "80-"} {
		_, err = clusterInstance.AddTablet(ctx, cell2, keyspaceName, shard, "replica")
		require.NoError(t, err)
		_, err = clusterInstance.AddTablet(ctx, cell2, keyspaceName, shard, "rdonly")
		require.NoError(t, err)
	}

	result, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetTablets")
	require.NoError(t, err)

	tablets := getAllTablets()
	tabletsFromCMD := strings.Split(result, "\n")
	tabletCountFromCMD := 0
	for _, line := range tabletsFromCMD {
		if len(line) > 0 {
			tabletCountFromCMD = tabletCountFromCMD + 1
			assert.Contains(t, tablets, strings.Split(line, " ")[0])
		}
	}
	assert.Equal(t, len(tablets), tabletCountFromCMD)
}

// cellServerAddress returns the topology server address recorded for a cell,
// reused when re-adding a cell at a different root path.
func cellServerAddress(t *testing.T, cell string) string {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetCellInfo", cell)
	require.NoError(t, err)

	cellInfo := &topodatapb.CellInfo{}
	require.NoError(t, protojson.Unmarshal([]byte(out), cellInfo))
	return cellInfo.ServerAddress
}

func getAllTablets() []string {
	tablets := make([]string, 0)
	for _, ks := range clusterInstance.Keyspaces() {
		for _, shard := range ks.Shards() {
			for _, tablet := range shard.Tablets() {
				tablets = append(tablets, topoproto.TabletAliasString(&topodatapb.TabletAlias{
					Cell: tablet.Cell,
					Uid:  uint32(tablet.UID),
				}))
			}
		}
	}
	return tablets
}
