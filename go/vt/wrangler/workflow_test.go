/*
Copyright 2020 The Vitess Authors.

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

package wrangler

/*
import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/topodata"
)

func getMoveTablesWorkflow(t *testing.T, cells, tabletTypes string) *VReplicationWorkflow {
	mtp := &VReplicationWorkflowParams{
		Workflow:       "wf1",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		Tables:         "customer,corder",
		Cells:          cells,
		TabletTypes:    tabletTypes,
	}
	wf, _ := newWorkflow("wf1", "MoveTables")
	mtwf := &VReplicationWorkflow{
		ctx:    context.Background(),
		wf:     wf,
		wr:     nil,
		params: mtp,
		ts:     nil,
		ws:     nil,
	}
	return mtwf
}

func TestReshardingWorkflowErrorsAndMisc(t *testing.T) {
	mtwf := getMoveTablesWorkflow(t, "cell1,cell2", "replica,rdonly")
	require.False(t, mtwf.Exists())
	mtwf.ws = &workflowState{}
	require.True(t, mtwf.Exists())
	require.Errorf(t, mtwf.Complete(), errWorkflowNotFullySwitched)
	mtwf.ws.WritesSwitched = true
	require.Errorf(t, mtwf.Abort(), errWorkflowPartiallySwitched)

	require.ElementsMatch(t, mtwf.getCellsAsArray(), []string{"cell1", "cell2"})
	require.ElementsMatch(t, mtwf.getTabletTypes(), []topodata.TabletType{topodata.TabletType_REPLICA, topodata.TabletType_RDONLY})
	hasReplica, hasRdonly, hasMaster, err := mtwf.parseTabletTypes()
	require.NoError(t, err)
	require.True(t, hasReplica)
	require.True(t, hasRdonly)
	require.False(t, hasMaster)

	mtwf.params.TabletTypes = "replica,rdonly,master"
	require.ElementsMatch(t, mtwf.getTabletTypes(), []topodata.TabletType{topodata.TabletType_REPLICA, topodata.TabletType_RDONLY, topodata.TabletType_MASTER})

	hasReplica, hasRdonly, hasMaster, err = mtwf.parseTabletTypes()
	require.NoError(t, err)
	require.True(t, hasReplica)
	require.True(t, hasRdonly)
	require.True(t, hasMaster)
}

func TestReshardingWorkflowCurrentState(t *testing.T) {
}
*/
