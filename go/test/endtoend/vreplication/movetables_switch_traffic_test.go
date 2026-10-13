/*
Copyright 2026 The Vitess Authors.

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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// SwitchTraffic must not switch reads to a target whose workflow never ran,
// and must not leave them there when it fails to switch writes.
func TestMoveTablesSwitchTrafficOnWorkflowThatNeverRan(t *testing.T) {
	defer setupMinimalMoveTables(t)()
	_, err := moveTables(t, "create", "--source-keyspace", defaultSourceKs, "--tables", "loadtest", "--auto-start=false")
	require.NoError(t, err)
	out, err := moveTables(t, "SwitchTraffic", "--timeout", "10s")
	require.Error(t, err)
	require.Contains(t, out, "workflow is stopped")

	out, err = vc.VtctldClient.ExecuteCommandWithOutput("GetRoutingRules")
	require.NoError(t, err)
	var rr struct {
		Rules []struct {
			FromTable string   `json:"from_table"`
			ToTables  []string `json:"to_tables"`
		} `json:"rules"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &rr))
	checked := 0
	for _, r := range rr.Rules {
		if r.FromTable == "loadtest@replica" || r.FromTable == "loadtest@rdonly" {
			require.Equal(t, []string{defaultSourceKs + ".loadtest"}, r.ToTables, "%s reads were switched to the target, which has no data", r.FromTable)
			checked++
		}
	}
	require.Equal(t, 2, checked, "expected routing rules for loadtest@replica and loadtest@rdonly")
}

// setupMinimalMoveTables starts the minimal cluster, with no replicas, and
// returns its teardown.
func setupMinimalMoveTables(t *testing.T) func() {
	ogReplicas, ogRdOnly := defaultReplicas, defaultRdonly
	defaultReplicas, defaultRdonly = 0, 0
	vc = setupMinimalCluster(t)
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	setupMinimalTargetKeyspace(t)
	return func() {
		vc.TearDown()
		defaultReplicas, defaultRdonly = ogReplicas, ogRdOnly
	}
}

// moveTables runs a MoveTables subcommand on the default workflow.
func moveTables(t *testing.T, args ...string) (string, error) {
	cmd := []string{"MoveTables", "--workflow", defaultWorkflowName, "--target-keyspace", defaultTargetKs}
	out, err := vc.VtctldClient.ExecuteCommandWithOutput(append(cmd, args...)...)
	t.Logf("MoveTables %v: err=%v output=%s", args, err, out)
	return out, err
}
