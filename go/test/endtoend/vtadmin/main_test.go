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

package vtadmin

import (
	_ "embed"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

const vtadminCluster = "local_test"

var (
	clusterInstance *cluster.LocalProcessCluster
	uks             = "uks"
	tks             = "tks"
	cell            = "test_misc"

	uschemaSQL = `
create table u_a
(
    id bigint,
    a  bigint,
    primary key (id)
) Engine = InnoDB;

create table u_b
(
    id bigint,
    b  varchar(50),
    primary key (id)
) Engine = InnoDB;

`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start Unsharded keyspace
		ukeyspace := &cluster.Keyspace{
			Name:      uks,
			SchemaSQL: uschemaSQL,
			VSchema:   "{}",
		}
		err = clusterInstance.StartUnshardedKeyspace(*ukeyspace, 0, false, clusterInstance.Cell)
		if err != nil {
			return 1
		}

		targetKeyspace := &cluster.Keyspace{
			Name:      tks,
			SchemaSQL: "",
			VSchema:   "{}",
		}
		err = clusterInstance.StartUnshardedKeyspace(*targetKeyspace, 0, false, clusterInstance.Cell)
		if err != nil {
			return 1
		}

		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		clusterInstance.NewVTAdminProcess()
		// Override the default cluster ID to include an underscore to
		// exercise the gRPC name resolver handling of non-RFC3986 schemes.
		clusterInstance.VtadminProcess.ClusterID = vtadminCluster
		err = clusterInstance.VtadminProcess.Setup()
		if err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

// TestVtadminAPIs tests the vtadmin APIs.
func TestVtadminAPIs(t *testing.T) {
	// Test the vtadmin APIs
	t.Run("keyspaces api", func(t *testing.T) {
		resp := clusterInstance.VtadminProcess.MakeAPICallRetry(t, "api/keyspaces")
		require.Contains(t, resp, uks)
		require.Contains(t, resp, tks)
	})

	t.Run("workflows api summary_only", func(t *testing.T) {
		workflowName := "vtadmin_summary"
		mtw := cluster.NewMoveTables(t, clusterInstance, workflowName, tks, uks, "u_a", nil)

		output, err := mtw.Create()
		require.NoError(t, err, output)
		mtw.WaitForVreplCatchup(20 * time.Second)

		var (
			fullResp, summaryResp         string
			fullWorkflow, summaryWorkflow gjson.Result
		)
		require.Eventually(t, func() bool {
			status, resp, err := clusterInstance.VtadminProcess.MakeAPICall("api/workflows")
			if err != nil || status != 200 {
				return false
			}
			fullResp = resp
			fullWorkflow = findWorkflowByName(fullResp, workflowName)

			status, resp, err = clusterInstance.VtadminProcess.MakeAPICall("api/workflows?summary_only=true")
			if err != nil || status != 200 {
				return false
			}
			summaryResp = resp
			summaryWorkflow = findWorkflowByName(summaryResp, workflowName)

			return fullWorkflow.Exists() && summaryWorkflow.Exists()
		}, 20*time.Second, 500*time.Millisecond)

		require.True(t, fullWorkflow.Get("shard_streams").Exists())
		require.Equal(t, workflowName, summaryWorkflow.Get("name").String())
		require.Equal(t, "Reads Not Switched. Writes Not Switched", summaryWorkflow.Get("status.traffic_state").String())
		require.Greater(t, summaryWorkflow.Get("status.total_streams").Int(), int64(0))

		summaryShardStreams := summaryWorkflow.Get("shard_streams")
		require.True(t, !summaryShardStreams.Exists() || summaryShardStreams.Raw == "null" || summaryShardStreams.Raw == "{}")
		require.Less(t, len(summaryResp), len(fullResp))
	})
}

func findWorkflowByName(resp string, workflowName string) gjson.Result {
	workflows := gjson.Get(resp, "result.workflows_by_cluster."+vtadminCluster+".workflows").Array()
	for _, workflow := range workflows {
		result := workflow.Get("workflow")
		if result.Get("name").String() == workflowName {
			return result
		}
	}

	return gjson.Result{}
}
