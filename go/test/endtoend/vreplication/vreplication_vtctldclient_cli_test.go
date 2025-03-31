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
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

// TestVtctldclientCLI tests the vreplication vtctldclient CLI commands, primarily to check that non-standard flags
// are being handled correctly. The other end-to-end tests are expected to test the various common workflows.
func TestVtctldclientCLI(t *testing.T) {
	setSidecarDBName("_vt")
	var err error
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

	err = vc.Vtctl.AddCellInfo("zone2")
	require.NoError(t, err)
	zone2, err := vc.AddCell(t, "zone2")
	require.NoError(t, err)
	require.NotNil(t, zone2)
	defer vc.TearDown()

	sourceKeyspaceName := "product"
	targetKeyspaceName := "customer"
	var mt iMoveTables
	workflowName := "wf1"

	sourceReplicaTab = vc.Cells["zone1"].Keyspaces[sourceKeyspaceName].Shards["0"].Tablets["zone1-101"].Vttablet
	require.NotNil(t, sourceReplicaTab)
	sourceTab = vc.Cells["zone1"].Keyspaces[sourceKeyspaceName].Shards["0"].Tablets["zone1-100"].Vttablet
	require.NotNil(t, sourceTab)

	targetTabs := setupMinimalCustomerKeyspace(t)
	targetTab1 = targetTabs["-80"]
	require.NotNil(t, targetTab1)
	targetTab2 = targetTabs["80-"]
	require.NotNil(t, targetTab2)
	targetReplicaTab1 = vc.Cells["zone1"].Keyspaces[targetKeyspaceName].Shards["-80"].Tablets["zone1-201"].Vttablet
	require.NotNil(t, targetReplicaTab1)

	t.Run("RoutingRulesApply", func(t *testing.T) {
		testRoutingRulesApplyCommands(t)
	})
	t.Run("WorkflowList", func(t *testing.T) {
		testWorkflowList(t, sourceKeyspaceName, targetKeyspaceName)
	})
	t.Run("MoveTablesCreateFlags1", func(t *testing.T) {
		testMoveTablesFlags1(t, &mt, sourceKeyspaceName, targetKeyspaceName, workflowName, targetTabs)
	})
	t.Run("testWorkflowUpdateConfig", func(t *testing.T) {
		testWorkflowUpdateConfig(t, &mt, targetTabs, targetKeyspaceName, workflowName)
	})
	t.Run("MoveTablesCreateFlags2", func(t *testing.T) {
		testMoveTablesFlags2(t, &mt, sourceKeyspaceName, targetKeyspaceName, workflowName, targetTabs)
	})
	t.Run("MoveTablesCompleteFlags3", func(t *testing.T) {
		testMoveTablesFlags3(t, sourceKeyspaceName, targetKeyspaceName, targetTabs)
	})
	t.Run("Reshard", func(t *testing.T) {
		cell := vc.Cells["zone1"]
		targetKeyspace := cell.Keyspaces[targetKeyspaceName]
		sourceShard := "-80"
		newShards := "-40,40-80"
		require.NoError(t, vc.AddShards(t, []*Cell{cell}, targetKeyspace, newShards, 1, 0, 400, nil))
		reshardWorkflowName := "reshard"
		tablets := map[string]*cluster.VttabletProcess{
			"-40":   targetKeyspace.Shards["-40"].Tablets["zone1-400"].Vttablet,
			"40-80": targetKeyspace.Shards["40-80"].Tablets["zone1-500"].Vttablet,
		}

		sourceReplicaTab = vc.Cells["zone1"].Keyspaces[targetKeyspaceName].Shards["-80"].Tablets["zone1-201"].Vttablet
		require.NotNil(t, sourceReplicaTab)
		sourceTab = vc.Cells["zone1"].Keyspaces[targetKeyspaceName].Shards["-80"].Tablets["zone1-200"].Vttablet
		require.NotNil(t, sourceTab)

		targetTab1 = tablets["-40"]
		require.NotNil(t, targetTab1)
		targetTab2 = tablets["40-80"]
		require.NotNil(t, targetTab2)
		targetReplicaTab1 = vc.Cells["zone1"].Keyspaces[targetKeyspaceName].Shards["-40"].Tablets["zone1-401"].Vttablet
		require.NotNil(t, targetReplicaTab1)

		splitShard(t, targetKeyspaceName, reshardWorkflowName, sourceShard, newShards, tablets)
	})

	t.Run("Reshard Cancel", func(t *testing.T) {
		cell := vc.Cells["zone1"]
		targetKeyspace := cell.Keyspaces[targetKeyspaceName]
		sourceShard := "80-"
		newShards := "80-c0,c0-"
		require.NoError(t, vc.AddShards(t, []*Cell{cell}, targetKeyspace, newShards, 1, 0, 600, nil))
		reshardWorkflowName := "reshard"

		tablets := map[string]*cluster.VttabletProcess{
			"80-c0": targetKeyspace.Shards["80-c0"].Tablets["zone1-600"].Vttablet,
			"c0-":   targetKeyspace.Shards["c0-"].Tablets["zone1-700"].Vttablet,
		}

		sourceReplicaTab = vc.Cells["zone1"].Keyspaces[targetKeyspaceName].Shards["80-"].Tablets["zone1-301"].Vttablet
		require.NotNil(t, sourceReplicaTab)
		sourceTab = vc.Cells["zone1"].Keyspaces[targetKeyspaceName].Shards["80-"].Tablets["zone1-300"].Vttablet
		require.NotNil(t, sourceTab)

		targetTab1 = tablets["80-c0"]
		require.NotNil(t, targetTab1)
		targetTab2 = tablets["c0-"]
		require.NotNil(t, targetTab2)
		targetReplicaTab1 = vc.Cells["zone1"].Keyspaces[targetKeyspaceName].Shards["80-c0"].Tablets["zone1-601"].Vttablet
		require.NotNil(t, targetReplicaTab1)

		overrides := map[string]string{
			"vreplication_copy_phase_duration":     "10h11m12s",
			"vreplication_experimental_flags":      "7",
			"vreplication-parallel-insert-workers": "4",
			"vreplication_net_read_timeout":        "6000",
			"relay_log_max_items":                  "10000",
		}
		createFlags := []string{"--auto-start=false", "--defer-secondary-keys=false",
			"--on-ddl", "STOP", "--tablet-types", "primary,rdonly", "--tablet-types-in-preference-order=true",
			"--all-cells", "--format=json",
			"--config-overrides", mapToCSV(overrides),
		}

		rs := newReshard(vc, &reshardWorkflow{
			workflowInfo: &workflowInfo{
				vc:             vc,
				workflowName:   reshardWorkflowName,
				targetKeyspace: targetKeyspaceName,
			},
			sourceShards: sourceShard,
			targetShards: newShards,
			createFlags:  createFlags,
		}, workflowFlavorVtctld)

		rs.Create()

		resp := getReshardResponse(rs)
		require.NotNil(vc.t, resp)
		require.NotNil(vc.t, resp.ShardStreams)
		require.Equal(vc.t, len(resp.ShardStreams), 2)
		keyspace := "customer"
		for _, shard := range []string{"80-c0", "c0-"} {
			streams := resp.ShardStreams[fmt.Sprintf("%s/%s", keyspace, shard)]
			require.Equal(vc.t, 1, len(streams.Streams))
			require.Equal(vc.t, binlogdatapb.VReplicationWorkflowState_Stopped.String(), streams.Streams[0].Status)
		}

		rs.Start()
		waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", keyspace, reshardWorkflowName), binlogdatapb.VReplicationWorkflowState_Running.String())

		res, err := targetTab1.QueryTablet("show tables", keyspace, true)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.NotEmpty(t, res.Rows)

		res, err = targetTab2.QueryTablet("show tables", keyspace, true)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.NotEmpty(t, res.Rows)

		rs.Cancel()

		workflowNames := workflowList(keyspace)
		require.Empty(t, workflowNames)

		res, err = targetTab1.QueryTablet("show tables", keyspace, true)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Empty(t, res.Rows)

		res, err = targetTab2.QueryTablet("show tables", keyspace, true)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Empty(t, res.Rows)
	})
}

// Tests several create flags and some complete flags and validates that some of them are set correctly for the workflow.
func testMoveTablesFlags1(t *testing.T, mt *iMoveTables, sourceKeyspace, targetKeyspace, workflowName string, targetTabs map[string]*cluster.VttabletProcess) {
	tables := "customer,customer2"
	overrides := map[string]string{
		"vreplication_net_read_timeout":        "6000",
		"relay_log_max_items":                  "10000",
		"vreplication-parallel-insert-workers": "10",
	}
	createFlags := []string{"--auto-start=false", "--defer-secondary-keys=false", "--stop-after-copy",
		"--no-routing-rules", "--on-ddl", "STOP", "--exclude-tables", "customer2",
		"--tablet-types", "primary,rdonly", "--tablet-types-in-preference-order=true",
		"--all-cells", "--config-overrides", mapToCSV(overrides),
		"--sharded-auto-increment-handling=REPLACE", fmt.Sprintf("--global-keyspace=%s", sourceKeyspace),
	}
	completeFlags := []string{"--keep-routing-rules", "--keep-data"}
	switchFlags := []string{}
	// Test one set of MoveTable flags.
	*mt = createMoveTables(t, sourceKeyspace, targetKeyspace, workflowName, tables, createFlags, completeFlags, switchFlags)
	(*mt).Show()
	moveTablesResponse := getMoveTablesShowResponse(mt)
	workflowResponse := getWorkflow(targetKeyspace, workflowName)

	// also validates that MoveTables Show and Workflow Show return the same output.
	require.EqualValues(t, moveTablesResponse.CloneVT(), workflowResponse)

	// Validate that the flags are set correctly in the database.
	validateMoveTablesWorkflow(t, workflowResponse.Workflows)
	// Since we used --no-routing-rules, there should be no routing rules.
	confirmNoRoutingRules(t)
	validateOverrides(t, targetTabs, overrides)
}

func getMoveTablesShowResponse(mt *iMoveTables) *vtctldatapb.GetWorkflowsResponse {
	moveTablesOutput := (*mt).GetLastOutput()
	var moveTablesResponse vtctldatapb.GetWorkflowsResponse
	err := protojson.Unmarshal([]byte(moveTablesOutput), &moveTablesResponse)
	require.NoError(vc.t, err)
	moveTablesResponse.Workflows[0].MaxVReplicationTransactionLag = 0
	moveTablesResponse.Workflows[0].MaxVReplicationLag = 0
	return moveTablesResponse.CloneVT()
}

// Validates some of the flags created from the previous test.
func testMoveTablesFlags2(t *testing.T, mt *iMoveTables, sourceKeyspace, targetKeyspace, workflowName string, targetTabs map[string]*cluster.VttabletProcess) {
	ksWorkflow := fmt.Sprintf("%s.%s", targetKeyspace, workflowName)
	wf := (*mt).(iWorkflow)
	(*mt).Start() // Need to start because we set auto-start to false.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	confirmNoRoutingRules(t)
	for _, tab := range targetTabs {
		alias := fmt.Sprintf("zone1-%d", tab.TabletUID)
		query := "update _vt.vreplication set source := replace(source, 'stop_after_copy:true', 'stop_after_copy:false') where db_name = 'vt_customer' and workflow = 'wf1'"
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("ExecuteFetchAsDba", alias, query)
		require.NoError(t, err, output)
	}
	confirmNoRoutingRules(t)
	(*mt).Start() // Need to start because we set stop-after-copy to true.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	(*mt).Stop() // Test stopping workflow.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	(*mt).Start()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	for _, tab := range targetTabs {
		catchup(t, tab, workflowName, "MoveTables")
	}

	(*mt).SwitchReads()
	validateReadsRouteToTarget(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", sourceKs, targetKs)
	validateTableRoutingRule(t, "customer", "", targetKs, sourceKs)
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateReadsSwitched)

	(*mt).ReverseReads()
	validateReadsRouteToSource(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", targetKs, sourceKs)
	validateTableRoutingRule(t, "customer", "", targetKs, sourceKs)
	confirmStates(t, &wf, wrangler.WorkflowStateReadsSwitched, wrangler.WorkflowStateNotSwitched)

	(*mt).SwitchReadsAndWrites()
	validateReadsRouteToTarget(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", sourceKs, targetKs)
	validateWritesRouteToTarget(t)
	validateTableRoutingRule(t, "customer", "", sourceKs, targetKs)
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)

	(*mt).ReverseReadsAndWrites()
	validateReadsRouteToSource(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", targetKs, sourceKs)
	validateWritesRouteToSource(t)
	validateTableRoutingRule(t, "customer", "", targetKs, sourceKs)
	confirmStates(t, &wf, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateNotSwitched)

	(*mt).SwitchReadsAndWrites()
	validateReadsRouteToTarget(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", sourceKs, targetKs)
	validateWritesRouteToTarget(t)
	validateTableRoutingRule(t, "customer", "", sourceKs, targetKs)
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)

	(*mt).ReverseReads()
	validateReadsRouteToSource(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", targetKs, sourceKs)
	validateWritesRouteToTarget(t)
	validateTableRoutingRule(t, "customer", "", sourceKs, targetKs)
	confirmStates(t, &wf, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateWritesSwitched)

	(*mt).ReverseWrites()
	validateReadsRouteToSource(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", targetKs, sourceKs)
	validateWritesRouteToSource(t)
	validateTableRoutingRule(t, "customer", "", targetKs, sourceKs)
	confirmStates(t, &wf, wrangler.WorkflowStateWritesSwitched, wrangler.WorkflowStateNotSwitched)

	(*mt).SwitchReadsAndWrites()
	validateReadsRouteToTarget(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", sourceKs, targetKs)
	validateWritesRouteToTarget(t)
	validateTableRoutingRule(t, "customer", "", sourceKs, targetKs)
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)

	(*mt).ReverseWrites()
	validateReadsRouteToTarget(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", sourceKs, targetKs)
	validateWritesRouteToSource(t)
	validateTableRoutingRule(t, "customer", "", targetKs, sourceKs)
	confirmStates(t, &wf, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateReadsSwitched)

	(*mt).ReverseReads()
	validateReadsRouteToSource(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", targetKs, sourceKs)
	validateWritesRouteToSource(t)
	validateTableRoutingRule(t, "customer", "", targetKs, sourceKs)
	confirmStates(t, &wf, wrangler.WorkflowStateReadsSwitched, wrangler.WorkflowStateNotSwitched)

	// Confirm that everything is still in sync after our switch fest.
	vdiff(t, targetKeyspace, workflowName, "zone1", false, true, nil)

	(*mt).SwitchReadsAndWrites()
	validateReadsRouteToTarget(t, "replica")
	validateTableRoutingRule(t, "customer", "replica", sourceKs, targetKs)
	validateWritesRouteToTarget(t)
	validateTableRoutingRule(t, "customer", "", sourceKs, targetKs)
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)

	(*mt).Complete()
	confirmRoutingRulesExist(t)
	// Confirm that --keep-data was honored.
	require.True(t, checkTablesExist(t, "zone1-100", []string{"customer", "customer2"}))
}

// Tests SwitchTraffic and Complete flags
func testMoveTablesFlags3(t *testing.T, sourceKeyspace, targetKeyspace string, targetTabs map[string]*cluster.VttabletProcess) {
	for _, tab := range targetTabs {
		alias := fmt.Sprintf("zone1-%d", tab.TabletUID)
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("ExecuteFetchAsDba", alias, "drop table customer")
		require.NoError(t, err, output)
	}
	createFlags := []string{}
	completeFlags := []string{"--rename-tables"}
	tables := "customer2"
	switchFlags := []string{"--enable-reverse-replication=false"}
	mt := createMoveTables(t, sourceKeyspace, targetKeyspace, workflowName, tables, createFlags, completeFlags, switchFlags)
	mt.Start() // Need to start because we set stop-after-copy to true.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	mt.Stop() // Test stopping workflow.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	mt.Start()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	for _, tab := range targetTabs {
		catchup(t, tab, workflowName, "MoveTables")
	}
	mt.SwitchReadsAndWrites()
	mt.Complete()
	// Confirm that the source tables were renamed.
	require.True(t, checkTablesExist(t, "zone1-100", []string{"_customer2_old"}))
	require.False(t, checkTablesExist(t, "zone1-100", []string{"customer2"}))
}

// Create two workflows in order to confirm that listing all workflows works.
func testWorkflowList(t *testing.T, sourceKeyspace, targetKeyspace string) {
	createFlags := []string{"--auto-start=false", "--tablet-types",
		"primary,rdonly", "--tablet-types-in-preference-order=true", "--all-cells",
	}
	wfNames := []string{"list1", "list2"}
	tables := []string{"customer", "customer2"}
	for i := range wfNames {
		mt := createMoveTables(t, sourceKeyspace, targetKeyspace, wfNames[i], tables[i], createFlags, nil, nil)
		defer mt.Cancel()
	}
	slices.Sort(wfNames)

	workflowNames := workflowList(targetKeyspace)
	slices.Sort(workflowNames)
	require.EqualValues(t, wfNames, workflowNames)

	workflows := getWorkflows(targetKeyspace)
	workflowNames = make([]string, len(workflows.Workflows))
	for i := range workflows.Workflows {
		workflowNames[i] = workflows.Workflows[i].Name
	}
	slices.Sort(workflowNames)
	require.EqualValues(t, wfNames, workflowNames)
}

func testWorkflowUpdateConfig(t *testing.T, mt *iMoveTables, targetTabs map[string]*cluster.VttabletProcess, targetKeyspace, workflow string) {
	updateConfig := func(t *testing.T, overrides map[string]string) error {
		overridesCSV := mapToCSV(overrides)
		_, err := vc.VtctldClient.ExecuteCommandWithOutput("workflow", "--keyspace", targetKeyspace, "update",
			"--workflow", workflow, "--config-overrides", overridesCSV)
		return err
	}
	require.GreaterOrEqual(t, len(targetTabs), 1)
	tab := maps.Values(targetTabs)[0]
	require.NotNil(t, tab)
	defaultConfig := vttablet.InitVReplicationConfigDefaults()
	type testCase struct {
		name      string
		config    map[string]string
		needError bool
		clears    bool
	}
	testCases := []testCase{
		{
			name:   "reset flags",
			config: defaultConfig.Map(),
			clears: true,
		},
		{
			name: "one value",
			config: map[string]string{
				"vreplication_heartbeat_update_interval": "10",
			},
		},
		{
			name: "two values",
			config: map[string]string{
				"vreplication_heartbeat_update_interval": "100",
				"vreplication_store_compressed_gtid":     "true",
			},
		},
		{
			name: "invalid value",
			config: map[string]string{
				"vreplication_heartbeat_update_interval": "12s",
				"vreplication_store_compressed_gtid":     "true",
			},
			needError: true,
		},
		{
			name: "unknown flag",
			config: map[string]string{
				"vreplication_heartbeat_update_interval": "1",
				"vreplication_store_compressed_gtid":     "true",
				"unknown":                                "value",
			},
			needError: true,
		},
		{
			name: "clear flags",
			config: map[string]string{
				"vreplication_heartbeat_update_interval": "",
				"vreplication_store_compressed_gtid":     "",
			},
			clears: true,
		},
	}

	expectedConfig, err := vttablet.NewVReplicationConfig(nil)
	require.NoError(t, err)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := updateConfig(t, tc.config)
			if tc.needError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				expectedConfig, err = vttablet.NewVReplicationConfig(tc.config)
				require.NoError(t, err)
			}
			config := getVReplicationConfig(t, tab)
			if tc.clears {
				expectedConfig, err = vttablet.NewVReplicationConfig(nil)
				require.NoError(t, err)
			}
			require.EqualValues(t, expectedConfig.Map(), config)
		})
	}
}

func createMoveTables(t *testing.T, sourceKeyspace, targetKeyspace, workflowName, tables string,
	createFlags, completeFlags, switchFlags []string) iMoveTables {
	mt := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         tables,
		createFlags:    createFlags,
		completeFlags:  completeFlags,
		switchFlags:    switchFlags,
	}, workflowFlavorVtctld)
	mt.Create()
	return mt
}

// reshard helpers

func splitShard(t *testing.T, keyspace, workflowName, sourceShards, targetShards string, targetTabs map[string]*cluster.VttabletProcess) {
	overrides := map[string]string{
		"vreplication_copy_phase_duration":     "10h11m12s",
		"vreplication_experimental_flags":      "7",
		"vreplication-parallel-insert-workers": "4",
		"vreplication_net_read_timeout":        "6000",
		"relay_log_max_items":                  "10000",
	}
	createFlags := []string{"--auto-start=false", "--defer-secondary-keys=false", "--stop-after-copy",
		"--on-ddl", "STOP", "--tablet-types", "primary,rdonly", "--tablet-types-in-preference-order=true",
		"--all-cells", "--format=json",
		"--config-overrides", mapToCSV(overrides),
	}
	rs := newReshard(vc, &reshardWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: keyspace,
		},
		sourceShards: sourceShards,
		targetShards: targetShards,
		createFlags:  createFlags,
	}, workflowFlavorVtctld)

	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflowName)
	wf := rs.(iWorkflow)
	rs.Create()
	validateReshardResponse(rs)
	validateOverrides(t, targetTabs, overrides)
	workflowResponse := getWorkflow(keyspace, workflowName)
	reshardShowResponse := getReshardShowResponse(&rs)
	require.EqualValues(t, reshardShowResponse, workflowResponse)
	validateReshardWorkflow(t, workflowResponse.Workflows)
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", keyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Stopped.String())
	rs.Start()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	for _, tab := range targetTabs {
		alias := fmt.Sprintf("zone1-%d", tab.TabletUID)
		query := "update _vt.vreplication set source := replace(source, 'stop_after_copy:true', 'stop_after_copy:false') where db_name = 'vt_customer' and workflow = '" + workflowName + "'"
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("ExecuteFetchAsDba", alias, query)
		require.NoError(t, err, output)
	}
	rs.Start()
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", keyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	rs.Stop()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	rs.Start()
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", keyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	for _, targetTab := range targetTabs {
		catchup(t, targetTab, workflowName, "Reshard")
	}
	vdiff(t, keyspace, workflowName, "zone1", false, true, nil)

	shardReadsRouteToSource := func() {
		require.True(t, getShardRoute(t, keyspace, "-80", "replica"))
	}

	shardReadsRouteToTarget := func() {
		require.True(t, getShardRoute(t, keyspace, "-40", "replica"))
	}

	shardWritesRouteToSource := func() {
		require.True(t, getShardRoute(t, keyspace, "-80", "primary"))
	}

	shardWritesRouteToTarget := func() {
		require.True(t, getShardRoute(t, keyspace, "-40", "primary"))
	}

	rs.SwitchReadsAndWrites()
	waitForLowLag(t, keyspace, workflowName+"_reverse")
	vdiff(t, keyspace, workflowName+"_reverse", "zone1", true, false, nil)
	shardReadsRouteToTarget()
	shardWritesRouteToTarget()
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)

	rs.ReverseReadsAndWrites()
	waitForLowLag(t, keyspace, workflowName)
	vdiff(t, keyspace, workflowName, "zone1", false, true, nil)
	shardReadsRouteToSource()
	shardWritesRouteToSource()
	confirmStates(t, &wf, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateNotSwitched)

	rs.SwitchReads()
	shardReadsRouteToTarget()
	shardWritesRouteToSource()
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateReadsSwitched)

	rs.ReverseReads()
	shardReadsRouteToSource()
	shardWritesRouteToSource()
	confirmStates(t, &wf, wrangler.WorkflowStateReadsSwitched, wrangler.WorkflowStateNotSwitched)

	rs.SwitchReadsAndWrites()
	shardReadsRouteToTarget()
	shardWritesRouteToTarget()
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)

	rs.ReverseReadsAndWrites()
	shardReadsRouteToSource()
	shardWritesRouteToSource()
	confirmStates(t, &wf, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateNotSwitched)

	rs.SwitchReadsAndWrites()
	shardReadsRouteToTarget()
	shardWritesRouteToTarget()
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)

	rs.ReverseReads()
	shardReadsRouteToSource()
	shardWritesRouteToTarget()
	confirmStates(t, &wf, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateWritesSwitched)

	rs.ReverseWrites()
	shardReadsRouteToSource()
	shardWritesRouteToSource()
	confirmStates(t, &wf, wrangler.WorkflowStateWritesSwitched, wrangler.WorkflowStateNotSwitched)

	rs.SwitchReadsAndWrites()
	shardReadsRouteToTarget()
	shardWritesRouteToTarget()
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)

	rs.ReverseWrites()
	shardReadsRouteToTarget()
	shardWritesRouteToSource()
	confirmStates(t, &wf, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateReadsSwitched)

	rs.ReverseReads()
	shardReadsRouteToSource()
	shardWritesRouteToSource()
	confirmStates(t, &wf, wrangler.WorkflowStateReadsSwitched, wrangler.WorkflowStateNotSwitched)

	// Confirm that everything is still in sync after our switch fest.
	vdiff(t, keyspace, workflowName, "zone1", false, true, nil)

	rs.SwitchReadsAndWrites()
	shardReadsRouteToTarget()
	shardWritesRouteToTarget()
	confirmStates(t, &wf, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)

	rs.Complete()
}

func getSrvKeyspace(t *testing.T, keyspace string) *topodatapb.SrvKeyspace {
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("GetSrvKeyspaces", keyspace, "zone1")
	require.NoError(t, err)
	var srvKeyspaces map[string]*topodatapb.SrvKeyspace
	err = json2.Unmarshal([]byte(output), &srvKeyspaces)
	require.NoError(t, err)
	require.Equal(t, 1, len(srvKeyspaces))
	return srvKeyspaces["zone1"]
}

func getShardRoute(t *testing.T, keyspace, shard string, tabletType string) bool {
	srvKeyspace := getSrvKeyspace(t, keyspace)
	for _, partition := range srvKeyspace.Partitions {
		tt, err := topoproto.ParseTabletType(tabletType)
		require.NoError(t, err)
		if partition.ServedType == tt {
			for _, shardReference := range partition.ShardReferences {
				if shardReference.Name == shard {
					return true
				}
			}
		}
	}
	return false
}

func getReshardShowResponse(rs *iReshard) *vtctldatapb.GetWorkflowsResponse {
	(*rs).Show()
	reshardOutput := (*rs).GetLastOutput()
	var reshardResponse vtctldatapb.GetWorkflowsResponse
	err := protojson.Unmarshal([]byte(reshardOutput), &reshardResponse)
	require.NoError(vc.t, err)
	reshardResponse.Workflows[0].MaxVReplicationTransactionLag = 0
	reshardResponse.Workflows[0].MaxVReplicationLag = 0
	return reshardResponse.CloneVT()
}

func validateReshardResponse(rs iReshard) {
	resp := getReshardResponse(rs)
	require.NotNil(vc.t, resp)
	require.NotNil(vc.t, resp.ShardStreams)
	require.Equal(vc.t, len(resp.ShardStreams), 2)
	keyspace := "customer"
	for _, shard := range []string{"-40", "40-80"} {
		streams := resp.ShardStreams[fmt.Sprintf("%s/%s", keyspace, shard)]
		require.Equal(vc.t, 1, len(streams.Streams))
		require.Equal(vc.t, binlogdatapb.VReplicationWorkflowState_Stopped.String(), streams.Streams[0].Status)
	}
}

func validateReshardWorkflow(t *testing.T, workflows []*vtctldatapb.Workflow) {
	require.Equal(t, 1, len(workflows))
	wf := workflows[0]
	require.Equal(t, "reshard", wf.Name)
	require.Equal(t, binlogdatapb.VReplicationWorkflowType_Reshard.String(), wf.WorkflowType)
	require.Equal(t, "None", wf.WorkflowSubType)
	require.Equal(t, "customer", wf.Target.Keyspace)
	require.Equal(t, 2, len(wf.Target.Shards))
	require.Equal(t, "customer", wf.Source.Keyspace)
	require.Equal(t, 1, len(wf.Source.Shards))
	require.False(t, wf.DeferSecondaryKeys)

	require.GreaterOrEqual(t, len(wf.ShardStreams), int(1))
	oneStream := maps.Values(wf.ShardStreams)[0]
	require.NotNil(t, oneStream)

	stream := oneStream.Streams[0]
	require.Equal(t, binlogdatapb.VReplicationWorkflowState_Stopped.String(), stream.State)
	require.Equal(t, stream.TabletSelectionPreference, tabletmanagerdatapb.TabletSelectionPreference_INORDER)
	require.True(t, slices.Equal([]topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_RDONLY}, stream.TabletTypes))
	require.True(t, slices.Equal([]string{"zone1", "zone2"}, stream.Cells))

	bls := stream.BinlogSource
	require.Equal(t, binlogdatapb.OnDDLAction_STOP, bls.OnDdl)
	require.True(t, bls.StopAfterCopy)

}

func getReshardResponse(rs iReshard) *vtctldatapb.WorkflowStatusResponse {
	reshardOutput := rs.GetLastOutput()
	var reshardResponse vtctldatapb.WorkflowStatusResponse
	err := protojson.Unmarshal([]byte(reshardOutput), &reshardResponse)
	require.NoError(vc.t, err)
	return reshardResponse.CloneVT()
}

// helper functions

func getWorkflow(targetKeyspace, workflow string) *vtctldatapb.GetWorkflowsResponse {
	workflowOutput, err := vc.VtctldClient.ExecuteCommandWithOutput("Workflow", "--keyspace", targetKeyspace, "show", "--workflow", workflow)
	require.NoError(vc.t, err)
	var workflowResponse vtctldatapb.GetWorkflowsResponse
	err = protojson.Unmarshal([]byte(workflowOutput), &workflowResponse)
	require.NoError(vc.t, err)
	workflowResponse.Workflows[0].MaxVReplicationTransactionLag = 0
	workflowResponse.Workflows[0].MaxVReplicationLag = 0
	return workflowResponse.CloneVT()
}

func getWorkflows(targetKeyspace string) *vtctldatapb.GetWorkflowsResponse {
	getWorkflowsOutput, err := vc.VtctldClient.ExecuteCommandWithOutput("GetWorkflows", targetKeyspace, "--show-all", "--compact", "--include-logs=false")
	require.NoError(vc.t, err)
	var getWorkflowsResponse vtctldatapb.GetWorkflowsResponse
	err = protojson.Unmarshal([]byte(getWorkflowsOutput), &getWorkflowsResponse)
	require.NoError(vc.t, err)
	return getWorkflowsResponse.CloneVT()
}

func workflowList(targetKeyspace string) []string {
	workflowListOutput, err := vc.VtctldClient.ExecuteCommandWithOutput("Workflow", "--keyspace", targetKeyspace, "list")
	require.NoError(vc.t, err)
	var workflowList []string
	err = json.Unmarshal([]byte(workflowListOutput), &workflowList)
	require.NoError(vc.t, err)
	return workflowList
}

func checkTablesExist(t *testing.T, tabletAlias string, tables []string) bool {
	tablesResponse, err := vc.VtctldClient.ExecuteCommandWithOutput("GetSchema", tabletAlias, "--tables", strings.Join(tables, ","), "--table-names-only")
	require.NoError(t, err)
	tablesFound := strings.Split(tablesResponse, "\n")
	for _, table := range tables {
		found := false
		for _, tableFound := range tablesFound {
			if tableFound == table {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func getMirrorRules(t *testing.T) *vschemapb.MirrorRules {
	mirrorRules, err := vc.VtctldClient.ExecuteCommandWithOutput("GetMirrorRules")
	require.NoError(t, err)
	var mirrorRulesResponse vschemapb.MirrorRules
	err = protojson.Unmarshal([]byte(mirrorRules), &mirrorRulesResponse)
	require.NoError(t, err)
	return &mirrorRulesResponse
}

func confirmNoMirrorRules(t *testing.T) {
	mirrorRulesResponse := getMirrorRules(t)
	require.Zero(t, len(mirrorRulesResponse.Rules))
}

func confirmMirrorRulesExist(t *testing.T) {
	mirrorRulesResponse := getMirrorRules(t)
	require.NotZero(t, len(mirrorRulesResponse.Rules))
}

func expectMirrorRules(t *testing.T, sourceKeyspace, targetKeyspace string, tables []string, tabletTypes []topodatapb.TabletType, percent float32) {
	t.Helper()

	// Each table should have a mirror rule for each serving type.
	mirrorRules := getMirrorRules(t)
	require.Len(t, mirrorRules.Rules, len(tables)*len(tabletTypes))
	fromTableToRule := make(map[string]*vschemapb.MirrorRule)
	for _, rule := range mirrorRules.Rules {
		fromTableToRule[rule.FromTable] = rule
	}
	for _, table := range tables {
		for _, tabletType := range tabletTypes {
			fromTable := fmt.Sprintf("%s.%s", sourceKeyspace, table)
			if tabletType != topodatapb.TabletType_PRIMARY {
				fromTable = fmt.Sprintf("%s@%s", fromTable, topoproto.TabletTypeLString(tabletType))
			}
			require.Contains(t, fromTableToRule, fromTable)
			require.Equal(t, fmt.Sprintf("%s.%s", targetKeyspace, table), fromTableToRule[fromTable].ToTable)
			require.Equal(t, percent, fromTableToRule[fromTable].Percent)
		}
	}
}

func getRoutingRules(t *testing.T) *vschemapb.RoutingRules {
	routingRules, err := vc.VtctldClient.ExecuteCommandWithOutput("GetRoutingRules")
	require.NoError(t, err)
	var routingRulesResponse vschemapb.RoutingRules
	err = protojson.Unmarshal([]byte(routingRules), &routingRulesResponse)
	require.NoError(t, err)
	return &routingRulesResponse
}

func confirmNoRoutingRules(t *testing.T) {
	routingRulesResponse := getRoutingRules(t)
	require.Zero(t, len(routingRulesResponse.Rules))
}

func confirmRoutingRulesExist(t *testing.T) {
	routingRulesResponse := getRoutingRules(t)
	require.NotZero(t, len(routingRulesResponse.Rules))
}

// We only want to validate non-standard attributes that are set by the CLI. The other end-to-end tests validate the rest.
// We also check some of the standard attributes to make sure they are set correctly.
func validateMoveTablesWorkflow(t *testing.T, workflows []*vtctldatapb.Workflow) {
	require.Equal(t, 1, len(workflows))
	wf := workflows[0]
	require.Equal(t, "wf1", wf.Name)
	require.Equal(t, binlogdatapb.VReplicationWorkflowType_MoveTables.String(), wf.WorkflowType)
	require.Equal(t, "None", wf.WorkflowSubType)
	require.Equal(t, "customer", wf.Target.Keyspace)
	require.Equal(t, 2, len(wf.Target.Shards))
	require.Equal(t, "product", wf.Source.Keyspace)
	require.Equal(t, 1, len(wf.Source.Shards))
	require.False(t, wf.DeferSecondaryKeys)

	require.GreaterOrEqual(t, len(wf.ShardStreams), int(1))
	oneStream := maps.Values(wf.ShardStreams)[0]
	require.NotNil(t, oneStream)

	stream := oneStream.Streams[0]
	require.Equal(t, binlogdatapb.VReplicationWorkflowState_Stopped.String(), stream.State)
	require.Equal(t, stream.TabletSelectionPreference, tabletmanagerdatapb.TabletSelectionPreference_INORDER)
	require.True(t, slices.Equal([]topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_RDONLY}, stream.TabletTypes))
	require.True(t, slices.Equal([]string{"zone1", "zone2"}, stream.Cells))

	bls := stream.BinlogSource
	require.Equalf(t, 1, len(bls.Filter.Rules), "Rules are %+v", bls.Filter.Rules) // only customer, customer2 should be excluded
	require.Equal(t, binlogdatapb.OnDDLAction_STOP, bls.OnDdl)
	require.True(t, bls.StopAfterCopy)

	// Validate the sharded-auto-increment-handling related value handling.
	require.Equal(t, vtctldatapb.ShardedAutoIncrementHandling_REPLACE, vtctldatapb.ShardedAutoIncrementHandling(wf.Options.ShardedAutoIncrementHandling))
	require.Equal(t, wf.Source.Keyspace, wf.Options.GlobalKeyspace)
}

// Test that routing rules can be applied using the vtctldclient CLI for all types of routing rules.
func testRoutingRulesApplyCommands(t *testing.T) {
	var rulesBytes []byte
	var err error
	var validateRules func(want, got string)

	ruleTypes := []string{"RoutingRules", "ShardRoutingRules", "KeyspaceRoutingRules"}
	for _, typ := range ruleTypes {
		switch typ {
		case "RoutingRules":
			rr := &vschemapb.RoutingRules{
				Rules: []*vschemapb.RoutingRule{
					{
						FromTable: "from1",
						ToTables:  []string{"to1", "to2"},
					},
				},
			}
			rulesBytes, err = json2.MarshalPB(rr)
			require.NoError(t, err)
			validateRules = func(want, got string) {
				var wantRules = &vschemapb.RoutingRules{}
				require.NoError(t, json2.UnmarshalPB([]byte(want), wantRules))
				var gotRules = &vschemapb.RoutingRules{}
				require.NoError(t, json2.UnmarshalPB([]byte(got), gotRules))
				require.EqualValues(t, wantRules, gotRules)
			}
		case "ShardRoutingRules":
			srr := &vschemapb.ShardRoutingRules{
				Rules: []*vschemapb.ShardRoutingRule{
					{
						FromKeyspace: "from1",
						ToKeyspace:   "to1",
						Shard:        "-80",
					},
				},
			}
			rulesBytes, err = json2.MarshalPB(srr)
			require.NoError(t, err)
			validateRules = func(want, got string) {
				var wantRules = &vschemapb.ShardRoutingRules{}
				require.NoError(t, json2.UnmarshalPB([]byte(want), wantRules))
				var gotRules = &vschemapb.ShardRoutingRules{}
				require.NoError(t, json2.UnmarshalPB([]byte(got), gotRules))
				require.EqualValues(t, wantRules, gotRules)
			}
		case "KeyspaceRoutingRules":
			krr := &vschemapb.KeyspaceRoutingRules{
				Rules: []*vschemapb.KeyspaceRoutingRule{
					{
						FromKeyspace: "from1",
						ToKeyspace:   "to1",
					},
				},
			}
			rulesBytes, err = json2.MarshalPB(krr)
			require.NoError(t, err)
			validateRules = func(want, got string) {
				var wantRules = &vschemapb.KeyspaceRoutingRules{}
				require.NoError(t, json2.UnmarshalPB([]byte(want), wantRules))
				var gotRules = &vschemapb.KeyspaceRoutingRules{}
				require.NoError(t, json2.UnmarshalPB([]byte(got), gotRules))
				require.EqualValues(t, wantRules, gotRules)
			}
		default:
			require.FailNow(t, "Unknown type %s", typ)
		}
		testOneRoutingRulesCommand(t, typ, string(rulesBytes), validateRules)
	}

}

// For a given routing rules type, test that the rules can be applied using the vtctldclient CLI.
// We test both inline and file-based rules.
// The test also validates that both camelCase and snake_case key names work correctly.
func testOneRoutingRulesCommand(t *testing.T, typ string, rules string, validateRules func(want, got string)) {
	type routingRulesTest struct {
		name    string
		rules   string
		useFile bool // if true, use a file to pass the rules
	}
	tests := []routingRulesTest{
		{
			name:  "inline",
			rules: rules,
		},
		{
			name:    "file",
			rules:   rules,
			useFile: true,
		},
		{
			name:  "empty", // finally, cleanup rules
			rules: "{}",
		},
	}
	for _, tt := range tests {
		t.Run(typ+"/"+tt.name, func(t *testing.T) {
			wantRules := tt.rules
			// The input rules are in camelCase, since they are the output of json2.MarshalPB
			// The first iteration uses the output of routing rule Gets which are in snake_case.
			for _, keyCase := range []string{"camelCase", "snake_case"} {
				t.Run(keyCase, func(t *testing.T) {
					var args []string
					apply := fmt.Sprintf("Apply%s", typ)
					get := fmt.Sprintf("Get%s", typ)
					args = append(args, apply)
					if tt.useFile {
						tmpFile, err := os.CreateTemp("", fmt.Sprintf("%s_rules.json", tt.name))
						require.NoError(t, err)
						defer os.Remove(tmpFile.Name())
						_, err = tmpFile.WriteString(wantRules)
						require.NoError(t, err)
						args = append(args, "--rules-file", tmpFile.Name())
					} else {
						args = append(args, "--rules", wantRules)
					}
					var output string
					var err error
					if output, err = vc.VtctldClient.ExecuteCommandWithOutput(args...); err != nil {
						require.FailNowf(t, "failed action", apply, "%v: %s", err, output)
					}
					if output, err = vc.VtctldClient.ExecuteCommandWithOutput(get); err != nil {
						require.FailNowf(t, "failed action", get, "%v: %s", err, output)
					}
					validateRules(wantRules, output)
					// output of GetRoutingRules is in snake_case and we use it for the next iteration which
					// tests applying rules with snake_case keys.
					wantRules = output
				})
			}
		})
	}
}

func confirmStates(t *testing.T, workflow *iWorkflow, startState, endState string) {
	require.Contains(t, (*workflow).GetLastOutput(), fmt.Sprintf("Start State: %s", startState))
	require.Contains(t, (*workflow).GetLastOutput(), fmt.Sprintf("Current State: %s", endState))
}
