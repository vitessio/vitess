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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestVtctldclientCLI(t *testing.T) {
	setSidecarDBName("_vt")
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultRdonly = origDefaultRdonly
	}()
	defaultRdonly = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()
	sourceKeyspace := "product"
	targetKeyspace := "customer"
	workflowName := "wf1"
	setupMinimalCustomerKeyspace(t)
	createFlags := []string{"--auto-start=false", "--defer-secondary-keys=false", "--stop-after-copy",
		"--no-routing-rules", "--on-ddl", "STOP",
		"--exclude-tables", "customer2",
		"--tablet-types", "primary,rdonly", "--tablet-types-in-preference-order=true",
	}

	mt := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         "customer,customer2",
		createFlags:    createFlags,
	}, workflowFlavorVtctld)
	mt.Create()

	mt.Show()
	moveTablesOutput := mt.GetLastOutput()

	workflowOutput, err := vc.VtctldClient.ExecuteCommandWithOutput("Workflow", "--keyspace", "customer", "show", "--workflow", "wf1")
	require.NoError(t, err)
	require.Equalf(t, moveTablesOutput, workflowOutput, "output of MoveTables Show should be same as that of Workflow Show")
	var response vtctldata.GetWorkflowsResponse
	err = protojson.Unmarshal([]byte(workflowOutput), &response)
	require.NoError(t, err)
	validateWorkflow1(t, response.Workflows)
	confirmNoRoutingRules(t)
}

func confirmNoRoutingRules(t *testing.T) {
	routingRules, err := vc.VtctldClient.ExecuteCommandWithOutput("GetRoutingRules")
	require.NoError(t, err)
	var routingRulesResponse vschema.RoutingRules
	err = protojson.Unmarshal([]byte(routingRules), &routingRulesResponse)
	require.NoError(t, err)
	require.Equal(t, 0, len(routingRulesResponse.Rules))
}

// We only want to validate non-standard attributes that are set by the CLI. The other end-to-end tests validate the rest.
// We also check some of the standard attributes to make sure they are set correctly.
func validateWorkflow1(t *testing.T, workflows []*vtctldata.Workflow) {
	require.Equal(t, 1, len(workflows))
	wf := workflows[0]
	require.Equal(t, "wf1", wf.Name)
	require.Equal(t, "MoveTables", wf.WorkflowType)
	require.Equal(t, "None", wf.WorkflowSubType)
	require.Equal(t, "customer", wf.Target.Keyspace)
	require.Equal(t, 2, len(wf.Target.Shards))
	require.Equal(t, "product", wf.Source.Keyspace)
	require.Equal(t, 1, len(wf.Source.Shards))
	require.False(t, wf.DeferSecondaryKeys)

	var oneStream *vtctldata.Workflow_ShardStream
	for _, stream := range wf.ShardStreams {
		oneStream = stream
		break
	}
	require.NotNil(t, oneStream)
	stream := oneStream.Streams[0]
	require.Equal(t, "Stopped", stream.State)

	bls := stream.BinlogSource
	// todo: this fails
	require.Equalf(t, 1, len(bls.Filter.Rules), "Rules are %+v", bls.Filter.Rules) // only customer, customer2 should be excluded
	require.Equal(t, binlogdata.OnDDLAction_STOP, bls.OnDdl)
	require.True(t, bls.StopAfterCopy)
	// fixme: this fails
	require.Equal(t, "in-order:primary,rdonly", bls.TabletType.String())
}
