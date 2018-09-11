/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reshardingworkflowgen

import (
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/workflow/resharding"

	// import the gRPC client implementation for tablet manager
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	testKeyspace  = "test_keyspace"
	testVtworkers = "localhost:15032"
)

func init() {
	Register()
	resharding.Register()
}

// TestWorkflowGenerator runs the happy path of HorizontalReshardingWorkflow.
func TestWorfklowGenerator(t *testing.T) {
	ctx := context.Background()

	// Initialize the topology.
	ts := setupTopology(ctx, t, testKeyspace)
	m := workflow.NewManager(ts)
	// Run the manager in the background.
	workflow.StartManager(m)
	// Create the workflow.
	vtworkersParameter := testVtworkers + "," + testVtworkers
	uuid, err := m.Create(ctx, keyspaceReshardingFactoryName, []string{"-keyspace=" + testKeyspace, "-vtworkers=" + vtworkersParameter, "-min_healthy_rdonly_tablets=2"})
	if err != nil {
		t.Fatalf("cannot create resharding workflow: %v", err)
	}

	// Start the job.
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start resharding workflow: %v", err)
	}

	// Wait for the workflow to end.
	m.Wait(ctx, uuid)
	if err := workflow.VerifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop resharding workflow: %v", err)
	}
}

func setupTopology(ctx context.Context, t *testing.T, keyspace string) *topo.Server {
	ts := memorytopo.NewServer("cell")
	if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}
	ts.CreateShard(ctx, keyspace, "0")
	ts.CreateShard(ctx, keyspace, "-80")
	ts.CreateShard(ctx, keyspace, "80-")
	return ts
}
