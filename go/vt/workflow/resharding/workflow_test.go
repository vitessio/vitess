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

package resharding

import (
	"flag"
	"testing"

	"github.com/golang/mock/gomock"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/worker/fakevtworkerclient"
	"vitess.io/vitess/go/vt/worker/vtworkerclient"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/wrangler"

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
}

// TestSourceDestShards tests that provided source/dest shards are valid
func TestSourceDestShards(t *testing.T) {
	ctx := context.Background()

	// Set up the mock wrangler. It is used for the CopySchema,
	// WaitforFilteredReplication and Migrate phase.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Set up the fakeworkerclient. It is used at SplitClone and SplitDiff phase.
	fakeVtworkerClient := setupFakeVtworker(testKeyspace, testVtworkers, false, "", "SplitDiff")
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")

	// Initialize the topology.
	ts := setupTopology(ctx, t, testKeyspace)
	m := workflow.NewManager(ts)
	// Run the manager in the background.
	_, _, cancel := workflow.StartManager(m)
	// Create the workflow.
	vtworkersParameter := testVtworkers + "," + testVtworkers
	_, err := m.Create(ctx, horizontalReshardingFactoryName, []string{"-keyspace=" + testKeyspace, "-vtworkers=" + vtworkersParameter, "-phase_enable_approvals=", "-min_healthy_rdonly_tablets=2", "-source_shards=0", "-destination_shards=-40,40-"})
	want := "the specified destination shard test_keyspace/-40 is not in any overlapping shard"
	if err == nil || err.Error() != want {
		t.Errorf("workflow error: %v, want %s", err, want)
	}

	_, err = m.Create(ctx, horizontalReshardingFactoryName, []string{"-keyspace=" + testKeyspace, "-vtworkers=" + vtworkersParameter, "-phase_enable_approvals=", "-min_healthy_rdonly_tablets=2", "-source_shards=0", "-destination_shards=-80,40-"})

	want = "the specified destination shard test_keyspace/40- is not in any overlapping shard"
	if err == nil || err.Error() != want {
		t.Errorf("workflow error: %v, want %s", err, want)
	}

	_, err = m.Create(ctx, horizontalReshardingFactoryName, []string{"-keyspace=" + testKeyspace, "-vtworkers=" + vtworkersParameter, "-phase_enable_approvals=", "-min_healthy_rdonly_tablets=2", "-source_shards=-20", "-destination_shards=-80,80-"})

	want = "the specified source shard test_keyspace/-20 is not in any overlapping shard"
	if err == nil || err.Error() != want {
		t.Errorf("workflow error: %v, want %s", err, want)
	}
	cancel()
}

// TestHorizontalResharding runs the happy path of HorizontalReshardingWorkflow.
func TestHorizontalResharding(t *testing.T) {
	testHorizontalReshardingWorkflow(t, false, "", "SplitDiff")
}

// TestHorizontalReshardingWithConsistentSnapshot runs the happy path of HorizontalReshardingWorkflow with consistent snapshot.
func TestHorizontalReshardingWithConsistentSnapshot(t *testing.T) {
	testHorizontalReshardingWorkflow(t, true, "", "SplitDiff")
}

// TestHorizontalReshardingWithExcludedTables runs the happy path of HorizontalReshardingWorkflow with excluded tables.
func TestHorizontalReshardingWithExcludedTables(t *testing.T) {
	testHorizontalReshardingWorkflow(t, true, "table_a,table_b", "SplitDiff")
}

func TestHorizontalReshardingWithMultiDiffCommand(t *testing.T) {
	testHorizontalReshardingWorkflow(t, true, "table_a,table_b", "MultiSplitDiff")
}

func testHorizontalReshardingWorkflow(t *testing.T, useConsistentSnapshot bool, excludeTables, splitDiffCommand string) {
	ctx := context.Background()
	// Set up the mock wrangler. It is used for the CopySchema,
	// WaitforFilteredReplication and Migrate phase.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockWranglerInterface := setupMockWrangler(ctrl, testKeyspace)
	// Set up the fakeworkerclient. It is used at SplitClone and SplitDiff phase.
	fakeVtworkerClient := setupFakeVtworker(testKeyspace, testVtworkers, useConsistentSnapshot, excludeTables, splitDiffCommand)
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")
	// Initialize the topology.
	ts := setupTopology(ctx, t, testKeyspace)
	m := workflow.NewManager(ts)
	// Run the manager in the background.
	wg, _, cancel := workflow.StartManager(m)
	// Create the workflow.
	vtworkersParameter := testVtworkers + "," + testVtworkers
	args := []string{"-keyspace=" + testKeyspace, "-vtworkers=" + vtworkersParameter, "-phase_enable_approvals=", "-min_healthy_rdonly_tablets=2"}
	if useConsistentSnapshot {
		args = append(args, "-use_consistent_snapshot")
	}
	if excludeTables != "" {
		args = append(args, "-exclude_tables="+excludeTables)
	}
	args = append(args, "-source_shards=0", "-destination_shards=-80,80-", "-split_diff_cmd="+splitDiffCommand)
	uuid, err := m.Create(ctx, horizontalReshardingFactoryName, args)
	if err != nil {
		t.Fatalf("cannot create resharding workflow: %v", err)
	}
	// Inject the mock wranger into the workflow.
	w, err := m.WorkflowForTesting(uuid)
	if err != nil {
		t.Fatalf("fail to get workflow from manager: %v", err)
	}
	hw := w.(*horizontalReshardingWorkflow)
	hw.wr = mockWranglerInterface
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
	cancel()
	wg.Wait()
}

func setupFakeVtworker(keyspace, vtworkers string, useConsistentSnapshot bool, excludeTables, splitDiffCmd string) *fakevtworkerclient.FakeVtworkerClient {
	flag.Set("vtworker_client_protocol", "fake")
	fakeVtworkerClient := fakevtworkerclient.NewFakeVtworkerClient()
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, resetCommand(), "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, splitCloneCommand(keyspace, useConsistentSnapshot, excludeTables), "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, resetCommand(), "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, resetCommand(), "", nil)

	switch splitDiffCmd {
	case "SplitDiff":
		fakeVtworkerClient.RegisterResultForAddr(vtworkers, splitDiffCommand(keyspace, "-80", useConsistentSnapshot, excludeTables, splitDiffCmd), "", nil)
		fakeVtworkerClient.RegisterResultForAddr(vtworkers, splitDiffCommand(keyspace, "80-", useConsistentSnapshot, excludeTables, splitDiffCmd), "", nil)
	case "MultiSplitDiff":
		fakeVtworkerClient.RegisterResultForAddr(vtworkers, splitDiffCommand(keyspace, "0", useConsistentSnapshot, excludeTables, splitDiffCmd), "", nil)
		fakeVtworkerClient.RegisterResultForAddr(vtworkers, splitDiffCommand(keyspace, "0", useConsistentSnapshot, excludeTables, splitDiffCmd), "", nil)
	}
	return fakeVtworkerClient
}

func resetCommand() []string {
	return []string{"Reset"}
}

func splitCloneCommand(keyspace string, useConsistentSnapshot bool, excludeTables string) []string {
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets=2"}
	if useConsistentSnapshot {
		args = append(args, "--use_consistent_snapshot")
	}
	if excludeTables != "" {
		args = append(args, "--exclude_tables="+excludeTables)
	}

	args = append(args, keyspace+"/0")
	return args
}

func splitDiffCommand(keyspace string, shardId string, useConsistentSnapshot bool, excludeTables, splitDiffCommand string) []string {
	args := []string{splitDiffCommand}
	if useConsistentSnapshot {
		args = append(args, "--use_consistent_snapshot")
	}
	if excludeTables != "" {
		args = append(args, "--exclude_tables="+excludeTables)
	}

	switch splitDiffCommand {
	case "SplitDiff":
		args = append(args, "--min_healthy_rdonly_tablets=1", "--dest_tablet_type=RDONLY", keyspace+"/"+shardId)
	case "MultiSplitDiff":
		args = append(args, "--min_healthy_tablets=1", "--tablet_type=RDONLY", keyspace+"/"+shardId)
	}

	return args
}

func setupMockWrangler(ctrl *gomock.Controller, keyspace string) *MockReshardingWrangler {
	mockWranglerInterface := NewMockReshardingWrangler(ctrl)
	// Set the expected behaviors for mock wrangler.
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(gomock.Any(), nil /* tableArray*/, gomock.Any() /* excludeTableArray */, true /*includeViews*/, keyspace, "0", keyspace, "-80", wrangler.DefaultWaitSlaveTimeout).Return(nil)
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(gomock.Any(), nil /* tableArray*/, gomock.Any() /* excludeTableArray */, true /*includeViews*/, keyspace, "0", keyspace, "80-", wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().WaitForFilteredReplication(gomock.Any(), keyspace, "-80", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)
	mockWranglerInterface.EXPECT().WaitForFilteredReplication(gomock.Any(), keyspace, "80-", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)

	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		mockWranglerInterface.EXPECT().MigrateServedTypes(gomock.Any(), keyspace, "0", nil /* cells */, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime, false /* reverseReplication */).Return(nil)
	}
	return mockWranglerInterface
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
