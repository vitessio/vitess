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

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/worker/fakevtworkerclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"

	// import the gRPC client implementation for tablet manager
	_ "github.com/youtube/vitess/go/vt/vttablet/grpctmclient"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	testKeyspace  = "test_keyspace"
	testVtworkers = "localhost:15032"
)

func init() {
	Register()
}

// TestHorizontalResharding runs the happy path of HorizontalReshardingWorkflow.
func TestHorizontalResharding(t *testing.T) {
	ctx := context.Background()

	// Set up the mock wrangler. It is used for the CopySchema,
	// WaitforFilteredReplication and Migrate phase.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockWranglerInterface := setupMockWrangler(ctrl, testKeyspace)

	// Set up the fakeworkerclient. It is used at SplitClone and SplitDiff phase.
	fakeVtworkerClient := setupFakeVtworker(testKeyspace, testVtworkers)
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")

	// Initialize the topology.
	ts := setupTopology(ctx, t, testKeyspace)
	m := workflow.NewManager(ts)
	// Run the manager in the background.
	wg, _, cancel := startManager(m)
	// Create the workflow.
	uuid, err := m.Create(ctx, horizontalReshardingFactoryName, []string{"-keyspace=" + testKeyspace, "-vtworkers=" + testVtworkers, "-enable_approvals=false"})
	if err != nil {
		t.Fatalf("cannot create resharding workflow: %v", err)
	}
	// Inject the mock wranger into the workflow.
	w, err := m.WorkflowForTesting(uuid)
	if err != nil {
		t.Fatalf("fail to get workflow from manager: %v", err)
	}
	hw := w.(*HorizontalReshardingWorkflow)
	hw.wr = mockWranglerInterface

	// Start the job.
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start resharding workflow: %v", err)
	}

	// Wait for the workflow to end.
	m.Wait(ctx, uuid)
	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}

	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop resharding workflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func setupFakeVtworker(keyspace, vtworkers string) *fakevtworkerclient.FakeVtworkerClient {
	flag.Set("vtworker_client_protocol", "fake")
	fakeVtworkerClient := fakevtworkerclient.NewFakeVtworkerClient()
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, []string{"Reset"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, []string{"SplitClone", "--min_healthy_rdonly_tablets=1", keyspace + "/0"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, []string{"Reset"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", keyspace + "/-80"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, []string{"Reset"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", keyspace + "/80-"}, "", nil)
	return fakeVtworkerClient
}

func setupMockWrangler(ctrl *gomock.Controller, keyspace string) *MockReshardingWrangler {
	mockWranglerInterface := NewMockReshardingWrangler(ctrl)
	// Set the expected behaviors for mock wrangler.
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(gomock.Any(), nil /* tableArray*/, nil /* excludeTableArray */, true /*includeViews*/, keyspace, "0", keyspace, "-80", wrangler.DefaultWaitSlaveTimeout).Return(nil)
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(gomock.Any(), nil /* tableArray*/, nil /* excludeTableArray */, true /*includeViews*/, keyspace, "0", keyspace, "80-", wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().WaitForFilteredReplication(gomock.Any(), keyspace, "-80", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)
	mockWranglerInterface.EXPECT().WaitForFilteredReplication(gomock.Any(), keyspace, "80-", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)

	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		mockWranglerInterface.EXPECT().MigrateServedTypes(gomock.Any(), keyspace, "0", nil /* cells */, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime).Return(nil)
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
