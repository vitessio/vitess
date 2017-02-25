package resharding

import (
	"context"
	"flag"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/worker/fakevtworkerclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	testKeyspace  = "test_keyspace"
	testVtworkers = "localhost:15032"
)

// TestHorizontalResharding runs the happy path of HorizontalReshardingWorkflow.
func TestHorizontalResharding(t *testing.T) {
	ctx := context.Background()

	// Set up the mock wrangler. It is used for the CopySchema,
	// WaitforFilteredReplication and Migrate phase.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockWranglerInterface := setupMockWrangler(ctx, ctrl, testKeyspace)

	// Set up the fakeworkerclient. It is used at SplitClone and SplitDiff phase.
	fakeVtworkerClient := setupFakeVtworker(testKeyspace, testVtworkers)
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")

	// Initialize the topology.
	ts := setupTopology(ctx, t, testKeyspace)
	m := workflow.NewManager(ts)
	// Run the manager in the background.
	wg, cancel, _ := startManager(t, m)
	// Create the workflow.
	uuid, err := m.Create(context.Background(), horizontalReshardingFactoryName, []string{"-keyspace=" + testKeyspace, "-vtworkers=" + testVtworkers})
	if err != nil {
		t.Fatalf("cannot create testworkflow: %v", err)
	}
	// Inject the mock wranger into the workflow.
	w, err := m.GetWorkflowForTesting(uuid)
	if err != nil {
		t.Errorf("fail to get workflow from manager: %v", err)
	}
	hw := w.(*HorizontalReshardingWorkflow)
	hw.wr = mockWranglerInterface

	// Start the job.
	if err := m.Start(context.Background(), uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	// Wait for the workflow to end.
	m.Wait(context.Background(), uuid)
	verifyWorkflowSuccess(context.Background(), t, ts, uuid) // TODO: implemented in parallel_runner_test.

	// Stop the manager.
	if err := m.Stop(context.Background(), uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func setupFakeVtworker(keyspace, vtworkers string) *fakevtworkerclient.FakeVtworkerClient {
	flag.Set("vtworker_client_protocol", "fake")
	fakeVtworkerClient := fakevtworkerclient.NewFakeVtworkerClient()
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, []string{"SplitClone", "--min_healthy_rdonly_tablets=1", keyspace + "/0"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", keyspace + "/-80"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr(vtworkers, []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", keyspace + "/80-"}, "", nil)
	return fakeVtworkerClient
}

func setupMockWrangler(ctx context.Context, ctrl *gomock.Controller, keyspace string) *MockReshardingWrangler {
	// It is hard to set the expect call using the workflow's context since
	// it is only assigned when calling workflow.Run. Therefore, we only check
	// the type of the context argument in expect calls.
	managerCtx, mCancel := context.WithCancel(ctx)
	workflowCtx, wCancel := context.WithCancel(managerCtx)
	defer wCancel()
	defer mCancel()

	ctxMatcher := typeMatcher{workflowCtx}

	mockWranglerInterface := NewMockReshardingWrangler(ctrl)
	// Set the expected behaviors for mock wrangler.
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(ctxMatcher, nil /* tableArray*/, nil /* excludeTableArray */, true /*includeViews*/, keyspace, "0", keyspace, "-80", wrangler.DefaultWaitSlaveTimeout).Return(nil)
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(ctxMatcher, nil /* tableArray*/, nil /* excludeTableArray */, true /*includeViews*/, keyspace, "0", keyspace, "80-", wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().WaitForFilteredReplication(ctxMatcher, keyspace, "-80", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)
	mockWranglerInterface.EXPECT().WaitForFilteredReplication(ctxMatcher, keyspace, "80-", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)

	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		mockWranglerInterface.EXPECT().MigrateServedTypes(ctxMatcher, keyspace, "0", nil /* cells */, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime).Return(nil)
	}
	return mockWranglerInterface
}

func setupTopology(ctx context.Context, t *testing.T, keyspace string) topo.Server {
	ts := memorytopo.NewServer("cell")
	if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	createShard(ctx, t, ts.Impl, keyspace, "0", true)
	createShard(ctx, t, ts.Impl, keyspace, "-80", false)
	createShard(ctx, t, ts.Impl, keyspace, "80-", false)
	return ts
}

func createShard(ctx context.Context, t *testing.T, ts topo.Impl, keyspace, name string, isServing bool) {
	shard := &topodatapb.Shard{
		KeyRange: newKeyRange(name),
	}
	if isServing {
		servedTypes := map[topodatapb.TabletType]bool{
			topodatapb.TabletType_MASTER:  true,
			topodatapb.TabletType_REPLICA: true,
			topodatapb.TabletType_RDONLY:  true,
		}
		for st := range servedTypes {
			shard.ServedTypes = append(shard.ServedTypes, &topodatapb.Shard_ServedType{
				TabletType: st,
			})
		}
	}

	if err := ts.CreateShard(ctx, keyspace, name, shard); err != nil {
		t.Fatalf("CreateShard fails: %v", err)
	}
}

func newKeyRange(value string) *topodatapb.KeyRange {
	_, result, err := topo.ValidateShardName(value)
	if err != nil {
		panic(fmt.Sprintf("BUG: invalid shard name to initialize key range: %v", err))
	}
	return result
}

// typeMatcher implements the interface gomock.Matcher.
type typeMatcher struct {
	x interface{}
}

// Matches check whether the type of the variables are the same.
func (t typeMatcher) Matches(x interface{}) bool {
	return fmt.Sprintf("%T", t.x) == fmt.Sprintf("%T", x)
}

func (t typeMatcher) String() string {
	return fmt.Sprintf("has the same type with %v", t.x)
}
