package resharding

import (
	"context"
	"flag"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/worker/fakevtworkerclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

// TestHorizontalResharding runs the happy path of HorizontalReshardingWorkflow.
func TestHorizontalResharding(t *testing.T) {
	// Set up the mock wrangler. It is used for the CopySchema and Migrate phase.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	mockWranglerInterface := setupMockWrangler(ctx, ctrl)

	// Set up the fakeworkerclient. It is used at SplitClone and SplitDiff phase.
	fakeVtworkerClient := setupFakeVtworker()
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")

	// Create a checkpoint with initialized tasks.
	sourceShards := []string{"0"}
	destinationShards := []string{"-80", "80-"}
	vtworkers := []string{"localhost:15032"}
	checkpoint, err := initCheckpointFromShards("test_keyspace", vtworkers, sourceShards, destinationShards)
	if err != nil {
		t.Errorf("initialize checkpoint fails: %v", err)
	}

	hw, err := createWorkflow(ctx, mockWranglerInterface, checkpoint)
	if err != nil {
		t.Errorf("initialize Workflow fails: %v", err)
	}
	if err := hw.runWorkflow(); err != nil {
		t.Errorf("%s: Horizontal resharding workflow should not fail", err)
	}

	verifySuccess(t, hw.checkpoint)
}

// TestHorizontalReshardingRetry retries a stopped workflow,
// which the tasks are partially finished.
func TestHorizontalReshardingRetry(t *testing.T) {
	// Set up mock wrangler. It is used for the CopySchema and Migrate phase.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	mockWranglerInterface := setupMockWranglerForRetry(ctx, ctrl)

	// Set up fakeworkerclient. It is used at SplitClone and SplitDiff phase.
	fakeVtworkerClient := setupFakeVtworker()
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")

	// Create a checkpoint for the stopped workflow. For the stopped workflow,
	// the task of copying schema to shard 80- succeed while the task of copying
	// schema to shard -80 failed. The rest of tasks haven't been executed.
	sourceShards := []string{"0"}
	destinationShards := []string{"-80", "80-"}
	vtworkers := []string{"localhost:15032"}
	checkpoint, err := initCheckpointFromShards("test_keyspace", vtworkers, sourceShards, destinationShards)
	if err != nil {
		t.Errorf("initialize checkpoint fails: %v", err)
	}
	setTaskSuccessOrFailure(checkpoint, createTaskID(phaseCopySchema, "80-"), true /* isSuccess*/)
	setTaskSuccessOrFailure(checkpoint, createTaskID(phaseCopySchema, "-80"), false /* isSuccess*/)

	hw, err := createWorkflow(ctx, mockWranglerInterface, checkpoint)
	if err != nil {
		t.Errorf("initialize Workflow fails: %v", err)
	}
	// Rerunning the workflow.
	if err := hw.runWorkflow(); err != nil {
		t.Errorf("%s: Horizontal resharding workflow should not fail", err)
	}

	verifySuccess(t, hw.checkpoint)
}

func setTaskSuccessOrFailure(checkpoint *workflowpb.WorkflowCheckpoint, taskID string, isSuccess bool) {
	t := checkpoint.Tasks[taskID]
	t.State = workflowpb.TaskState_TaskDone
	if !isSuccess {
		t.Error = "failed"
	} else {
		t.Error = ""
	}
}

func createWorkflow(ctx context.Context, mockWranglerInterface *MockReshardingWrangler, checkpoint *workflowpb.WorkflowCheckpoint) (*HorizontalReshardingWorkflow, error) {
	ts := memorytopo.NewServer("cell")
	w := &workflowpb.Workflow{
		Uuid:        "test_hw",
		FactoryName: horizontalReshardingFactoryName,
		State:       workflowpb.WorkflowState_NotStarted,
	}
	wi, err := ts.CreateWorkflow(ctx, w)
	if err != nil {
		return nil, err
	}
	hw := &HorizontalReshardingWorkflow{
		ctx:              ctx,
		wr:               mockWranglerInterface,
		manager:          workflow.NewManager(ts),
		wi:               wi,
		topoServer:       ts,
		logger:           logutil.NewMemoryLogger(),
		checkpoint:       checkpoint,
		checkpointWriter: NewCheckpointWriter(ts, checkpoint, wi),
	}
	return hw, nil
}

func setupFakeVtworker() *fakevtworkerclient.FakeVtworkerClient {
	flag.Set("vtworker_client_protocol", "fake")
	fakeVtworkerClient := fakevtworkerclient.NewFakeVtworkerClient()
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitClone", "--min_healthy_rdonly_tablets=1", "test_keyspace/0"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/-80"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/80-"}, "", nil)
	return fakeVtworkerClient
}

func setupMockWranglerForRetry(ctx context.Context, ctrl *gomock.Controller) *MockReshardingWrangler {
	mockWranglerInterface := NewMockReshardingWrangler(ctrl)
	// Set the expected behaviors for mock wrangler. copy schema to shard 80-
	// should not be called.
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(
		ctx,
		nil,  /* tableArray*/
		nil,  /* excludeTableArray */
		true, /*includeViews*/
		"test_keyspace",
		"0",
		"test_keyspace",
		"-80",
		wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().WaitForFilteredReplication(ctx, "test_keyspace", "-80", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)
	mockWranglerInterface.EXPECT().WaitForFilteredReplication(ctx, "test_keyspace", "80-", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)

	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		mockWranglerInterface.EXPECT().MigrateServedTypes(
			ctx,
			"test_keyspace",
			"0",
			nil, /* cells */
			servedType,
			false, /* reverse */
			false, /* skipReFreshState */
			wrangler.DefaultFilteredReplicationWaitTime).Return(nil)
	}
	return mockWranglerInterface
}

func setupMockWrangler(ctx context.Context, ctrl *gomock.Controller) *MockReshardingWrangler {
	mockWranglerInterface := NewMockReshardingWrangler(ctrl)
	// Set the expected behaviors for mock wrangler.
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(
		ctx,
		nil,  /* tableArray*/
		nil,  /* excludeTableArray */
		true, /*includeViews*/
		"test_keyspace",
		"0",
		"test_keyspace",
		"-80",
		wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(
		ctx,
		nil,  /* tableArray*/
		nil,  /* excludeTableArray */
		true, /*includeViews*/
		"test_keyspace",
		"0",
		"test_keyspace",
		"80-",
		wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().WaitForFilteredReplication(ctx, "test_keyspace", "-80", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)
	mockWranglerInterface.EXPECT().WaitForFilteredReplication(ctx, "test_keyspace", "80-", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)

	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		mockWranglerInterface.EXPECT().MigrateServedTypes(
			ctx,
			"test_keyspace",
			"0",
			nil, /* cells */
			servedType,
			false, /* reverse */
			false, /* skipReFreshState */
			wrangler.DefaultFilteredReplicationWaitTime).Return(nil)
	}
	return mockWranglerInterface
}

func verifySuccess(t *testing.T, checkpoint *workflowpb.WorkflowCheckpoint) {
	for _, task := range checkpoint.Tasks {
		if task.State != workflowpb.TaskState_TaskDone || task.Error != "" {
			t.Fatalf("task: %v should succeed: task status: %v, %v", task.Id, task.State, task.Error)
		}
	}
}
