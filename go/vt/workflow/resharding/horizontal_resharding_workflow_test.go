package resharding

import (
	"context"
	"flag"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/worker/fakevtworkerclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

// TestHorizontalResharding runs resharding from 1 shard to 2 shards.
func TestHorizontalResharding(t *testing.T) {
	// Initialize the checkpoint for the workflow.
	oneShard := ReshardingData{
		Keyspace:          "test_keyspace",
		SourceShard:       "0",
		DestinationShards: []string{"-80", "80-"},
		Vtworker:          "localhost:15032",
	}
	initCp := createCheckpoint([]ReshardingData{oneShard})

	// Create the horizontal resharding workflow.
	hw := setupWorkflow(t, initCp)
	if hw == nil {
		return
	}
	// Create the mock wrangler and set the expected behavior.
	// Then pass it to the workflow.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	hw.wr = setupMockWrangler(hw.ctx, ctrl)

	// Set up the fake vtworkerclient.
	fakeVtworkerClient := setupFakeVtworker()
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")

	// Run the workflow.
	if err := hw.runWorkflow(); err != nil {
		t.Errorf("%s: Horizontal resharding workflow should not fail", err)
	}

	// Checking all tasks are Done.
	for _, task := range hw.checkpoint.Tasks {
		if task.State != workflowpb.TaskState_TaskDone || task.Error != "" {
			t.Fatalf("task is not done: Id: %v, State: %v, Attributes:%v", task.Id, task.State, task.Attributes)
		}
	}
}

// TestHorizontalReshardingRestart restarts a stopped worklow
// by loading a hand-crafted checkpoint. This checkpoint is used to fake
// the one saved by the killed workflow. It records that some tasks
// in the workflow are finished successfully.
func TestHorizontalReshardingRestart(t *testing.T) {
	// Initialize the checkpoint for the workflow.
	oneShard := ReshardingData{
		Keyspace:          "test_keyspace",
		SourceShard:       "0",
		DestinationShards: []string{"-80", "80-"},
		Vtworker:          "localhost:15032",
	}
	initCp := createCheckpoint([]ReshardingData{oneShard})

	// Set checkpoint to record that the copySchemaTask on destination shard
	// "-80" succeeded.
	t1 := initCp.Tasks[createTaskID(copySchemaName, "dest", "-80")]
	t1.State = workflowpb.TaskState_TaskDone
	// Set checkpoint to record that the copySchemaTask on destination shard
	// "80-" failed with errors.
	t2 := initCp.Tasks[createTaskID(copySchemaName, "dest", "80-")]
	t2.State = workflowpb.TaskState_TaskDone
	t2.Error = "the task CopySchema for shard 80- fails."

	// Create the workflow proto message, which will be loaded
	// when restarting the stopped workflow.
	workflowProto := &workflowpb.Workflow{
		Uuid:        "testworkflow0000",
		FactoryName: "horizontal_resharding",
		State:       workflowpb.WorkflowState_Running,
	}
	data, err := proto.Marshal(initCp)
	if err != nil {
		t.Errorf("error in encoding checkpoint proto message: %v", err)
	}
	workflowProto.Data = data

	nodeManager := workflow.NewNodeManager()
	rootNode := &workflow.Node{
		PathName: "test_root",
		Name:     "root",
	}
	if err := nodeManager.AddRootNode(rootNode); err != nil {
		t.Errorf("adding root node failed: %v", err)
	}

	// The workflow is created using Instantiate method when it is restarted.
	var factory *HorizontalReshardingWorkflowFactory
	w, err := factory.Instantiate(workflowProto, rootNode)
	if err != nil {
		t.Errorf("horizontal resharding workflow not instantiated successfully")
	}
	hw := w.(*HorizontalReshardingWorkflow)

	ts := memorytopo.NewServer("cell")
	wi, err := ts.CreateWorkflow(context.TODO(), workflowProto)
	if err != nil {
		t.Errorf("creating workflow fails: %v", err)
	}
	hw.ctx = context.Background()
	hw.topoServer = ts
	hw.wi = wi
	hw.checkpointWriter = NewCheckpointWriter(hw.topoServer, hw.checkpoint, hw.wi)
	if err := hw.checkpointWriter.Save(); err != nil {
		t.Errorf("checkpointWriter save fails: %v", err)
	}

	// Create the mock wrangler and set the expected behavior.
	// Then pass it to the workflow.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	hw.wr = setupMockWranglerRestart(hw.ctx, ctrl)

	// Set up the fake vtworkerclient.
	fakeVtworkerClient := setupFakeVtworker()
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")

	// Run the workflow.
	if err := hw.runWorkflow(); err != nil {
		t.Errorf("%s: Horizontal resharding workflow should not fail", err)
	}

	// Checking all tasks are Done.
	for _, task := range hw.checkpoint.Tasks {
		if task.State != workflowpb.TaskState_TaskDone || task.Error != "" {
			t.Fatalf("task is not done: Id: %v, State: %v, Attributes:%v", task.Id, task.State, task.Attributes)
		}
	}
}

func setupFakeVtworker() *fakevtworkerclient.FakeVtworkerClient {
	// Create fakeworkerclient, which is used for the unit test in phase of
	// SplitClone and SplitDiff.
	flag.Set("vtworker_client_protocol", "fake")
	fakeVtworkerClient := fakevtworkerclient.NewFakeVtworkerClient()
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitClone", "--min_healthy_rdonly_tablets=1", "test_keyspace/0"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/-80"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/80-"}, "", nil)
	return fakeVtworkerClient
}

// setUpWorkflow prepares the test environement for the happy path.
func setupWorkflow(t *testing.T, initCheckpoint *workflowpb.WorkflowCheckpoint) *HorizontalReshardingWorkflow {
	ts := memorytopo.NewServer("cell")
	// Create fake wrangler using mock interface, which is used for the unit test in steps CopySchema and MigratedServedType.

	hw := &HorizontalReshardingWorkflow{
		topoServer:    ts,
		logger:        logutil.NewMemoryLogger(),
		checkpoint:    initCheckpoint,
		taskUINodeMap: make(map[string]*workflow.Node),
	}

	// Create the initial workflowpb.Workflow object.
	w := &workflowpb.Workflow{
		Uuid:        "testworkflow0000",
		FactoryName: "horizontal_resharding",
		State:       workflowpb.WorkflowState_NotStarted,
	}
	var err error
	hw.wi, err = hw.topoServer.CreateWorkflow(hw.ctx, w)
	if err != nil {
		t.Errorf("%s: Horizontal resharding workflow fails in creating workflowInfo", err)
		return nil
	}
	hw.checkpointWriter = NewCheckpointWriter(hw.topoServer, hw.checkpoint, hw.wi)
	return hw
}

// setupMockWrangler sets the expected behaviors for mock wrangler.
func setupMockWrangler(ctx context.Context, ctrl *gomock.Controller) *MockReshardingWrangler {
	mockWranglerInterface := NewMockReshardingWrangler(ctrl)
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

func setupMockWranglerRestart(ctx context.Context, ctrl *gomock.Controller) *MockReshardingWrangler {
	// Set the mock wrangler expectations without the call of copyschema
	// on shard "-80". That task is supposed to be finished
	// and must not be called when restarting the workflow.
	mockWranglerInterface := NewMockReshardingWrangler(ctrl)
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

// ReshardingData stores the data for resharding one source shard.
type ReshardingData struct {
	Keyspace          string
	SourceShard       string
	DestinationShards []string
	Vtworker          string
}

func createCheckpoint(data []ReshardingData) *workflowpb.WorkflowCheckpoint {
	taskMap := make(map[string]*workflowpb.Task)
	var sourceList, destinationList []string

	for _, info := range data {
		keyspace := info.Keyspace
		s := info.SourceShard
		worker := info.Vtworker
		sourceList = append(sourceList, s)
		updatePerSourceTask(keyspace, s, worker, splitCloneName, taskMap)
		updatePerSourceTask(keyspace, s, worker, migrateName, taskMap)
		for _, d := range info.DestinationShards {
			destinationList = append(destinationList, d)
			updatePerDestinationTask(keyspace, s, d, worker, copySchemaName, taskMap)
			updatePerDestinationTask(keyspace, s, d, worker, waitFilteredReplicationName, taskMap)
			updatePerDestinationTask(keyspace, s, d, worker, splitDiffName, taskMap)
		}
	}

	return &workflowpb.WorkflowCheckpoint{
		CodeVersion: codeVersion,
		Tasks:       taskMap,
		Settings: map[string]string{
			"source_shards":      strings.Join(sourceList, ","),
			"destination_shards": strings.Join(destinationList, ","),
		},
	}
}
