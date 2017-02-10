package resharding

import (
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

func TestHorizontalResharding(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hw := setUp(t, ctrl)
	if hw == nil {
		return
	}
	// Create fakeworkerclient, which is used for the unit test in steps SplitClone and SplitDiff.
	flag.Set("vtworker_client_protocol", "fake")
	fakeVtworkerClient := fakevtworkerclient.NewFakeVtworkerClient()
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")

	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitClone", "--min_healthy_rdonly_tablets=1", "test_keyspace/0"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/-80"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/80-"}, "", nil)

	// Test the execution of horizontal resharding.
	// To simply demonstate the ability to track task status and leverage it for control the workflow execution, only happy path is used here.
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

// setUp prepare the test environement for the happy path.
// Other test cases can reuse this basic setup and modified it based on its need.
func setUp(t *testing.T, ctrl *gomock.Controller) *HorizontalReshardingWorkflow {
	ts := memorytopo.NewServer("cell")
	// Create fake wrangler using mock interface, which is used for the unit test in steps CopySchema and MigratedServedType.
	mockWranglerInterface := NewMockReshardingWrangler(ctrl)
	// Create the checkpoint for workflow.
	keyspace := "test_keyspace"
	vtworkers := []string{"localhost:15032"}

	taskMap := make(map[string]*workflowpb.Task)
	source := "0"
	destinations := []string{"-80", "80-"}
	worker := vtworkers[0]
	updatePerSourceTask(keyspace, source, worker, SplitCloneName, taskMap)
	updatePerSourceTask(keyspace, source, worker, MigrateName, taskMap)

	for _, d := range destinations {
		updatePerDestinationTask(keyspace, source, d, worker, CopySchemaName, taskMap)
		updatePerDestinationTask(keyspace, source, d, worker, WaitFilteredReplicationName, taskMap)
		updatePerDestinationTask(keyspace, source, d, worker, SplitDiffName, taskMap)
	}

	// Create the workflow (ignore the node construction since we don't test the front-end part in this unit test).
	hw := &HorizontalReshardingWorkflow{
		wr:         mockWranglerInterface,
		topoServer: ts,
		logger:     logutil.NewMemoryLogger(),
		checkpoint: &workflowpb.WorkflowCheckpoint{
			CodeVersion: codeVersion,
			Tasks:       taskMap,
			Settings: map[string]string{
				"source_shards":      "0",
				"destination_shards": "-80,80-",
			},
		},
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

	// Set the expected behaviors for mock wrangler.
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(
		hw.ctx,
		nil,  /* tableArray*/
		nil,  /* excludeTableArray */
		true, /*includeViews*/
		"test_keyspace",
		"0",
		"test_keyspace",
		"-80",
		wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(
		hw.ctx,
		nil,  /* tableArray*/
		nil,  /* excludeTableArray */
		true, /*includeViews*/
		"test_keyspace",
		"0",
		"test_keyspace",
		"80-",
		wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().WaitForFilteredReplication(hw.ctx, "test_keyspace", "-80", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)
	mockWranglerInterface.EXPECT().WaitForFilteredReplication(hw.ctx, "test_keyspace", "80-", wrangler.DefaultWaitForFilteredReplicationMaxDelay).Return(nil)

	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		mockWranglerInterface.EXPECT().MigrateServedTypes(
			hw.ctx,
			"test_keyspace",
			"0",
			nil, /* cells */
			servedType,
			false, /* reverse */
			false, /* skipReFreshState */
			wrangler.DefaultFilteredReplicationWaitTime).Return(nil)
	}
	return hw
}

// TODO(yipeiw): fake a retry situation: fails first for made error, then fix the inserted bug and manually trigger the retry signal,
// verify whether the retrying job can be done successfully.
// problem for unit test: hard to fake action, node part, hard to separate the logic from front-end control. (figure out the call path of Init, s.t. we can create the front-end needed set-up if it is easy enough)
// problem for end-to-end test, need a way to check the workflow status; need to trigger the button through http request.
