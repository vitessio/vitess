package resharding

import (
	"flag"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/worker/fakevtworkerclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
	statepb "github.com/youtube/vitess/go/vt/proto/workflowstate"
)

func TestHorizontalResharding(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	// Create fake wrangler using mock interface, which is used for the unit test in steps CopySchema and MigratedServedType.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockWranglerInterface := NewMockReshardingWrangler(ctrl)

	// Create the workflow (ignore the node construction since we don't test the front-end part in this unit test).
	hw := &HorizontalReshardingWorkflow{
		keyspace:   "test_keyspace",
		vtworkers:  []string{"localhost:15032"},
		wr:         mockWranglerInterface,
		topoServer: ts,
		logger:     logutil.NewMemoryLogger(),
	}

	perShard := &PerShardHorizontalResharding{
		PerShardHorizontalReshardingData: PerShardHorizontalReshardingData{
			Keyspace:          "test_keyspace",
			SourceShard:       "0",
			DestinationShards: []string{"-80", "80-"},
			Vtworker:          "localhost:15032",
		},
	}
	perShard.parent = hw
	hw.subWorkflows = append(hw.subWorkflows, perShard)

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

	// Create fakeworkerclient, which is used for the unit test in steps SplitClone and SplitDiff.
	fakeVtworkerClient := fakevtworkerclient.NewFakeVtworkerClient()
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")
	flag.Set("vtworker_client_protocol", "fake")
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitClone", "--min_healthy_rdonly_tablets=1", "test_keyspace/0"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/-80"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/80-"}, "", nil)

	// manually complete the task initalization, which is part of createSubWorkflows.
	// TODO(yipeiw): this code is repeated, should be removed from unit test.
	hw.subTasks = map[string]*statepb.TaskContainer{
		copySchemaTaskName:              new(CopySchemaTaskHelper).InitTasks(hw.subWorkflows),
		splitCloneTaskName:              new(SplitCloneTaskHelper).InitTasks(hw.subWorkflows),
		waitFilteredReplicationTaskName: new(WaitFilteredReplicationTaskHelper).InitTasks(hw.subWorkflows),
		splitDiffTaskName:               new(SplitDiffTaskHelper).InitTasks(hw.subWorkflows),
		migrateTaskName:                 new(MigrateTaskHelper).InitTasks(hw.subWorkflows),
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
	}

	// Test the execution of horizontal resharding.
	// To simply demonstate the ability to track task status and leverage it for control the workflow execution, only happy path is used here.
	if err := hw.runWorkflow(); err != nil {
		t.Errorf("%s: Horizontal resharding workflow should not fail", err)
	}
}
