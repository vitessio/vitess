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

// Package resharding contains a workflow for automatic horizontal resharding.
// The workflow assumes that there are as many vtworker processes running as source shards.
// Plus, these vtworker processes must be reachable via RPC.

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"

	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	codeVersion = 1

	horizontalReshardingFactoryName = "horizontal_resharding"
)

// PhaseType is used to store the phase name in a workflow.
type PhaseType string

const (
	phaseCopySchema                 PhaseType = "copy_schema"
	phaseClone                      PhaseType = "clone"
	phaseWaitForFilteredReplication PhaseType = "wait_for_filtered_replication"
	phaseDiff                       PhaseType = "diff"
	phaseMigrateRdonly              PhaseType = "migrate_rdonly"
	phaseMigrateReplica             PhaseType = "migrate_replica"
	phaseMigrateMaster              PhaseType = "migrate_master"
)

// Register registers the HorizontalReshardingWorkflowFactory as a factory
// in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardingFactoryName, &HorizontalReshardingWorkflowFactory{})
}

// HorizontalReshardingWorkflowFactory is the factory to create
// a horizontal resharding workflow.
type HorizontalReshardingWorkflowFactory struct{}

// Init is part of the workflow.Factory interface.
func (*HorizontalReshardingWorkflowFactory) Init(m *workflow.Manager, w *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(horizontalReshardingFactoryName, flag.ContinueOnError)
	keyspace := subFlags.String("keyspace", "", "Name of keyspace to perform horizontal resharding")
	vtworkersStr := subFlags.String("vtworkers", "", "A comma-separated list of vtworker addresses")
	enableApprovals := subFlags.Bool("enable_approvals", true, "If true, executions of tasks require user's approvals on the UI.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *keyspace == "" || *vtworkersStr == "" {
		return fmt.Errorf("Keyspace name, vtworkers information must be provided for horizontal resharding")
	}

	vtworkers := strings.Split(*vtworkersStr, ",")
	w.Name = fmt.Sprintf("Horizontal resharding on keyspace %s", *keyspace)

	checkpoint, err := initCheckpoint(m.TopoServer(), *keyspace, vtworkers)
	if err != nil {
		return err
	}

	checkpoint.Settings["enable_approvals"] = fmt.Sprintf("%v", *enableApprovals)

	w.Data, err = proto.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is part the workflow.Factory interface.
func (*HorizontalReshardingWorkflowFactory) Instantiate(m *workflow.Manager, w *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	rootNode.Message = "This is a workflow to execute horizontal resharding automatically."

	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(w.Data, checkpoint); err != nil {
		return nil, err
	}

	enableApprovals, err := strconv.ParseBool(checkpoint.Settings["enable_approvals"])
	if err != nil {
		return nil, err
	}

	hw := &HorizontalReshardingWorkflow{
		checkpoint:      checkpoint,
		rootUINode:      rootNode,
		logger:          logutil.NewMemoryLogger(),
		wr:              wrangler.New(logutil.NewConsoleLogger(), m.TopoServer(), tmclient.NewTabletManagerClient()),
		topoServer:      m.TopoServer(),
		manager:         m,
		enableApprovals: enableApprovals,
	}
	copySchemaUINode := &workflow.Node{
		Name:     "CopySchemaShard",
		PathName: string(phaseCopySchema),
	}
	cloneUINode := &workflow.Node{
		Name:     "SplitClone",
		PathName: string(phaseClone),
	}
	waitForFilteredReplicationUINode := &workflow.Node{
		Name:     "WaitForFilteredReplication",
		PathName: string(phaseWaitForFilteredReplication),
	}
	diffUINode := &workflow.Node{
		Name:     "SplitDiff",
		PathName: string(phaseDiff),
	}
	migrateRdonlyUINode := &workflow.Node{
		Name:     "MigrateServedTypeRDONLY",
		PathName: string(phaseMigrateRdonly),
	}
	migrateReplicaUINode := &workflow.Node{
		Name:     "MigrateServedTypeREPLICA",
		PathName: string(phaseMigrateReplica),
	}
	migrateMasterUINode := &workflow.Node{
		Name:     "MigrateServedTypeMASTER",
		PathName: string(phaseMigrateMaster),
	}

	hw.rootUINode.Children = []*workflow.Node{
		copySchemaUINode,
		cloneUINode,
		waitForFilteredReplicationUINode,
		diffUINode,
		migrateRdonlyUINode,
		migrateReplicaUINode,
		migrateMasterUINode,
	}

	sourceShards := strings.Split(hw.checkpoint.Settings["source_shards"], ",")
	destinationShards := strings.Split(hw.checkpoint.Settings["destination_shards"], ",")

	if err := createUINodes(hw.rootUINode, phaseCopySchema, destinationShards); err != nil {
		return hw, err
	}
	if err := createUINodes(hw.rootUINode, phaseClone, sourceShards); err != nil {
		return hw, err
	}
	if err := createUINodes(hw.rootUINode, phaseWaitForFilteredReplication, destinationShards); err != nil {
		return hw, err
	}
	if err := createUINodes(hw.rootUINode, phaseDiff, destinationShards); err != nil {
		return hw, err
	}
	if err := createUINodes(hw.rootUINode, phaseMigrateRdonly, sourceShards); err != nil {
		return hw, err
	}
	if err := createUINodes(hw.rootUINode, phaseMigrateReplica, sourceShards); err != nil {
		return hw, err
	}
	if err := createUINodes(hw.rootUINode, phaseMigrateMaster, sourceShards); err != nil {
		return hw, err
	}

	return hw, nil
}

func createUINodes(rootNode *workflow.Node, phaseName PhaseType, shards []string) error {
	phaseNode, err := rootNode.GetChildByPath(string(phaseName))
	if err != nil {
		return fmt.Errorf("fails to find phase node for: %v", phaseName)
	}

	for _, shard := range shards {
		taskUINode := &workflow.Node{
			Name:     "Shard " + shard,
			PathName: shard,
		}
		phaseNode.Children = append(phaseNode.Children, taskUINode)
	}
	return nil
}

// initCheckpoint initialize the checkpoint for the horizontal workflow.
func initCheckpoint(ts *topo.Server, keyspace string, vtworkers []string) (*workflowpb.WorkflowCheckpoint, error) {
	sourceShards, destinationShards, err := findSourceAndDestinationShards(ts, keyspace)
	if err != nil {
		return nil, err
	}
	return initCheckpointFromShards(keyspace, vtworkers, sourceShards, destinationShards)
}

func findSourceAndDestinationShards(ts *topo.Server, keyspace string) ([]string, []string, error) {
	overlappingShards, err := topotools.FindOverlappingShards(context.Background(), ts, keyspace)
	if err != nil {
		return nil, nil, err
	}

	var sourceShards, destinationShards []string

	for _, os := range overlappingShards {
		var sourceShardInfo *topo.ShardInfo
		var destinationShardInfos []*topo.ShardInfo
		// Judge which side is source shard by checking the number of servedTypes.
		if len(os.Left[0].ServedTypes) > 0 {
			sourceShardInfo = os.Left[0]
			destinationShardInfos = os.Right
		} else {
			sourceShardInfo = os.Right[0]
			destinationShardInfos = os.Left
		}
		sourceShards = append(sourceShards, sourceShardInfo.ShardName())
		for _, d := range destinationShardInfos {
			destinationShards = append(destinationShards, d.ShardName())
		}
	}
	return sourceShards, destinationShards, nil
}

func initCheckpointFromShards(keyspace string, vtworkers, sourceShards, destinationShards []string) (*workflowpb.WorkflowCheckpoint, error) {
	if len(vtworkers) != len(sourceShards) {
		return nil, fmt.Errorf("there are %v vtworkers, %v source shards: the number should be same", len(vtworkers), len(sourceShards))
	}

	tasks := make(map[string]*workflowpb.Task)
	initTasks(tasks, phaseCopySchema, destinationShards, func(i int, shard string) map[string]string {
		return map[string]string{
			"keyspace":          keyspace,
			"source_shard":      sourceShards[0],
			"destination_shard": shard,
		}
	})
	initTasks(tasks, phaseClone, sourceShards, func(i int, shard string) map[string]string {
		return map[string]string{
			"keyspace":     keyspace,
			"source_shard": shard,
			"vtworker":     vtworkers[i],
		}
	})
	initTasks(tasks, phaseWaitForFilteredReplication, destinationShards, func(i int, shard string) map[string]string {
		return map[string]string{
			"keyspace":          keyspace,
			"destination_shard": shard,
		}
	})
	initTasks(tasks, phaseDiff, destinationShards, func(i int, shard string) map[string]string {
		return map[string]string{
			"keyspace":          keyspace,
			"destination_shard": shard,
			"vtworker":          vtworkers[0],
		}
	})
	initTasks(tasks, phaseMigrateRdonly, sourceShards, func(i int, shard string) map[string]string {
		return map[string]string{
			"keyspace":     keyspace,
			"source_shard": shard,
			"served_type":  topodatapb.TabletType_RDONLY.String(),
		}
	})
	initTasks(tasks, phaseMigrateReplica, sourceShards, func(i int, shard string) map[string]string {
		return map[string]string{
			"keyspace":     keyspace,
			"source_shard": shard,
			"served_type":  topodatapb.TabletType_REPLICA.String(),
		}
	})
	initTasks(tasks, phaseMigrateMaster, sourceShards, func(i int, shard string) map[string]string {
		return map[string]string{
			"keyspace":     keyspace,
			"source_shard": shard,
			"served_type":  topodatapb.TabletType_MASTER.String(),
		}
	})

	return &workflowpb.WorkflowCheckpoint{
		CodeVersion: codeVersion,
		Tasks:       tasks,
		Settings: map[string]string{
			"source_shards":      strings.Join(sourceShards, ","),
			"destination_shards": strings.Join(destinationShards, ","),
		},
	}, nil
}

func initTasks(tasks map[string]*workflowpb.Task, phase PhaseType, shards []string, getAttributes func(int, string) map[string]string) {
	for i, shard := range shards {
		taskID := createTaskID(phase, shard)
		tasks[taskID] = &workflowpb.Task{
			Id:         taskID,
			State:      workflowpb.TaskState_TaskNotStarted,
			Attributes: getAttributes(i, shard),
		}
	}
}

// HorizontalReshardingWorkflow contains meta-information and methods to
// control the horizontal resharding workflow.
type HorizontalReshardingWorkflow struct {
	ctx        context.Context
	wr         ReshardingWrangler
	manager    *workflow.Manager
	topoServer *topo.Server
	wi         *topo.WorkflowInfo
	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode *workflow.Node

	checkpoint       *workflowpb.WorkflowCheckpoint
	checkpointWriter *CheckpointWriter

	enableApprovals bool
}

// Run executes the horizontal resharding process.
// It implements the workflow.Workflow interface.
func (hw *HorizontalReshardingWorkflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.ctx = ctx
	hw.wi = wi
	hw.checkpointWriter = NewCheckpointWriter(hw.topoServer, hw.checkpoint, hw.wi)
	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(true /* updateChildren */)

	if err := hw.runWorkflow(); err != nil {
		return err
	}
	hw.setUIMessage(fmt.Sprintf("Horizontal Resharding is finished sucessfully."))
	return nil
}

func (hw *HorizontalReshardingWorkflow) runWorkflow() error {
	copySchemaTasks := hw.GetTasks(phaseCopySchema)
	copySchemaRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, copySchemaTasks, hw.runCopySchema, Parallel, hw.enableApprovals)
	if err := copySchemaRunner.Run(); err != nil {
		return err
	}

	cloneTasks := hw.GetTasks(phaseClone)
	cloneRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, cloneTasks, hw.runSplitClone, Parallel, hw.enableApprovals)
	if err := cloneRunner.Run(); err != nil {
		return err
	}

	waitForFilteredReplicationTasks := hw.GetTasks(phaseWaitForFilteredReplication)
	waitForFilteredReplicationRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, waitForFilteredReplicationTasks, hw.runWaitForFilteredReplication, Parallel, hw.enableApprovals)
	if err := waitForFilteredReplicationRunner.Run(); err != nil {
		return err
	}

	diffTasks := hw.GetTasks(phaseDiff)
	diffRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, diffTasks, hw.runSplitDiff, Sequential, hw.enableApprovals)
	if err := diffRunner.Run(); err != nil {
		return err
	}

	migrateRdonlyTasks := hw.GetTasks(phaseMigrateRdonly)
	migrateRdonlyRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, migrateRdonlyTasks, hw.runMigrate, Sequential, hw.enableApprovals)
	if err := migrateRdonlyRunner.Run(); err != nil {
		return err
	}

	migrateReplicaTasks := hw.GetTasks(phaseMigrateReplica)
	migrateReplicaRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, migrateReplicaTasks, hw.runMigrate, Sequential, hw.enableApprovals)
	if err := migrateReplicaRunner.Run(); err != nil {
		return err
	}

	migrateMasterTasks := hw.GetTasks(phaseMigrateMaster)
	migrateMasterRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, migrateMasterTasks, hw.runMigrate, Sequential, hw.enableApprovals)
	if err := migrateMasterRunner.Run(); err != nil {
		return err
	}

	return nil
}

func (hw *HorizontalReshardingWorkflow) setUIMessage(message string) {
	log.Infof("Horizontal resharding : %v.", message)
	hw.rootUINode.Log = hw.logger.String()
	hw.rootUINode.Message = message
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)
}
