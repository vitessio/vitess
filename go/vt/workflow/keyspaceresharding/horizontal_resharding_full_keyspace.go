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

package keyspaceresharding

// Package resharding contains a workflow for automatic horizontal resharding.
// The workflow assumes that there are as many vtworker processes running as source shards.
// Plus, these vtworker processes must be reachable via RPC.

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/workflow/resharding"

	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

const (
	codeVersion = 1

	horizontalReshardingFullKeyspaceFactoryName = "horizontal_resharding_full_keyspace"
	phaseName                                   = "create_workflows"
)

// Register registers the HorizontalReshardingFullKeyspaceFactory as a factory
// in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardingFullKeyspaceFactoryName, &HorizontalReshardingFullKeyspaceFactory{})
}

// HorizontalReshardingFullKeyspaceFactory is the factory to create
// a horizontal resharding workflow.
type HorizontalReshardingFullKeyspaceFactory struct{}

// Init is part of the workflow.Factory interface.
func (*HorizontalReshardingFullKeyspaceFactory) Init(m *workflow.Manager, w *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(horizontalReshardingFullKeyspaceFactoryName, flag.ContinueOnError)
	keyspace := subFlags.String("keyspace", "", "Name of keyspace to perform horizontal resharding")
	vtworkersStr := subFlags.String("vtworkers", "", "A comma-separated list of vtworker addresses")
	minHealthyRdonlyTablets := subFlags.String("min_healthy_rdonly_tablets", "1", "Minimum number of healthy RDONLY tablets required in source shards")
	splitCmd := subFlags.String("split_cmd", "SplitClone", "Split command to use to perform horizontal resharding (either SplitClone or LegacySplitClone)")
	splitDiffDestTabletType := subFlags.String("split_diff_dest_tablet_type", "RDONLY", "Specifies tablet type to use in destination shards while performing SplitDiff operation")
	enableApprovals := subFlags.Bool("enable_approvals", true, "If true, executions of tasks require user's approvals on the UI.")
	skipStartWorkflows := subFlags.Bool("skip_start_workflows", false, "If true, newly created workflows will have skip_start set")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *keyspace == "" || *vtworkersStr == "" || *minHealthyRdonlyTablets == "" || *splitCmd == "" {
		return fmt.Errorf("Keyspace name, min healthy rdonly tablets, split command, and vtworkers information must be provided for horizontal resharding")
	}

	vtworkers := strings.Split(*vtworkersStr, ",")

	w.Name = fmt.Sprintf("Full keyspace reshard on %s", *keyspace)
	shardsToSplit, err := findSourceAndDestinationShards(m.TopoServer(), *keyspace)
	if err != nil {
		return err
	}

	checkpoint, err := initCheckpoint(
		*keyspace,
		vtworkers,
		shardsToSplit,
		*minHealthyRdonlyTablets,
		*splitCmd,
		*splitDiffDestTabletType,
		*enableApprovals,
		*skipStartWorkflows,
	)
	if err != nil {
		return err
	}

	w.Data, err = proto.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is part the workflow.Factory interface.
func (*HorizontalReshardingFullKeyspaceFactory) Instantiate(m *workflow.Manager, w *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	rootNode.Message = "This is a workflow to execute horizontal resharding automatically."

	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(w.Data, checkpoint); err != nil {
		return nil, err
	}

	workflowsCount, err := strconv.Atoi(checkpoint.Settings["workflows_count"])
	if err != nil {
		return nil, err
	}

	hw := &HorizontalReshardingFullKeyspace{
		checkpoint:                   checkpoint,
		rootUINode:                   rootNode,
		logger:                       logutil.NewMemoryLogger(),
		topoServer:                   m.TopoServer(),
		manager:                      m,
		enableApprovalsParam:         checkpoint.Settings["enable_approvals"],
		skipStartWorkflowParam:       checkpoint.Settings["skip_start_workflows"],
		minHealthyRdonlyTabletsParam: checkpoint.Settings["min_healthy_rdonly_tablets"],
		keyspaceParam:                checkpoint.Settings["keyspace"],
		splitDiffDestTabletTypeParam: checkpoint.Settings["split_diff_dest_tablet_type"],
		splitCmdParam:                checkpoint.Settings["split_cmd"],
		workflowsCount:               workflowsCount,
	}
	createWorkflowsUINode := &workflow.Node{
		Name:     "CreateWorkflows",
		PathName: phaseName,
	}
	hw.rootUINode.Children = []*workflow.Node{
		createWorkflowsUINode,
	}

	phaseNode, err := rootNode.GetChildByPath(phaseName)
	if err != nil {
		return nil, fmt.Errorf("fails to find phase node for: %v", phaseName)
	}

	for i := 0; i < workflowsCount; i++ {
		taskID := fmt.Sprintf("%s/%v", phaseName, i)
		task := hw.checkpoint.Tasks[taskID]
		taskUINode := &workflow.Node{
			Name:     fmt.Sprintf("Create workflow to split shards %v to %v", task.Attributes["source_shards"], task.Attributes["destination_shards"]),
			PathName: fmt.Sprintf("%v", i),
		}
		phaseNode.Children = append(phaseNode.Children, taskUINode)
	}
	return hw, nil
}

func findSourceAndDestinationShards(ts *topo.Server, keyspace string) ([][][]string, error) {
	overlappingShards, err := topotools.FindOverlappingShards(context.Background(), ts, keyspace)
	if err != nil {
		return nil, err
	}

	var shardsToSplit [][][]string

	for _, os := range overlappingShards {
		var sourceShards, destinationShards []string
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
		shardsToSplit = append(shardsToSplit, [][]string{sourceShards, destinationShards})
	}
	return shardsToSplit, nil
}

// initCheckpoint initialize the checkpoint for full keyspace split
func initCheckpoint(keyspace string, vtworkers []string, shardsToSplit [][][]string, minHealthyRdonlyTablets, splitCmd, splitDiffDestTabletType string, enableApprovals, skipStartWorkflows bool) (*workflowpb.WorkflowCheckpoint, error) {
	sourceShards := 0
	destShards := 0
	for _, shardToSplit := range shardsToSplit {
		sourceShards = sourceShards + len(shardToSplit[0])
		destShards = destShards + len(shardToSplit[1])
	}
	if sourceShards == 0 || destShards == 0 {
		return nil, fmt.Errorf("invalid source or destination shards")
	}
	if len(vtworkers) != destShards {
		return nil, fmt.Errorf("there are %v vtworkers, %v destination shards: the number should be same", len(vtworkers), destShards)
	}

	splitRatio := destShards / sourceShards
	if minHealthyRdonlyTabletsVal, err := strconv.Atoi(minHealthyRdonlyTablets); err != nil || minHealthyRdonlyTabletsVal < splitRatio {
		return nil, fmt.Errorf("there are not enough rdonly tablets in source shards. You need at least %v, it got: %v", splitRatio, minHealthyRdonlyTablets)
	}

	tasks := make(map[string]*workflowpb.Task)
	usedVtworkersIdx := 0
	for i, shardToSplit := range shardsToSplit {
		taskID := fmt.Sprintf("%s/%v", phaseName, i)
		tasks[taskID] = &workflowpb.Task{
			Id:    taskID,
			State: workflowpb.TaskState_TaskNotStarted,
			Attributes: map[string]string{
				"source_shards":      strings.Join(shardToSplit[0], ","),
				"destination_shards": strings.Join(shardToSplit[1], ","),
				"vtworkers":          strings.Join(vtworkers[usedVtworkersIdx:usedVtworkersIdx+len(shardToSplit[1])], ","),
			},
		}
		usedVtworkersIdx = usedVtworkersIdx + len(shardToSplit[1])
	}
	return &workflowpb.WorkflowCheckpoint{
		CodeVersion: codeVersion,
		Tasks:       tasks,
		Settings: map[string]string{
			"vtworkers":                   strings.Join(vtworkers, ","),
			"min_healthy_rdonly_tablets":  minHealthyRdonlyTablets,
			"split_cmd":                   splitCmd,
			"split_diff_dest_tablet_type": splitDiffDestTabletType,
			"enable_approvals":            fmt.Sprintf("%v", enableApprovals),
			"skip_start_workflows":        fmt.Sprintf("%v", skipStartWorkflows),
			"workflows_count":             fmt.Sprintf("%v", len(shardsToSplit)),
			"keyspace":                    keyspace,
		},
	}, nil
}

// HorizontalReshardingFullKeyspace contains meta-information and methods to
// control the horizontal resharding workflow.
type HorizontalReshardingFullKeyspace struct {
	ctx        context.Context
	manager    *workflow.Manager
	topoServer *topo.Server
	wi         *topo.WorkflowInfo
	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode *workflow.Node

	checkpoint       *workflowpb.WorkflowCheckpoint
	checkpointWriter *resharding.CheckpointWriter

	workflowsCount int

	// params to horizontal reshard workflow
	enableApprovalsParam         string
	minHealthyRdonlyTabletsParam string
	keyspaceParam                string
	splitDiffDestTabletTypeParam string
	splitCmdParam                string
	skipStartWorkflowParam       string
}

// Run executes the horizontal resharding process.
// It implements the workflow.Workflow interface.
func (hw *HorizontalReshardingFullKeyspace) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.ctx = ctx
	hw.wi = wi
	hw.checkpointWriter = resharding.NewCheckpointWriter(hw.topoServer, hw.checkpoint, hw.wi)
	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(true /* updateChildren */)

	if err := hw.runWorkflow(); err != nil {
		return err
	}
	hw.setUIMessage(fmt.Sprintf("Horizontal Resharding is finished successfully."))
	return nil
}

func (hw *HorizontalReshardingFullKeyspace) runWorkflow() error {
	var tasks []*workflowpb.Task
	for i := 0; i < hw.workflowsCount; i++ {
		taskID := fmt.Sprintf("%s/%v", phaseName, i)
		tasks = append(tasks, hw.checkpoint.Tasks[taskID])
	}

	ctx := context.Background()
	skipStart, err := strconv.ParseBool(hw.skipStartWorkflowParam)
	log.Infof("This is skipStart")
	if err != nil {
		return err

	}
	for _, task := range tasks {
		horizontalReshardingParams := []string{
			"-keyspace=" + hw.keyspaceParam,
			"-vtworkers=" + task.Attributes["vtworkers"],
			"-enable_approvals=" + hw.enableApprovalsParam,
			"-split_cmd=" + hw.splitCmdParam,
			"-split_diff_dest_tablet_type=" + hw.splitDiffDestTabletTypeParam,
			"-min_healthy_rdonly_tablets=" + hw.minHealthyRdonlyTabletsParam,
			"-source_shards=" + task.Attributes["source_shards"],
			"-destination_shards=" + task.Attributes["destination_shards"],
			"-enable_approvals=" + hw.enableApprovalsParam,
		}
		log.Infof("This are the params %v", horizontalReshardingParams)
		uuid, err := hw.manager.Create(ctx, "horizontal_resharding", horizontalReshardingParams)

		if err != nil {
			return err
		}
		if !skipStart {
			err = hw.manager.Start(ctx, uuid)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (hw *HorizontalReshardingFullKeyspace) setUIMessage(message string) {
	log.Infof("Horizontal resharding : %v.", message)
	hw.rootUINode.Log = hw.logger.String()
	hw.rootUINode.Message = message
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)
}
