/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reshardingworkflowgen

// This package contains a workflow to generate horizontal resharding workflows
// that automatically discovers available overlapping shards to split/merge.

import (
	"flag"
	"fmt"
	"path"
	"strconv"
	"strings"

	"context"

	"github.com/golang/protobuf/proto"

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

	keyspaceReshardingFactoryName = "hr_workflow_gen"
	phaseName                     = "create_workflows"
)

// Register registers the KeyspaceResharding as a factory
// in the workflow framework.
func Register() {
	workflow.Register(keyspaceReshardingFactoryName, &Factory{})
}

// Factory is the factory to create
// a horizontal resharding workflow.
type Factory struct{}

// Init is part of the workflow.Factory interface.
func (*Factory) Init(m *workflow.Manager, w *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(keyspaceReshardingFactoryName, flag.ContinueOnError)
	keyspace := subFlags.String("keyspace", "", "Name of keyspace to perform horizontal resharding")
	vtworkersStr := subFlags.String("vtworkers", "", "A comma-separated list of vtworker addresses")
	excludeTablesStr := subFlags.String("exclude_tables", "", "A comma-separated list of tables to exclude")
	minHealthyRdonlyTablets := subFlags.String("min_healthy_rdonly_tablets", "1", "Minimum number of healthy RDONLY tablets required in source shards")
	skipSplitRatioCheck := subFlags.Bool("skip_split_ratio_check", false, "Skip validation on minimum number of healthy RDONLY tablets")
	splitCmd := subFlags.String("split_cmd", "SplitClone", "Split command to use to perform horizontal resharding (either SplitClone or LegacySplitClone)")
	splitDiffCmd := subFlags.String("split_diff_cmd", "SplitDiff", "Split diff command to use to perform horizontal resharding (either SplitDiff or MultiSplitDiff)")
	splitDiffDestTabletType := subFlags.String("split_diff_dest_tablet_type", "RDONLY", "Specifies tablet type to use in destination shards while performing SplitDiff operation")
	skipStartWorkflows := subFlags.Bool("skip_start_workflows", true, "If true, newly created workflows will have skip_start set")
	phaseEnableApprovalsDesc := fmt.Sprintf("Comma separated phases that require explicit approval in the UI to execute. Phase names are: %v", strings.Join(resharding.WorkflowPhases(), ","))
	phaseEnableApprovalsStr := subFlags.String("phase_enable_approvals", strings.Join(resharding.WorkflowPhases(), ","), phaseEnableApprovalsDesc)
	useConsistentSnapshot := subFlags.Bool("use_consistent_snapshot", false, "Instead of pausing replication on the source, uses transactions with consistent snapshot to have a stable view of the data.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *keyspace == "" || *vtworkersStr == "" || *minHealthyRdonlyTablets == "" || *splitCmd == "" || *splitDiffCmd == "" {
		return fmt.Errorf("keyspace name, min healthy rdonly tablets, split command, and vtworkers information must be provided for horizontal resharding")
	}

	vtworkers := strings.Split(*vtworkersStr, ",")
	excludeTables := strings.Split(*excludeTablesStr, ",")

	w.Name = fmt.Sprintf("Keyspace reshard on %s", *keyspace)
	shardsToSplit, err := findSourceAndDestinationShards(m.TopoServer(), *keyspace)
	if err != nil {
		return err
	}

	checkpoint, err := initCheckpoint(
		*keyspace,
		vtworkers,
		excludeTables,
		shardsToSplit,
		*minHealthyRdonlyTablets,
		*splitCmd,
		*splitDiffCmd,
		*splitDiffDestTabletType,
		*phaseEnableApprovalsStr,
		*skipStartWorkflows,
		*useConsistentSnapshot,
		*skipSplitRatioCheck,
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
func (*Factory) Instantiate(m *workflow.Manager, w *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	rootNode.Message = "This is a workflow to execute a keyspace resharding automatically."

	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(w.Data, checkpoint); err != nil {
		return nil, err
	}

	workflowsCount, err := strconv.Atoi(checkpoint.Settings["workflows_count"])
	if err != nil {
		return nil, err
	}

	hw := &reshardingWorkflowGen{
		checkpoint:                   checkpoint,
		rootUINode:                   rootNode,
		logger:                       logutil.NewMemoryLogger(),
		topoServer:                   m.TopoServer(),
		manager:                      m,
		phaseEnableApprovalsParam:    checkpoint.Settings["phase_enable_approvals"],
		skipStartWorkflowParam:       checkpoint.Settings["skip_start_workflows"],
		minHealthyRdonlyTabletsParam: checkpoint.Settings["min_healthy_rdonly_tablets"],
		keyspaceParam:                checkpoint.Settings["keyspace"],
		splitDiffDestTabletTypeParam: checkpoint.Settings["split_diff_dest_tablet_type"],
		splitCmdParam:                checkpoint.Settings["split_cmd"],
		splitDiffCmdParam:            checkpoint.Settings["split_diff_cmd"],
		useConsistentSnapshot:        checkpoint.Settings["use_consistent_snapshot"],
		excludeTablesParam:           checkpoint.Settings["exclude_tables"],
		skipSplitRatioCheckParam:     checkpoint.Settings["skip_split_ratio_check"],
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
			Name:     fmt.Sprintf("Split shards %v to %v workflow creation", task.Attributes["source_shards"], task.Attributes["destination_shards"]),
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

		isLeftServing := os.Left[0].IsMasterServing
		if err != nil {
			return nil, err
		}
		if isLeftServing {
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

// initCheckpoint initialize the checkpoint for keyspace reshard
func initCheckpoint(keyspace string, vtworkers, excludeTables []string, shardsToSplit [][][]string, minHealthyRdonlyTablets, splitCmd, splitDiffCmd, splitDiffDestTabletType, phaseEnableApprovals string, skipStartWorkflows, useConsistentSnapshot, skipSplitRatioCheck bool) (*workflowpb.WorkflowCheckpoint, error) {
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
	if minHealthyRdonlyTabletsVal, err := strconv.Atoi(minHealthyRdonlyTablets); err != nil || (!skipSplitRatioCheck && minHealthyRdonlyTabletsVal < splitRatio) {
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
			"split_diff_cmd":              splitDiffCmd,
			"split_diff_dest_tablet_type": splitDiffDestTabletType,
			"phase_enable_approvals":      phaseEnableApprovals,
			"skip_start_workflows":        fmt.Sprintf("%v", skipStartWorkflows),
			"workflows_count":             fmt.Sprintf("%v", len(shardsToSplit)),
			"keyspace":                    keyspace,
			"use_consistent_snapshot":     fmt.Sprintf("%v", useConsistentSnapshot),
			"exclude_tables":              strings.Join(excludeTables, ","),
			"skip_split_ratio_check":      fmt.Sprintf("%v", skipSplitRatioCheck),
		},
	}, nil
}

// reshardingWorkflowGen contains meta-information and methods to
// control workflow.
type reshardingWorkflowGen struct {
	ctx        context.Context
	manager    *workflow.Manager
	topoServer *topo.Server
	wi         *topo.WorkflowInfo
	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode *workflow.Node

	checkpoint       *workflowpb.WorkflowCheckpoint
	checkpointWriter *workflow.CheckpointWriter

	workflowsCount int

	// params to horizontal reshard workflow
	phaseEnableApprovalsParam    string
	minHealthyRdonlyTabletsParam string
	keyspaceParam                string
	splitDiffDestTabletTypeParam string
	splitCmdParam                string
	splitDiffCmdParam            string
	skipStartWorkflowParam       string
	useConsistentSnapshot        string
	excludeTablesParam           string
	skipSplitRatioCheckParam     string
}

// Run implements workflow.Workflow interface. It creates one horizontal resharding workflow per shard to split
func (hw *reshardingWorkflowGen) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.ctx = ctx
	hw.wi = wi
	hw.checkpointWriter = workflow.NewCheckpointWriter(hw.topoServer, hw.checkpoint, hw.wi)
	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(true /* updateChildren */)

	if err := hw.runWorkflow(); err != nil {
		hw.setUIMessage(hw.rootUINode, fmt.Sprintf("Keyspace resharding failed to create workflows")) //nolint
		return err
	}
	hw.setUIMessage(hw.rootUINode, fmt.Sprintf("Keyspace resharding is finished successfully.")) //nolint
	return nil
}

func (hw *reshardingWorkflowGen) runWorkflow() error {
	var tasks []*workflowpb.Task
	for i := 0; i < hw.workflowsCount; i++ {
		taskID := fmt.Sprintf("%s/%v", phaseName, i)
		tasks = append(tasks, hw.checkpoint.Tasks[taskID])
	}

	workflowsCreator := workflow.NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, tasks, hw.workflowCreator, workflow.Sequential, false /*phaseEnableApprovals  we don't require enable approvals in this workflow*/)
	return workflowsCreator.Run()
}

func (hw *reshardingWorkflowGen) workflowCreator(ctx context.Context, task *workflowpb.Task) error {
	horizontalReshardingParams := []string{
		"-keyspace=" + hw.keyspaceParam,
		"-vtworkers=" + task.Attributes["vtworkers"],
		"-exclude_tables=" + hw.excludeTablesParam,
		"-skip_split_ratio_check=" + hw.skipSplitRatioCheckParam,
		"-split_cmd=" + hw.splitCmdParam,
		"-split_diff_cmd=" + hw.splitDiffCmdParam,
		"-split_diff_dest_tablet_type=" + hw.splitDiffDestTabletTypeParam,
		"-min_healthy_rdonly_tablets=" + hw.minHealthyRdonlyTabletsParam,
		"-source_shards=" + task.Attributes["source_shards"],
		"-destination_shards=" + task.Attributes["destination_shards"],
		"-phase_enable_approvals=" + hw.phaseEnableApprovalsParam,
		"-use_consistent_snapshot=" + hw.useConsistentSnapshot,
	}

	skipStart, err := strconv.ParseBool(hw.skipStartWorkflowParam)
	if err != nil {
		return err

	}
	phaseID := path.Dir(task.Id)
	phaseUINode, err := hw.rootUINode.GetChildByPath(phaseID)
	if err != nil {
		return err
	}

	uuid, err := hw.manager.Create(ctx, "horizontal_resharding", horizontalReshardingParams)
	if err != nil {
		hw.setUIMessage(phaseUINode, fmt.Sprintf("Couldn't create shard split workflow for source shards: %v. Got error: %v", task.Attributes["source_shards"], err))
		return err
	}
	hw.setUIMessage(phaseUINode, fmt.Sprintf("Created shard split workflow: %v for source shards: %v.", uuid, task.Attributes["source_shards"]))
	workflowCmd := "WorkflowCreate horizontal_resharding" + strings.Join(horizontalReshardingParams, " ")
	hw.setUIMessage(phaseUINode, fmt.Sprintf("Created workflow with the following params: %v", workflowCmd))
	if !skipStart {
		err = hw.manager.Start(ctx, uuid)
		if err != nil {
			hw.setUIMessage(phaseUINode, fmt.Sprintf("Couldn't start shard split workflow: %v for source shards: %v. Got error: %v", uuid, task.Attributes["source_shards"], err))
			return err
		}
	}
	return nil
}

func (hw *reshardingWorkflowGen) setUIMessage(node *workflow.Node, message string) {
	log.Infof("Keyspace resharding : %v.", message)
	hw.logger.Infof(message)
	node.Log = hw.logger.String()
	node.Message = message
	node.BroadcastChanges(false /* updateChildren */)
}
