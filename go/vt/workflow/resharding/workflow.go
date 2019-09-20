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

package resharding

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
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

const (
	codeVersion                                        = 2
	horizontalReshardingFactoryName                    = "horizontal_resharding"
	phaseCopySchema                 workflow.PhaseType = "copy_schema"
	phaseClone                      workflow.PhaseType = "clone"
	phaseWaitForFilteredReplication workflow.PhaseType = "wait_for_filtered_replication"
	phaseDiff                       workflow.PhaseType = "diff"
	phaseMigrateRdonly              workflow.PhaseType = "migrate_rdonly"
	phaseMigrateReplica             workflow.PhaseType = "migrate_replica"
	phaseMigrateMaster              workflow.PhaseType = "migrate_master"
)

// Register registers the HorizontalReshardingWorkflowFactory as a factory
// in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardingFactoryName, &Factory{})
}

// Factory is the factory to create
// a horizontal resharding workflow.
type Factory struct{}

// Init is part of the workflow.Factory interface.
func (*Factory) Init(m *workflow.Manager, w *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(horizontalReshardingFactoryName, flag.ContinueOnError)
	keyspace := subFlags.String("keyspace", "", "Name of keyspace to perform horizontal resharding")
	vtworkersStr := subFlags.String("vtworkers", "", "A comma-separated list of vtworker addresses")
	sourceShardsStr := subFlags.String("source_shards", "", "A comma-separated list of source shards")
	destinationShardsStr := subFlags.String("destination_shards", "", "A comma-separated list of destination shards")
	minHealthyRdonlyTablets := subFlags.String("min_healthy_rdonly_tablets", "1", "Minimum number of healthy RDONLY tablets required in source shards")
	splitCmd := subFlags.String("split_cmd", "SplitClone", "Split command to use to perform horizontal resharding (either SplitClone or LegacySplitClone)")
	splitDiffDestTabletType := subFlags.String("split_diff_dest_tablet_type", "RDONLY", "Specifies tablet type to use in destination shards while performing SplitDiff operation")
	phaseEnaableApprovalsDesc := fmt.Sprintf("Comma separated phases that require explicit approval in the UI to execute. Phase names are: %v", strings.Join(WorkflowPhases(), ","))
	phaseEnableApprovalsStr := subFlags.String("phase_enable_approvals", strings.Join(WorkflowPhases(), ","), phaseEnaableApprovalsDesc)
	useConsistentSnapshot := subFlags.Bool("use_consistent_snapshot", false, "Instead of pausing replication on the source, uses transactions with consistent snapshot to have a stable view of the data.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *keyspace == "" || *vtworkersStr == "" || *minHealthyRdonlyTablets == "" || *splitCmd == "" {
		return fmt.Errorf("keyspace name, min healthy rdonly tablets, split command, and vtworkers information must be provided for horizontal resharding")
	}

	vtworkers := strings.Split(*vtworkersStr, ",")
	sourceShards := strings.Split(*sourceShardsStr, ",")
	destinationShards := strings.Split(*destinationShardsStr, ",")
	phaseEnableApprovals := parsePhaseEnableApprovals(*phaseEnableApprovalsStr)
	for _, phase := range phaseEnableApprovals {
		validPhase := false
		for _, registeredPhase := range WorkflowPhases() {
			if phase == registeredPhase {
				validPhase = true
			}
		}
		if !validPhase {
			return fmt.Errorf("invalid phase in phase_enable_approvals: %v", phase)
		}
	}
	useConsistentSnapshotArg := ""
	if *useConsistentSnapshot {
		useConsistentSnapshotArg = "true"
	}

	err := validateWorkflow(m, *keyspace, vtworkers, sourceShards, destinationShards, *minHealthyRdonlyTablets)
	if err != nil {
		return err
	}

	w.Name = fmt.Sprintf("Reshard shards %v into shards %v of keyspace %v.", *keyspace, *sourceShardsStr, *destinationShardsStr)
	checkpoint, err := initCheckpoint(*keyspace, vtworkers, sourceShards, destinationShards, *minHealthyRdonlyTablets, *splitCmd, *splitDiffDestTabletType, useConsistentSnapshotArg)
	if err != nil {
		return err
	}

	checkpoint.Settings["phase_enable_approvals"] = *phaseEnableApprovalsStr

	w.Data, err = proto.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is part the workflow.Factory interface.
func (*Factory) Instantiate(m *workflow.Manager, w *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	rootNode.Message = "This is a workflow to execute horizontal resharding automatically."

	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(w.Data, checkpoint); err != nil {
		return nil, err
	}

	phaseEnableApprovals := make(map[string]bool)
	for _, phase := range parsePhaseEnableApprovals(checkpoint.Settings["phase_enable_approvals"]) {
		phaseEnableApprovals[phase] = true
	}

	hw := &horizontalReshardingWorkflow{
		checkpoint:           checkpoint,
		rootUINode:           rootNode,
		logger:               logutil.NewMemoryLogger(),
		wr:                   wrangler.New(logutil.NewConsoleLogger(), m.TopoServer(), tmclient.NewTabletManagerClient()),
		topoServer:           m.TopoServer(),
		manager:              m,
		phaseEnableApprovals: phaseEnableApprovals,
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

func createUINodes(rootNode *workflow.Node, phaseName workflow.PhaseType, shards []string) error {
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

// validateWorkflow validates that workflow has valid input parameters.
func validateWorkflow(m *workflow.Manager, keyspace string, vtworkers, sourceShards, destinationShards []string, minHealthyRdonlyTablets string) error {
	if len(sourceShards) == 0 || len(destinationShards) == 0 {
		return fmt.Errorf("invalid source or destination shards")
	}
	if len(vtworkers) != len(destinationShards) {
		return fmt.Errorf("there are %v vtworkers, %v destination shards: the number should be same", len(vtworkers), len(destinationShards))
	}

	splitRatio := len(destinationShards) / len(sourceShards)
	if minHealthyRdonlyTabletsVal, err := strconv.Atoi(minHealthyRdonlyTablets); err != nil || minHealthyRdonlyTabletsVal < splitRatio {
		return fmt.Errorf("there are not enough rdonly tablets in source shards. You need at least %v, it got: %v", splitRatio, minHealthyRdonlyTablets)
	}

	// find the OverlappingShards in the keyspace
	osList, err := topotools.FindOverlappingShards(context.Background(), m.TopoServer(), keyspace)
	if err != nil {
		return fmt.Errorf("cannot FindOverlappingShards in %v: %v", keyspace, err)
	}

	// find the shard we mentioned in there, if any
	os := topotools.OverlappingShardsForShard(osList, sourceShards[0])
	if os == nil {
		return fmt.Errorf("the specified source shard %v/%v is not in any overlapping shard", keyspace, sourceShards[0])
	}
	for _, sourceShard := range sourceShards {
		if !os.ContainsShard(sourceShard) {
			return fmt.Errorf("the specified source shard %v/%v is not in any overlapping shard", keyspace, sourceShard)
		}
	}
	for _, destinationShard := range destinationShards {
		if !os.ContainsShard(destinationShard) {
			return fmt.Errorf("the specified destination shard %v/%v is not in any overlapping shard", keyspace, destinationShard)
		}
	}
	return nil
}

// initCheckpoint initialize the checkpoint for the horizontal workflow.
func initCheckpoint(keyspace string, vtworkers, sourceShards, destinationShards []string, minHealthyRdonlyTablets, splitCmd, splitDiffDestTabletType string, useConsistentSnapshot string) (*workflowpb.WorkflowCheckpoint, error) {
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
			"keyspace":                   keyspace,
			"source_shard":               shard,
			"min_healthy_rdonly_tablets": minHealthyRdonlyTablets,
			"split_cmd":                  splitCmd,
			"vtworker":                   vtworkers[i],
			"use_consistent_snapshot":    useConsistentSnapshot,
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
			"keyspace":                keyspace,
			"destination_shard":       shard,
			"dest_tablet_type":        splitDiffDestTabletType,
			"vtworker":                vtworkers[i],
			"use_consistent_snapshot": useConsistentSnapshot,
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

func initTasks(tasks map[string]*workflowpb.Task, phase workflow.PhaseType, shards []string, getAttributes func(int, string) map[string]string) {
	for i, shard := range shards {
		taskID := createTaskID(phase, shard)
		tasks[taskID] = &workflowpb.Task{
			Id:         taskID,
			State:      workflowpb.TaskState_TaskNotStarted,
			Attributes: getAttributes(i, shard),
		}
	}
}

// horizontalReshardingWorkflow contains meta-information and methods to
// control the horizontal resharding workflow.
type horizontalReshardingWorkflow struct {
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
	checkpointWriter *workflow.CheckpointWriter

	phaseEnableApprovals map[string]bool
}

// Run executes the horizontal resharding process.
// It implements the workflow.Workflow interface.
func (hw *horizontalReshardingWorkflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.ctx = ctx
	hw.wi = wi
	hw.checkpointWriter = workflow.NewCheckpointWriter(hw.topoServer, hw.checkpoint, hw.wi)
	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(true /* updateChildren */)

	if err := hw.runWorkflow(); err != nil {
		return err
	}
	hw.setUIMessage(fmt.Sprintf("Horizontal Resharding is finished successfully."))
	return nil
}

func (hw *horizontalReshardingWorkflow) runWorkflow() error {
	copySchemaTasks := hw.GetTasks(phaseCopySchema)
	copySchemaRunner := workflow.NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, copySchemaTasks, hw.runCopySchema, workflow.Parallel, hw.phaseEnableApprovals[string(phaseCopySchema)])
	if err := copySchemaRunner.Run(); err != nil {
		return err
	}

	cloneTasks := hw.GetTasks(phaseClone)
	cloneRunner := workflow.NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, cloneTasks, hw.runSplitClone, workflow.Parallel, hw.phaseEnableApprovals[string(phaseClone)])
	if err := cloneRunner.Run(); err != nil {
		return err
	}

	waitForFilteredReplicationTasks := hw.GetTasks(phaseWaitForFilteredReplication)
	waitForFilteredReplicationRunner := workflow.NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, waitForFilteredReplicationTasks, hw.runWaitForFilteredReplication, workflow.Parallel, hw.phaseEnableApprovals[string(phaseWaitForFilteredReplication)])
	if err := waitForFilteredReplicationRunner.Run(); err != nil {
		return err
	}

	diffTasks := hw.GetTasks(phaseDiff)
	diffRunner := workflow.NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, diffTasks, hw.runSplitDiff, workflow.Parallel, hw.phaseEnableApprovals[string(phaseWaitForFilteredReplication)])
	if err := diffRunner.Run(); err != nil {
		return err
	}

	migrateRdonlyTasks := hw.GetTasks(phaseMigrateRdonly)
	migrateRdonlyRunner := workflow.NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, migrateRdonlyTasks, hw.runMigrate, workflow.Sequential, hw.phaseEnableApprovals[string(phaseMigrateRdonly)])
	if err := migrateRdonlyRunner.Run(); err != nil {
		return err
	}

	migrateReplicaTasks := hw.GetTasks(phaseMigrateReplica)
	migrateReplicaRunner := workflow.NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, migrateReplicaTasks, hw.runMigrate, workflow.Sequential, hw.phaseEnableApprovals[string(phaseMigrateReplica)])
	if err := migrateReplicaRunner.Run(); err != nil {
		return err
	}

	migrateMasterTasks := hw.GetTasks(phaseMigrateMaster)
	migrateMasterRunner := workflow.NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, migrateMasterTasks, hw.runMigrate, workflow.Sequential, hw.phaseEnableApprovals[string(phaseMigrateReplica)])
	return migrateMasterRunner.Run()
}

func (hw *horizontalReshardingWorkflow) setUIMessage(message string) {
	log.Infof("Horizontal resharding : %v.", message)
	hw.logger.Infof(message)
	hw.rootUINode.Log = hw.logger.String()
	hw.rootUINode.Message = message
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)
}

// WorkflowPhases returns phases for resharding workflow
func WorkflowPhases() []string {
	return []string{
		string(phaseCopySchema),
		string(phaseClone),
		string(phaseWaitForFilteredReplication),
		string(phaseDiff),
		string(phaseMigrateReplica),
		string(phaseMigrateRdonly),
		string(phaseMigrateMaster),
	}
}

func parsePhaseEnableApprovals(phaseEnableApprovalsStr string) []string {
	var phaseEnableApprovals []string
	if phaseEnableApprovalsStr == "" {
		return phaseEnableApprovals
	}
	phaseEnableApprovals = strings.Split(phaseEnableApprovalsStr, ",")
	for i, phase := range phaseEnableApprovals {
		phaseEnableApprovals[i] = strings.Trim(phase, " ")
	}
	return phaseEnableApprovals
}
