package resharding

// Package resharding contains a workflow for automatic horizontal resharding.
// The workflow assumes that there are as many vtworker processes running as source shards.
// Plus, these vtworker processes must be reachable via RPC.
// TO DO: it can be used to save checkpointer

import (
	"flag"
	"fmt"
	"strings"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"

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

// HorizontalReshardingWorkflow contains meta-information and methods to
// control the horizontal resharding workflow.
type HorizontalReshardingWorkflow struct {
	ctx        context.Context
	wr         ReshardingWrangler
	manager    *workflow.Manager
	topoServer topo.Server
	wi         *topo.WorkflowInfo
	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode *workflow.Node

	checkpoint       *workflowpb.WorkflowCheckpoint
	checkpointWriter *CheckpointWriter
}

// Run executes the horizontal resharding process.
// It implements the workflow.Workflow interface.
func (hw *HorizontalReshardingWorkflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.ctx = ctx
	hw.topoServer = manager.TopoServer()
	hw.manager = manager
	hw.wr = wrangler.New(logutil.NewConsoleLogger(), manager.TopoServer(), tmclient.NewTabletManagerClient())
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
	copySchemaTasks := hw.GetTasks(hw.checkpoint, phaseCopySchema)
	copySchemaRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, copySchemaTasks, hw.runCopySchema, PARALLEL)
	if err := copySchemaRunner.Run(); err != nil {
		return err
	}

	cloneTasks := hw.GetTasks(hw.checkpoint, phaseClone)
	cloneRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, cloneTasks, hw.runSplitClone, PARALLEL)
	if err := cloneRunner.Run(); err != nil {
		return err
	}

	waitForFilteredReplicationTasks := hw.GetTasks(hw.checkpoint, phaseWaitForFilteredReplication)
	waitForFilteredReplicationRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, waitForFilteredReplicationTasks, hw.runWaitForFilteredReplication, PARALLEL)
	if err := waitForFilteredReplicationRunner.Run(); err != nil {
		return err
	}

	diffTasks := hw.GetTasks(hw.checkpoint, phaseDiff)
	diffRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, diffTasks, hw.runSplitDiff, SEQUENTIAL)
	if err := diffRunner.Run(); err != nil {
		return err
	}

	migrateRdonlyTasks := hw.GetTasks(hw.checkpoint, phaseMigrateRdonly)
	migrateRdonlyRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, migrateRdonlyTasks, hw.runMigrate, SEQUENTIAL)
	if err := migrateRdonlyRunner.Run(); err != nil {
		return err
	}

	migrateReplicaTasks := hw.GetTasks(hw.checkpoint, phaseMigrateReplica)
	migrateReplicaRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, migrateReplicaTasks, hw.runMigrate, SEQUENTIAL)
	if err := migrateReplicaRunner.Run(); err != nil {
		return err
	}

	migrateMasterTasks := hw.GetTasks(hw.checkpoint, phaseMigrateMaster)
	migrateMasterRunner := NewParallelRunner(hw.ctx, hw.rootUINode, hw.checkpointWriter, migrateMasterTasks, hw.runMigrate, SEQUENTIAL)
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

// Register registers the HorizontalReshardingWorkflowFactory as a factory
// in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardingFactoryName, &HorizontalReshardingWorkflowFactory{})
}

// HorizontalReshardingWorkflowFactory is the factory to create
// a horizontal resharding workflow.
type HorizontalReshardingWorkflowFactory struct{}

// Init is part of the workflow.Factory interface.
func (*HorizontalReshardingWorkflowFactory) Init(w *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(horizontalReshardingFactoryName, flag.ContinueOnError)
	keyspace := subFlags.String("keyspace", "", "Name of keyspace to perform horizontal resharding")
	vtworkersStr := subFlags.String("vtworkers", "", "A comma-separated list of vtworker addresses")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *keyspace == "" || *vtworkersStr == "" {
		return fmt.Errorf("Keyspace name, vtworkers information must be provided for horizontal resharding")
	}

	vtworkers := strings.Split(*vtworkersStr, ",")
	w.Name = fmt.Sprintf("Horizontal resharding on keyspace %s", *keyspace)

	ts := topo.Open()
	defer ts.Close()
	checkpoint, err := initCheckpoint(*keyspace, vtworkers, ts)
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
func (*HorizontalReshardingWorkflowFactory) Instantiate(w *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	rootNode.Message = "This is a workflow to execute horizontal resharding automatically."

	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(w.Data, checkpoint); err != nil {
		return nil, err
	}

	hw := &HorizontalReshardingWorkflow{
		checkpoint: checkpoint,
		rootUINode: rootNode,
		logger:     logutil.NewMemoryLogger(),
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

	destinationShards := strings.Split(hw.checkpoint.Settings["destination_shards"], ",")
	sourceShards := strings.Split(hw.checkpoint.Settings["source_shards"], ",")

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
func initCheckpoint(keyspace string, vtworkers []string, ts topo.Server) (*workflowpb.WorkflowCheckpoint, error) {
	sourceShardList, destinationShardList, err := findSourceAndDestinationShards(ts, keyspace)
	if err != nil {
		return nil, err
	}
	return initCheckpointFromShards(keyspace, vtworkers, sourceShardList, destinationShardList)
}

func findSourceAndDestinationShards(ts topo.Server, keyspace string) ([]string, []string, error) {
	overlappingShards, err := topotools.FindOverlappingShards(context.Background(), ts, keyspace)
	if err != nil {
		return nil, nil, err
	}

	var sourceShardList, destinationShardList []string

	for _, os := range overlappingShards {
		var sourceShard *topo.ShardInfo
		var destinationShards []*topo.ShardInfo
		// Judge which side is source shard by checking the number of servedTypes.
		if len(os.Left[0].ServedTypes) > 0 {
			sourceShard = os.Left[0]
			destinationShards = os.Right
		} else {
			sourceShard = os.Right[0]
			destinationShards = os.Left
		}
		sourceShardList = append(sourceShardList, sourceShard.ShardName())
		for _, d := range destinationShards {
			destinationShardList = append(destinationShardList, d.ShardName())
		}
	}
	return sourceShardList, destinationShardList, nil
}

func initCheckpointFromShards(keyspace string, vtworkers, sourceShardList, destinationShardList []string) (*workflowpb.WorkflowCheckpoint, error) {
	taskMap := make(map[string]*workflowpb.Task)
	initTasks(phaseCopySchema, destinationShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"source_shard":      sourceShardList[0],
			"destination_shard": shard,
			"keyspace":          keyspace,
		}
	})

	initTasks(phaseClone, sourceShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"source_shard": shard,
			"vtworker":     vtworkers[i],
			"keyspace":     keyspace,
		}
	})

	initTasks(phaseWaitForFilteredReplication, destinationShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"destination_shard": shard,
			"keyspace":          keyspace,
		}
	})

	initTasks(phaseDiff, destinationShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"destination_shard": shard,
			"keyspace":          keyspace,
			"vtworker":          vtworkers[0],
		}
	})

	initTasks(phaseMigrateRdonly, sourceShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"source_shard": shard,
			"keyspace":     keyspace,
			"served_type":  topodatapb.TabletType_RDONLY.String(),
		}
	})
	initTasks(phaseMigrateReplica, sourceShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"source_shard": shard,
			"keyspace":     keyspace,
			"served_type":  topodatapb.TabletType_REPLICA.String(),
		}
	})
	initTasks(phaseMigrateMaster, sourceShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"source_shard": shard,
			"keyspace":     keyspace,
			"served_type":  topodatapb.TabletType_MASTER.String(),
		}
	})

	return &workflowpb.WorkflowCheckpoint{
		CodeVersion: codeVersion,
		Tasks:       taskMap,
		Settings: map[string]string{
			"source_shards":      strings.Join(sourceShardList, ","),
			"destination_shards": strings.Join(destinationShardList, ","),
		},
	}, nil
}

func initTasks(phase PhaseType, shards []string, taskMap map[string]*workflowpb.Task, getAttributes func(int, string) map[string]string) {
	for i, shard := range shards {
		taskID := createTaskID(phase, shard)
		taskMap[taskID] = &workflowpb.Task{
			Id:         taskID,
			State:      workflowpb.TaskState_TaskNotStarted,
			Attributes: getAttributes(i, shard),
		}
	}
}
