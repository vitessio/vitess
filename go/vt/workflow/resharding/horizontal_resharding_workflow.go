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

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	codeVersion = 1

	horizontalReshardingFactoryName = "horizontal_resharding"

	CopySchemaName              = "copy_schema"
	SplitCloneName              = "clone"
	WaitFilteredReplicationName = "wait_replication"
	SplitDiffName               = "diff"
	MigrateName                 = "migrate"
)

// HorizontalReshardingData is the data structure to store resharding arguments.
type HorizontalReshardingData struct {
	Keyspace  string
	Vtworkers []string
}

// HorizontalReshardingWorkflow contains meta-information and methods to control horizontal resharding workflow.
type HorizontalReshardingWorkflow struct {
	// ctx is the context of the whole horizontal resharding process. Once this context is canceled,
	// the horizontal resharding process stops.
	ctx        context.Context
	wr         ReshardingWrangler
	manager    *workflow.Manager
	topoServer topo.Server
	wi         *topo.WorkflowInfo
	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode                    *workflow.Node
	copySchemaUINode              *workflow.Node
	splitCloneUINode              *workflow.Node
	waitFilteredReplicationUINode *workflow.Node
	splitDiffUINode               *workflow.Node
	migrateUINode                 *workflow.Node
	taskUINodeMap                 map[string]*workflow.Node

	checkpoint       *workflowpb.WorkflowCheckpoint
	checkpointWriter *CheckpointWriter
}

// Run executes the horizontal resharding process and updates the UI message.
// It implements the workflow.Workflow interface.
func (hw *HorizontalReshardingWorkflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.ctx = ctx
	hw.topoServer = manager.TopoServer()
	hw.wr = wrangler.New(logutil.NewConsoleLogger(), manager.TopoServer(), tmclient.NewTabletManagerClient())
	hw.wi = wi
	hw.checkpointWriter = NewCheckpointWriter(hw.topoServer, hw.checkpoint, hw.wi)
	if err := hw.checkpointWriter.Save(); err != nil {
		return err
	}
	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(true /* updateChildren */)

	if err := hw.runWorkflow(); err != nil {
		hw.setUIMessage(fmt.Sprintf("Horizontal Resharding failed: %v", err))
		return err
	}
	hw.setUIMessage(fmt.Sprintf("Horizontal Resharding is finished sucessfully."))
	return nil
}

func (hw *HorizontalReshardingWorkflow) runWorkflow() error {
	copyTasks := hw.GenerateTasks(hw.checkpoint, CopySchemaName)
	copyRunner := NewParallelRunner(hw.ctx, hw.checkpointWriter, hw.taskUINodeMap, copyTasks)
	if err := copyRunner.Run(hw.runCopySchema, PARALLEL); err != nil {
		return err
	}

	cloneTasks := hw.GenerateTasks(hw.checkpoint, SplitCloneName)
	cloneRunner := NewParallelRunner(hw.ctx, hw.checkpointWriter, hw.taskUINodeMap, cloneTasks)
	if err := cloneRunner.Run(hw.runSplitClone, PARALLEL); err != nil {
		return err
	}

	waitTasks := hw.GenerateTasks(hw.checkpoint, WaitFilteredReplicationName)
	waitRunner := NewParallelRunner(hw.ctx, hw.checkpointWriter, hw.taskUINodeMap, waitTasks)
	if err := waitRunner.Run(hw.runWaitFilteredReplication, PARALLEL); err != nil {
		return err
	}

	diffTasks := hw.GenerateTasks(hw.checkpoint, SplitDiffName)
	diffRunner := NewParallelRunner(hw.ctx, hw.checkpointWriter, hw.taskUINodeMap, diffTasks)
	// SplitDiff requires the vtworker only work for one destination shard at a time.
	// To simplify the concurrency control, we run all the SplitDiff task sequentially.
	if err := diffRunner.Run(hw.runSplitDiff, SEQUENTIAL); err != nil {
		return err
	}

	migrateTasks := hw.GenerateTasks(hw.checkpoint, MigrateName)
	migrateRunner := NewParallelRunner(hw.ctx, hw.checkpointWriter, hw.taskUINodeMap, migrateTasks)
	if err := migrateRunner.Run(hw.runMigrate, SEQUENTIAL); err != nil {
		return err
	}
	return nil
}

func (hw *HorizontalReshardingWorkflow) setUIMessage(message string) {
	log.Infof("Horizontal resharding: %v.", message)
	hw.rootUINode.Log = hw.logger.String()
	hw.rootUINode.Message = message
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)
}

// Register registers horizontal_resharding as a valid factory in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardingFactoryName, &HorizontalReshardingWorkflowFactory{})
}

// HorizontalReshardingWorkflowFactory is the factory to register the HorizontalReshard Workflow.
type HorizontalReshardingWorkflowFactory struct{}

// Init is part of the workflow.Factory interface.
func (*HorizontalReshardingWorkflowFactory) Init(workflowProto *workflowpb.Workflow, args []string) error {
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
	workflowProto.Name = fmt.Sprintf("Horizontal resharding on keyspace %s", *keyspace)

	ts := topo.Open()
	defer ts.Close()
	workflowCheckpoint, err := initCheckpoint(*keyspace, vtworkers, ts)
	if err != nil {
		return err
	}

	workflowProto.Data, err = proto.Marshal(workflowCheckpoint)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is part of the workflow.Factory interface.
func (*HorizontalReshardingWorkflowFactory) Instantiate(workflowProto *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	rootNode.Message = "This is a workflow to execute horizontal resharding automatically."

	workflowCheckpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(workflowProto.Data, workflowCheckpoint); err != nil {
		return nil, err
	}

	hw := &HorizontalReshardingWorkflow{
		checkpoint: workflowCheckpoint,
		rootUINode: rootNode,
		copySchemaUINode: &workflow.Node{
			Name:     "CopySchemaShard",
			PathName: CopySchemaName,
		},
		splitCloneUINode: &workflow.Node{
			Name:     "SplitClone",
			PathName: "clone",
		},
		waitFilteredReplicationUINode: &workflow.Node{
			Name:     "WaitForFilteredReplication",
			PathName: WaitFilteredReplicationName,
		},
		splitDiffUINode: &workflow.Node{
			Name:     "SplitDiff",
			PathName: SplitDiffName,
		},
		migrateUINode: &workflow.Node{
			Name:     "MigrateServedType",
			PathName: "migrate",
		},
		logger: logutil.NewMemoryLogger(),
	}
	hw.rootUINode.Children = []*workflow.Node{
		hw.copySchemaUINode,
		hw.splitCloneUINode,
		hw.waitFilteredReplicationUINode,
		hw.splitDiffUINode,
		hw.migrateUINode,
	}

	taskNodeMap := make(map[string]*workflow.Node)
	for _, d := range strings.Split(hw.checkpoint.Settings["destination_shards"], ",") {
		createUINode(CopySchemaName, "dest", d, hw.copySchemaUINode, taskNodeMap)
		createUINode(WaitFilteredReplicationName, "dest", d, hw.waitFilteredReplicationUINode, taskNodeMap)
		createUINode(SplitDiffName, "dest", d, hw.splitDiffUINode, taskNodeMap)
	}
	for _, s := range strings.Split(hw.checkpoint.Settings["source_shards"], ",") {
		createUINode(SplitCloneName, "source", s, hw.splitCloneUINode, taskNodeMap)
		createUINode(MigrateName, "source", s, hw.migrateUINode, taskNodeMap)
	}
	return hw, nil
}

func createUINode(name, shardType, shardName string, rootNode *workflow.Node, nodeMap map[string]*workflow.Node) {
	taskID := createTaskID(name, shardType, shardName)
	taskUINode := &workflow.Node{
		Name:     "Shard " + shardName,
		PathName: taskID,
	}
	rootNode.Children = append(rootNode.Children, taskUINode)
	nodeMap[taskID] = taskUINode
}

// createSubWorkflows creates a per source shard horizontal resharding workflow for each source shard in the keyspace.
func initCheckpoint(keyspace string, vtworkers []string, ts topo.Server) (*workflowpb.WorkflowCheckpoint, error) {
	overlappingShards, err := topotools.FindOverlappingShards(context.Background(), ts, keyspace)
	if err != nil {
		return nil, err
	}

	taskMap := make(map[string]*workflowpb.Task)
	var sourceShardList, destinationShardList []string

	for i, os := range overlappingShards {
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

		s := sourceShard.ShardName()
		sourceShardList = append(sourceShardList, s)
		worker := vtworkers[i]
		for _, ds := range destinationShards {
			d := ds.ShardName()
			destinationShardList = append(destinationShardList, d)

			updatePerDestinationTask(keyspace, s, d, worker, CopySchemaName, taskMap)
			updatePerDestinationTask(keyspace, s, d, worker, WaitFilteredReplicationName, taskMap)
			updatePerDestinationTask(keyspace, s, d, worker, SplitDiffName, taskMap)
		}
		updatePerSourceTask(keyspace, s, worker, SplitCloneName, taskMap)
		updatePerSourceTask(keyspace, s, worker, MigrateName, taskMap)
	}

	return &workflowpb.WorkflowCheckpoint{
		CodeVersion: codeVersion,
		Tasks:       taskMap,
		Settings: map[string]string{
			"source_shards":      strings.Join(sourceShardList, ","),
			"destination_shards": strings.Join(destinationShardList, ","),
		},
	}, nil
}
