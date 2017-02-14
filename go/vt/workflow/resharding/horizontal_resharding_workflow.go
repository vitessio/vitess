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

	copySchemaName                 = "copy_schema"
	cloneName                      = "clone"
	waitForFilteredReplicationName = "wait_for_filtered_replication"
	diffName                       = "diff"
	migrateRdonlyName              = "migrate_rdonly"
	migrateReplicaName             = "migrate_replica"
	migrateMasterName              = "migrate_master"
)

// HorizontalReshardingWorkflow contains meta-information and methods
// to control horizontal resharding workflow.
type HorizontalReshardingWorkflow struct {
	// ctx is the context of the whole horizontal resharding process.
	// Once this context is canceled, the horizontal resharding process stops.
	ctx        context.Context
	manager    *workflow.Manager
	topoServer topo.Server
	wi         *topo.WorkflowInfo
	wr         ReshardingWrangler
	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode                       *workflow.Node
	copySchemaUINode                 *workflow.Node
	cloneUINode                      *workflow.Node
	waitForFilteredReplicationUINode *workflow.Node
	diffUINode                       *workflow.Node
	migrateRdonlyUINode              *workflow.Node
	migrateReplicaUINode             *workflow.Node
	migrateMasterUINode              *workflow.Node

	checkpoint       *workflowpb.WorkflowCheckpoint
	checkpointWriter *CheckpointWriter
}

// Run executes the horizontal resharding process and updates the UI message.
// It implements the workflow.Workflow interface.
func (hw *HorizontalReshardingWorkflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.ctx = ctx
	hw.manager = manager
	hw.topoServer = manager.TopoServer()
	hw.wi = wi
	hw.wr = wrangler.New(logutil.NewConsoleLogger(), manager.TopoServer(), tmclient.NewTabletManagerClient())

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
	copyTasks := hw.GetTasks(hw.checkpoint, copySchemaName)
	copyRunner := NewParallelRunner(hw.ctx, hw.manager.NodeManager(), hw.copySchemaUINode, hw.checkpointWriter, copyTasks, hw.runCopySchema, PARALLEL)
	if err := copyRunner.Run(); err != nil {
		return err
	}

	cloneTasks := hw.GetTasks(hw.checkpoint, cloneName)
	cloneRunner := NewParallelRunner(hw.ctx, hw.manager.NodeManager(), hw.cloneUINode, hw.checkpointWriter, cloneTasks, hw.runSplitClone, PARALLEL)
	if err := cloneRunner.Run(); err != nil {
		return err
	}

	waitTasks := hw.GetTasks(hw.checkpoint, waitForFilteredReplicationName)
	waitRunner := NewParallelRunner(hw.ctx, hw.manager.NodeManager(), hw.waitForFilteredReplicationUINode, hw.checkpointWriter, waitTasks, hw.runWaitForFilteredReplication, PARALLEL)
	if err := waitRunner.Run(); err != nil {
		return err
	}

	diffTasks := hw.GetTasks(hw.checkpoint, diffName)
	diffRunner := NewParallelRunner(hw.ctx, hw.manager.NodeManager(), hw.diffUINode, hw.checkpointWriter, diffTasks, hw.runSplitDiff, SEQUENTIAL)
	// SplitDiff requires the vtworker only work for one destination shard
	// at a time. To simplify the concurrency control, we run all the SplitDiff
	// task sequentially.
	if err := diffRunner.Run(); err != nil {
		return err
	}

	migrateRdonlyTasks := hw.GetTasks(hw.checkpoint, migrateRdonlyName)
	migrateRdonlyRunner := NewParallelRunner(hw.ctx, hw.manager.NodeManager(), hw.migrateRdonlyUINode, hw.checkpointWriter, migrateRdonlyTasks, hw.runMigrate, SEQUENTIAL)
	if err := migrateRdonlyRunner.Run(); err != nil {
		return err
	}

	migrateReplicaTasks := hw.GetTasks(hw.checkpoint, migrateReplicaName)
	migrateReplicaRunner := NewParallelRunner(hw.ctx, hw.manager.NodeManager(), hw.migrateReplicaUINode, hw.checkpointWriter, migrateReplicaTasks, hw.runMigrate, SEQUENTIAL)
	if err := migrateReplicaRunner.Run(); err != nil {
		return err
	}

	migrateMasterTasks := hw.GetTasks(hw.checkpoint, migrateMasterName)
	migrateMasterRunner := NewParallelRunner(hw.ctx, hw.manager.NodeManager(), hw.migrateMasterUINode, hw.checkpointWriter, migrateMasterTasks, hw.runMigrate, SEQUENTIAL)
	if err := migrateMasterRunner.Run(); err != nil {
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

// Register registers horizontal_resharding as a valid factory
// in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardingFactoryName, &HorizontalReshardingWorkflowFactory{})
}

// HorizontalReshardingWorkflowFactory is the factory to register
// the HorizontalResharding Workflow.
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

	checkpoint, err := initCheckpoint(*keyspace, vtworkers)
	if err != nil {
		return err
	}

	w.Data, err = proto.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is part of the workflow.Factory interface.
func (*HorizontalReshardingWorkflowFactory) Instantiate(w *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	rootNode.Message = "This is a workflow to execute horizontal resharding automatically."

	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(w.Data, checkpoint); err != nil {
		return nil, err
	}

	hw := &HorizontalReshardingWorkflow{
		checkpoint: checkpoint,
		rootUINode: rootNode,
		copySchemaUINode: &workflow.Node{
			Name:     "CopySchemaShard",
			PathName: copySchemaName,
		},
		cloneUINode: &workflow.Node{
			Name:     "SplitClone",
			PathName: cloneName,
		},
		waitForFilteredReplicationUINode: &workflow.Node{
			Name:     "WaitForFilteredReplication",
			PathName: waitForFilteredReplicationName,
		},
		diffUINode: &workflow.Node{
			Name:     "SplitDiff",
			PathName: diffName,
		},
		migrateRdonlyUINode: &workflow.Node{
			Name:     "MigrateServedTypeRDONLY",
			PathName: migrateRdonlyName,
		},
		migrateReplicaUINode: &workflow.Node{
			Name:     "MigrateServedTypeREPLICA",
			PathName: migrateReplicaName,
		},
		migrateMasterUINode: &workflow.Node{
			Name:     "MigrateServedTypeMASTER",
			PathName: migrateMasterName,
		},
		logger: logutil.NewMemoryLogger(),
	}
	hw.rootUINode.Children = []*workflow.Node{
		hw.copySchemaUINode,
		hw.cloneUINode,
		hw.waitForFilteredReplicationUINode,
		hw.diffUINode,
		hw.migrateRdonlyUINode,
		hw.migrateReplicaUINode,
		hw.migrateMasterUINode,
	}

	destinationShards := strings.Split(checkpoint.Settings["destination_shards"], ",")
	sourceShards := strings.Split(checkpoint.Settings["source_shards"], ",")
	createUINode(copySchemaName, destinationShards, hw.copySchemaUINode)
	createUINode(cloneName, sourceShards, hw.cloneUINode)
	createUINode(waitForFilteredReplicationName, destinationShards, hw.waitForFilteredReplicationUINode)
	createUINode(diffName, destinationShards, hw.diffUINode)
	createUINode(migrateRdonlyName, sourceShards, hw.migrateRdonlyUINode)
	createUINode(migrateReplicaName, sourceShards, hw.migrateReplicaUINode)
	createUINode(migrateMasterName, sourceShards, hw.migrateMasterUINode)

	return hw, nil
}

func createUINode(phaseName string, shards []string, rootNode *workflow.Node) {
	for _, shardName := range shards {
		taskID := createTaskID(phaseName, shardName)
		taskUINode := &workflow.Node{
			Name:     "Shard " + shardName,
			PathName: taskID,
		}
		rootNode.Children = append(rootNode.Children, taskUINode)
	}
}

func initCheckpoint(keyspace string, vtworkers []string) (*workflowpb.WorkflowCheckpoint, error) {
	ts := topo.Open()
	defer ts.Close()

	overlappingShards, err := topotools.FindOverlappingShards(context.Background(), ts, keyspace)
	if err != nil {
		return nil, err
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

	taskMap := make(map[string]*workflowpb.Task)
	initTasks(copySchemaName, destinationShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"source_shard":      sourceShardList[0],
			"destination_shard": shard,
			"keyspace":          keyspace,
		}
	})

	initTasks(cloneName, sourceShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"source_shard": shard,
			"vtworker":     vtworkers[i],
			"keyspace":     keyspace,
		}
	})

	initTasks(waitForFilteredReplicationName, destinationShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"destination_shard": shard,
			"keyspace":          keyspace,
		}
	})

	initTasks(diffName, destinationShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"destination_shard": shard,
			"vtworker":          vtworkers[0],
			"keyspace":          keyspace,
		}
	})

	initTasks(migrateRdonlyName, sourceShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"source_shard": shard,
			"keyspace":     keyspace,
			"served_type":  topodatapb.TabletType_RDONLY.String(),
		}
	})
	initTasks(migrateReplicaName, sourceShardList, taskMap, func(i int, shard string) map[string]string {
		return map[string]string{
			"source_shard": shard,
			"keyspace":     keyspace,
			"served_type":  topodatapb.TabletType_REPLICA.String(),
		}
	})
	initTasks(migrateMasterName, sourceShardList, taskMap, func(i int, shard string) map[string]string {
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

func initTasks(phaseName string, shards []string, taskMap map[string]*workflowpb.Task, createAttributes func(int, string) map[string]string) {
	for i, s := range shards {
		taskID := createTaskID(phaseName, s)
		taskMap[taskID] = &workflowpb.Task{
			Id:         taskID,
			State:      workflowpb.TaskState_TaskNotStarted,
			Attributes: createAttributes(i, s),
		}
	}
}
