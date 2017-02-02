package resharding

// Package resharding contains a workflow for automatic horizontal resharding.
// The workflow assumes that there are as many vtworker processes running as source shards.
// Plus, these vtworker processes must be reachable via RPC.
// TO DO: it can be used to save checkpointer

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/automation"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

// TODO(yipeiw): the order of exported and unexported variables?
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
	rootUINode       *workflow.Node
	copySchemaUINode *workflow.Node
	splitCloneUINode *workflow.Node
	splitDiffUINode  *workflow.Node
	migrateUINode    *workflow.Node

	keyspace  string
	vtworkers []string

	data             []*PerShardHorizontalResharding
	checkpoint       *workflowpb.WorkflowCheckpoint
	checkpointWriter *CheckpointWriter

	// Each phase has its own ParallelRunner object.
	copyRunner    *ParallelRunner
	cloneRunner   *ParallelRunner
	waitRunner    *ParallelRunner
	diffRunner    *ParallelRunner
	migrateRunner *ParallelRunner
}

// PerShardHorizontalReshardingData is the data structure to store the resharding arguments for a single source shard.
type PerShardHorizontalReshardingData struct {
	keyspace          string
	sourceShard       string
	destinationShards []string
	vtworker          string
}

// PerShardHorizontalResharding contains the data for horizontal resharding from a single source shard.
type PerShardHorizontalResharding struct {
	PerShardHorizontalReshardingData

	parent *HorizontalReshardingWorkflow

	copySchemaShardUINode *workflow.Node
	splitCloneShardUINode *workflow.Node
	splitDiffShardUINode  *workflow.Node
	migrateShardUINode    *workflow.Node

	shardUILogger *logutil.MemoryLogger
}

// Run executes the horizontal resharding process and updates the UI message.
// It implements the workflow.Workflow interface.
func (hw *HorizontalReshardingWorkflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.ctx = ctx
	hw.topoServer = manager.TopoServer()
	hw.wr = wrangler.New(logutil.NewConsoleLogger(), manager.TopoServer(), tmclient.NewTabletManagerClient())
	hw.wi = wi

	// TODO(yipeiw): separate the source shards, destination shards finding code and other initialization code for the convenience of unit test.
	hw.createSubWorkflows()
	hw.setUIMessage("Horizontal resharding: workflow created successfully.")

	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(true /* updateChildren */)

	if err := hw.runWorkflow(); err != nil {
		hw.setUIMessage(fmt.Sprintf("Horizontal Resharding failed: %v", err))
		return err
	}

	hw.setUIMessage(fmt.Sprintf("Horizontal Resharding on %v: finished sucessfully.", hw.keyspace))

	return nil
}

func (hw *HorizontalReshardingWorkflow) runWorkflow() error {
	hw.checkpoint = hw.InitTasks()
	hw.checkpointWriter = NewCheckpointWriter(hw.topoServer, hw.checkpoint, hw.wi)
	hw.checkpointWriter.Save()

	copyTasks := hw.GenerateTasks(hw.checkpoint, CopySchemaName)
	if err := hw.copyRunner.Run(copyTasks, hw.runCopySchema, hw.checkpointWriter, PARALLEL); err != nil {
		return err
	}

	cloneTasks := hw.GenerateTasks(hw.checkpoint, SplitCloneName)
	if err := hw.cloneRunner.Run(cloneTasks, hw.runSplitClone, hw.checkpointWriter, PARALLEL); err != nil {
		return err
	}

	waitTasks := hw.GenerateTasks(hw.checkpoint, WaitFilteredReplicationName)
	if err := hw.waitRunner.Run(waitTasks, hw.runWaitFilteredReplication, hw.checkpointWriter, PARALLEL); err != nil {
		return err
	}

	diffTasks := hw.GenerateTasks(hw.checkpoint, SplitDiffName)
	// SplitDiff requires the vtworker only work for one destination shard at a time.
	// To simplify the concurrency control, we run all the SplitDiff task sequentially.
	if err := hw.diffRunner.Run(diffTasks, hw.runSplitDiff, hw.checkpointWriter, SEQUENTIAL); err != nil {
		return err
	}

	migrateTasks := hw.GenerateTasks(hw.checkpoint, MigrateName)
	if err := hw.migrateRunner.Run(migrateTasks, hw.runMigrate, hw.checkpointWriter, SEQUENTIAL); err != nil {
		return err
	}
	return nil
}

// createSubWorkflows creates a per source shard horizontal resharding workflow for each source shard in the keyspace.
func (hw *HorizontalReshardingWorkflow) createSubWorkflows() error {
	overlappingShards, err := topotools.FindOverlappingShards(hw.ctx, hw.topoServer, hw.keyspace)
	if err != nil {
		hw.logger.Infof("Horizontal Resharding: createSubWorkflows error in finding overlapping shards: %v.", err)
		return err
	}

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
		if err := hw.createWorkflowPerShard(sourceShard, destinationShards, hw.vtworkers[i]); err != nil {
			return err
		}
	}
	return nil
}

func (hw *HorizontalReshardingWorkflow) createWorkflowPerShard(sourceShard *topo.ShardInfo, destinationShards []*topo.ShardInfo, vtworker string) error {
	sourceShardName := sourceShard.ShardName()
	var destShardNames []string
	for _, s := range destinationShards {
		destShardNames = append(destShardNames, s.ShardName())
	}

	perShard := &PerShardHorizontalResharding{
		PerShardHorizontalReshardingData: PerShardHorizontalReshardingData{
			keyspace:          hw.keyspace,
			sourceShard:       sourceShardName,
			destinationShards: destShardNames,
			vtworker:          vtworker,
		},
		copySchemaShardUINode: &workflow.Node{
			Name:     "Shard " + sourceShardName,
			PathName: "shard_" + sourceShardName,
		},
		splitCloneShardUINode: &workflow.Node{
			Name:     "Shard " + sourceShardName,
			PathName: "shard_" + sourceShardName,
		},
		splitDiffShardUINode: &workflow.Node{
			Name:     "Shard " + sourceShardName,
			PathName: "shard_" + sourceShardName,
		},
		migrateShardUINode: &workflow.Node{
			Name:     "Shard " + sourceShardName,
			PathName: "shard_" + sourceShardName,
		},
		shardUILogger: logutil.NewMemoryLogger(),
	}
	perShard.parent = hw

	hw.copySchemaUINode.Children = append(hw.copySchemaUINode.Children, perShard.copySchemaShardUINode)
	hw.splitCloneUINode.Children = append(hw.splitCloneUINode.Children, perShard.splitCloneShardUINode)
	hw.splitDiffUINode.Children = append(hw.splitDiffUINode.Children, perShard.splitDiffShardUINode)
	hw.migrateUINode.Children = append(hw.migrateUINode.Children, perShard.migrateShardUINode)

	hw.data = append(hw.data, perShard)
	return nil
}

// runCopySchemaPerShard runs CopySchema for a destination shard.
// There should be #destshards parameters, while each param includes 1 sourceshard and 1 destshard.
func (hw *HorizontalReshardingWorkflow) runCopySchema(attr map[string]string) error {
	s := attr["source_shard"]
	d := attr["destination_shard"]
	err := hw.wr.CopySchemaShardFromShard(hw.ctx, nil /* tableArray*/, nil /* excludeTableArray */, true, /*includeViews*/
		hw.keyspace, s, hw.keyspace, d, wrangler.DefaultWaitSlaveTimeout)
	if err != nil {
		hw.logger.Infof("Horizontal Resharding: error in CopySchemaShardFromShard from %s to %s: %v.", s, d, err)
	}
	hw.logger.Infof("Horizontal Resharding: CopySchemaShardFromShard from %s to %s is finished.", s, d)
	return err
}

// runSplitClonePerShard runs SplitClone for a source shard.
// There should be #sourceshards parameters, while each param includes 1 sourceshard and its destshards. The destShards are useless here.
func (hw *HorizontalReshardingWorkflow) runSplitClone(attr map[string]string) error {
	s := attr["source_shard"]
	worker := attr["vtworker"]

	sourceKeyspaceShard := topoproto.KeyspaceShardString(hw.keyspace, s)
	// Reset the vtworker to avoid error if vtworker command has been called elsewhere.
	// This is because vtworker class doesn't cleanup the environment after execution.
	automation.ExecuteVtworker(hw.ctx, worker, []string{"Reset"})
	// The flag min_healthy_rdonly_tablets is set to 1 (default value is 2).
	// Therefore, we can reuse the normal end to end test setting, which has only 1 rdonly tablet.
	// TODO(yipeiw): Add min_healthy_rdonly_tablets as an input argument in UI.
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets=1", sourceKeyspaceShard}
	if _, err := automation.ExecuteVtworker(hw.ctx, worker, args); err != nil {
		hw.logger.Infof("Horizontal resharding: error in SplitClone in keyspace %s: %v.", hw.keyspace, err)
		return err
	}
	hw.logger.Infof("Horizontal resharding: SplitClone is finished.")

	return nil
}

// runWaitFilteredReplication runs WaitForFilteredReplication for a destination shard.
// There should be #destshards parameters, while each param includes 1 sourceshard and 1 destshard.
func (hw *HorizontalReshardingWorkflow) runWaitFilteredReplication(attr map[string]string) error {
	d := attr["destination_shard"]
	if err := hw.wr.WaitForFilteredReplication(hw.ctx, hw.keyspace, d, wrangler.DefaultWaitForFilteredReplicationMaxDelay); err != nil {
		hw.logger.Infof("Horizontal Resharding: error in WaitForFilteredReplication: %v.", err)
		return err
	}
	hw.logger.Infof("Horizontal Resharding:WaitForFilteredReplication is finished on " + d)
	return nil
}

// runSplitDiffPerShard runs SplitDiff for a source shard.
// There should be #sourceshards parameters, while each param includes 1 sourceshard and its destshards.
func (hw *HorizontalReshardingWorkflow) runSplitDiff(attr map[string]string) error {
	d := attr["destination_shard"]
	worker := attr["vtworker"]

	automation.ExecuteVtworker(hw.ctx, worker, []string{"Reset"})
	args := []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", topoproto.KeyspaceShardString(hw.keyspace, d)}
	_, err := automation.ExecuteVtworker(hw.ctx, worker, args)
	if err != nil {
		return err
	}

	hw.logger.Infof("Horizontal resharding: SplitDiff is finished.")
	return nil
}

// runMigratePerShard runs the migration sequentially among all source shards.
// There should be 1 parameter, which includes all source shards to be migrated.
func (hw *HorizontalReshardingWorkflow) runMigrate(attr map[string]string) error {
	s := attr["source_shard"]
	sourceKeyspaceShard := topoproto.KeyspaceShardString(hw.keyspace, s)
	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		err := hw.wr.MigrateServedTypes(hw.ctx, hw.keyspace, s, nil /* cells */, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime)
		if err != nil {
			hw.logger.Infof("Horizontal Resharding: error in MigrateServedTypes on servedType %s: %v.", servedType, err)
			return err
		}
		hw.logger.Infof("Horizontal Resharding: MigrateServedTypes is finished on tablet %s serve type %s.", sourceKeyspaceShard, servedType)
	}
	return nil
}

func (hw *HorizontalReshardingWorkflow) setUIMessage(message string) {
	log.Infof("Horizontal resharding on keyspace %v: %v.", hw.keyspace, message)
	hw.rootUINode.Log = hw.logger.String()
	hw.rootUINode.Message = message
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)
}

// WorkflowFactory is the factory to register the HorizontalReshard Workflow.
type WorkflowFactory struct{}

// Register registers horizontal_resharding as a valid factory in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardingFactoryName, &WorkflowFactory{})
}

// Init is part of the workflow.Factory interface.
func (*WorkflowFactory) Init(workflowProto *workflowpb.Workflow, args []string) error {
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
	data := &HorizontalReshardingData{
		Keyspace:  *keyspace,
		Vtworkers: vtworkers,
	}
	var err error
	workflowProto.Data, err = json.Marshal(data)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is part of the workflow.Factory interface.
func (*WorkflowFactory) Instantiate(workflowProto *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	rootNode.Message = "This is a workflow to execute horizontal resharding automatically."
	data := &HorizontalReshardingData{}
	if err := json.Unmarshal(workflowProto.Data, data); err != nil {
		return nil, err
	}

	hw := &HorizontalReshardingWorkflow{
		keyspace:   data.Keyspace,
		vtworkers:  data.Vtworkers,
		rootUINode: rootNode,
		copySchemaUINode: &workflow.Node{
			Name:     "CopySchemaShard",
			PathName: "copy_schema",
		},
		splitCloneUINode: &workflow.Node{
			Name:     "SplitClone",
			PathName: "clone",
		},
		splitDiffUINode: &workflow.Node{
			Name:     "SplitDiff",
			PathName: "diff",
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
		hw.splitDiffUINode,
		hw.migrateUINode,
	}
	return hw, nil
}
