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
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/automation"
	"github.com/youtube/vitess/go/vt/concurrency"
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

const (
	horizontalReshardingFactoryName = "horizontal_resharding"
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

	subWorkflows []*PerShardHorizontalResharding
}

// PerShardHorizontalReshardingData is the data structure to store the resharding arguments for each shard.
type PerShardHorizontalReshardingData struct {
	Keyspace          string
	SourceShard       string
	DestinationShards []string
	Vtworker          string
}

// PerShardHorizontalResharding contains the data and method for horizontal resharding from a single source shard.
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

	hw.createSubWorkflows()

	hw.setUIMessage("Horizontal resharding: workflow created successfully.")

	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(true /* updateChildren */)

	// TODO(yipeiw): Support action button to allow retry, stop, restart.
	if err := hw.executeWorkflow(); err != nil {
		return err
	}

	hw.setUIMessage(fmt.Sprintf("Horizontal Resharding on %v: finished sucessfully.", hw.keyspace))

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
			Keyspace:          hw.keyspace,
			SourceShard:       sourceShardName,
			DestinationShards: destShardNames,
			Vtworker:          vtworker,
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

	hw.subWorkflows = append(hw.subWorkflows, perShard)
	return nil
}

func (hw *HorizontalReshardingWorkflow) executeWorkflow() error {
	if err := hw.runAllSubWorkflows(hw.executeCopySchemaPerShard); err != nil {
		hw.logger.Infof("Horizontal Resharding: error in CopySchemaShard: %v.", err)
		return err
	}
	if err := hw.runAllSubWorkflows(hw.executeSplitClonePerShard); err != nil {
		hw.logger.Infof("Horizontal Resharding: error in SplitClone: %v.", err)
		return err
	}
	if err := hw.runAllSubWorkflows(hw.executeSplitDiffPerShard); err != nil {
		hw.logger.Infof("Horizontal Resharding: error in SplitDiff: %v.", err)
		return err
	}
	if err := hw.runAllSubWorkflows(hw.executeMigratePerShard); err != nil {
		hw.logger.Infof("Horizontal Resharding: error in MigratedServedType: %v.", err)
		return err
	}
	return nil
}

// runAllSubWorkflows runs jobs in parallel.
func (hw *HorizontalReshardingWorkflow) runAllSubWorkflows(executeFunc func(subWorkflow *PerShardHorizontalResharding) error) error {
	ec := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, sw := range hw.subWorkflows {
		wg.Add(1)
		go func(s *PerShardHorizontalResharding) {
			defer wg.Done()
			ec.RecordError(executeFunc(s))
		}(sw)
	}
	wg.Wait()
	return ec.Error()
}

// executeCopySchemaPerShard runs CopySchemaShard to copy the schema of a source shard to all its destination shards.
// TODO(yipeiw): excludeTable information can be added to UI input parameters, s.t the user can customize excluded tables during resharding.
func (hw *HorizontalReshardingWorkflow) executeCopySchemaPerShard(perhw *PerShardHorizontalResharding) error {
	sourceKeyspaceShard := topoproto.KeyspaceShardString(perhw.Keyspace, perhw.SourceShard)
	for _, d := range perhw.DestinationShards {
		err := hw.wr.CopySchemaShardFromShard(hw.ctx, nil /* tableArray*/, nil /* excludeTableArray */, true /*includeViews*/, perhw.Keyspace, perhw.SourceShard, perhw.Keyspace, d, wrangler.DefaultWaitSlaveTimeout)
		if err != nil {
			hw.logger.Infof("Horizontal Resharding: error in CopySchemaShardFromShard from %s to %s: %v.", sourceKeyspaceShard, d, err)
			return err
		}
		hw.logger.Infof("Horizontal Resharding: CopySchemaShardFromShard from %s to %s is finished.", sourceKeyspaceShard, d)
	}
	return nil
}

// executeSplitClonePerShard runs SplitClone to clone the data within a keyspace from a source shard to its destination shards.
func (hw *HorizontalReshardingWorkflow) executeSplitClonePerShard(perhw *PerShardHorizontalResharding) error {
	sourceKeyspaceShard := topoproto.KeyspaceShardString(perhw.Keyspace, perhw.SourceShard)
	var destinationKeyspaceShards []string
	for _, destShard := range perhw.DestinationShards {
		destinationKeyspaceShards = append(destinationKeyspaceShards, topoproto.KeyspaceShardString(perhw.Keyspace, destShard))
	}

	// Reset the vtworker to avoid error if vtworker command has been called elsewhere.
	// This is because vtworker class doesn't cleanup the environment after execution.
	automation.ExecuteVtworker(hw.ctx, perhw.Vtworker, []string{"Reset"})
	// The flag min_healthy_rdonly_tablets is set to 1 (default value is 2).
	// Therefore, we can reuse the normal end to end test setting, which has only 1 rdonly tablet.
	// TODO(yipeiw): Add min_healthy_rdonly_tablets as an input argument in UI.
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets=1", sourceKeyspaceShard}
	if _, err := automation.ExecuteVtworker(hw.ctx, perhw.Vtworker, args); err != nil {
		hw.logger.Infof("Horizontal resharding: error in SplitClone in keyspace %s: %v.", perhw.Keyspace, err)
		return err
	}
	hw.logger.Infof("Horizontal resharding: SplitClone is finished.")
	// Wait for filtered replication task.
	for _, d := range perhw.DestinationShards {
		if err := hw.wr.WaitForFilteredReplication(hw.ctx, perhw.Keyspace, d, wrangler.DefaultWaitForFilteredReplicationMaxDelay); err != nil {
			hw.logger.Infof("Horizontal Resharding: error in WaitForFilteredReplication: %v.", err)
			return err
		}
		hw.logger.Infof("Horizontal Resharding:WaitForFilteredReplication is finished on " + d)
	}
	return nil
}

// executeSplitDiffPerShard runs SplitDiff for every destination shard to the source and destination
// to ensure all the data is present and correct.
func (hw *HorizontalReshardingWorkflow) executeSplitDiffPerShard(perhw *PerShardHorizontalResharding) error {
	var destinationKeyspaceShards []string
	for _, destShard := range perhw.DestinationShards {
		destinationKeyspaceShards = append(destinationKeyspaceShards, topoproto.KeyspaceShardString(perhw.Keyspace, destShard))
	}

	for _, d := range destinationKeyspaceShards {
		automation.ExecuteVtworker(hw.ctx, perhw.Vtworker, []string{"Reset"})
		args := []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", d}
		_, err := automation.ExecuteVtworker(hw.ctx, perhw.Vtworker, args)
		if err != nil {
			return err
		}
	}
	hw.logger.Infof("Horizontal resharding: SplitDiff is finished.")
	return nil
}

// executeMigratePerShard runs MigrateServedTypes to switch over to serving from the new shards.
func (hw *HorizontalReshardingWorkflow) executeMigratePerShard(perhw *PerShardHorizontalResharding) error {
	sourceKeyspaceShard := topoproto.KeyspaceShardString(perhw.Keyspace, perhw.SourceShard)
	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		err := hw.wr.MigrateServedTypes(hw.ctx, perhw.Keyspace, perhw.SourceShard, nil /* cells */, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime)
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
