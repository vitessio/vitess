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
	"github.com/youtube/vitess/go/vt/vtctl"

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

// HorizontalReshardingWorkflow contains meta-information and methods to controll horizontal resharding workflow.
type HorizontalReshardingWorkflow struct {
	// ctx is the context of the whole schema swap process. Once this context is cancelled,
	// the horizontal resharding process stops.
	ctx context.Context

	wr *wrangler.Wrangler
	// manager is the current Manager.
	manager *workflow.Manager

	topoServer topo.Server
	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode       *workflow.Node
	copySchemaUINode *workflow.Node
	splitCloneUINode *workflow.Node
	splitDiffUINode  *workflow.Node
	migrateUINode    *workflow.Node

	keyspace string

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
	parent *HorizontalReshardingWorkflow

	shardName string

	copySchemaShardUINode *workflow.Node
	splitCloneShardUINode *workflow.Node
	splitDiffShardUINode  *workflow.Node
	migrateShardUINode    *workflow.Node

	shardUILogger *logutil.MemoryLogger

	data *PerShardHorizontalReshardingData
}

// createSubWorkflows creates per source shard horizontal resharding workflow for all source shards in the keyspace.
func (hw *HorizontalReshardingWorkflow) createSubWorkflows() error {
	overlappingShards, err := topotools.FindOverlappingShards(hw.ctx, hw.topoServer, hw.keyspace)
	if err != nil {
		errMessage := fmt.Sprintf("Horizontal Resharding: createSubWorkflows error in finding overlapping shards:%v.", err)
		hw.setUIMessage(errMessage)
		return err
	}

	var workerCount = 0
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

		err := hw.createWorkflowPerShard(sourceShard, destinationShards, hw.vtworkers[workerCount])
		if err != nil {
			return err
		}
		workerCount++
	}
	return nil
}

func (hw *HorizontalReshardingWorkflow) createWorkflowPerShard(sourceShard *topo.ShardInfo, destinationShards []*topo.ShardInfo, vtworker string) error {
	sourceShardName := sourceShard.ShardName()
	var destShardNames []string
	for _, s := range destinationShards {
		destShardNames = append(destShardNames, s.ShardName())
	}

	data := &PerShardHorizontalReshardingData{
		Keyspace:          hw.keyspace,
		SourceShard:       sourceShardName,
		DestinationShards: destShardNames,
		Vtworker:          vtworker,
	}

	perShard := &PerShardHorizontalResharding{
		data: data,
		copySchemaShardUINode: &workflow.Node{
			Name:     "Shard " + sourceShardName,
			PathName: "copy_schema",
		},
		splitCloneShardUINode: &workflow.Node{
			Name:     "Shard " + sourceShardName,
			PathName: "split_clone",
		},
		splitDiffShardUINode: &workflow.Node{
			Name:     "Shard " + sourceShardName,
			PathName: "split_diff",
		},
		migrateShardUINode: &workflow.Node{
			Name:     "Shard " + sourceShardName,
			PathName: "mst",
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

// Run executes the horizontal resharding process and updates the UI message.
// It implements the workflow.Workflow interface.
func (hw *HorizontalReshardingWorkflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.ctx = ctx
	hw.topoServer = manager.TopoServer()
	hw.wr = wrangler.New(logutil.NewConsoleLogger(), manager.TopoServer(), tmclient.NewTabletManagerClient())

	hw.createSubWorkflows()

	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(true /* updateChildren */)

	// TODO(yipeiw): support action button to allow retry, stop, restart
	if err := hw.runAllSubWorkflows(hw.executeCopySchemaPerShard); err != nil {
		errMessage := fmt.Sprintf("Horizontal Resharding: error in CopySchemaShard:%v.", err)
		hw.setUIMessage(errMessage)
		return err
	}
	if err := hw.runAllSubWorkflows(hw.executeSplitClonePerShard); err != nil {
		errMessage := fmt.Sprintf("Horizontal Resharding: error in SplitClone:%v.", err)
		hw.setUIMessage(errMessage)
		return err
	}
	if err := hw.runAllSubWorkflows(hw.executeSplitDiffPerShard); err != nil {
		errMessage := fmt.Sprintf("Horizontal Resharding: error in SplitDiff:%v.", err)
		hw.setUIMessage(errMessage)
		return err
	}
	if err := hw.runAllSubWorkflows(hw.executeMigratePerShard); err != nil {
		errMessage := fmt.Sprintf("Horizontal Resharding: error in MigratedServedType:%v.", err)
		hw.setUIMessage(errMessage)
		return err
	}

	progressMessage := fmt.Sprintf("Horizontal Resharding: finished sucessfully.")
	hw.setUIMessage(progressMessage)

	return nil
}

// runAllSubWorkflows runs jobs parallely, fails when any job fails and only returns one of the errors when several
// fails.
func (hw *HorizontalReshardingWorkflow) runAllSubWorkflows(executeFunc func(subworkflow *PerShardHorizontalResharding) error) error {
	var errorRecorder concurrency.AllErrorRecorder
	var waitGroup sync.WaitGroup
	for _, sw := range hw.subWorkflows {
		waitGroup.Add(1)
		go func(s *PerShardHorizontalResharding) {
			defer waitGroup.Done()
			errorRecorder.RecordError(executeFunc(s))
		}(sw)
	}
	waitGroup.Wait()
	return errorRecorder.Error()
}

// executeCopySchemaPerShard runs CopySchemaShard to copy the schema of a source shard to all its destination shards.
// TODO(yipeiw): excludeTable information can be added to UI input parameters, s.t the user can customized excluded tables during resharding.
func (hw *HorizontalReshardingWorkflow) executeCopySchemaPerShard(perhw *PerShardHorizontalResharding) error {
	sourceKeyspaceShard := topoproto.KeyspaceShardString(perhw.data.Keyspace, perhw.data.SourceShard)
	for _, d := range perhw.data.DestinationShards {
		err := hw.wr.CopySchemaShardFromShard(context.TODO(), nil /* tableArray*/, nil /* excludeTableArray */, true /*includeViews*/, perhw.data.Keyspace, perhw.data.SourceShard, perhw.data.Keyspace, d, wrangler.DefaultWaitSlaveTimeout)
		if err != nil {
			errorMessage := fmt.Sprintf("Horizontal Resharding: error in CopySchemaShardFromShard from %s to %s: %v.",
				sourceKeyspaceShard, d, err)
			hw.setUIMessage(errorMessage)
			return err
		}
		progressMessage := fmt.Sprintf("Horizontal Resharding: CopySchemaShardFromShard from %s to %s is finished.", sourceKeyspaceShard, d)
		hw.setUIMessage(progressMessage)
	}
	return nil
}

// executeSplitClonePerShard runs SplitClone to clone the data within a keyspace from a source shard to its destination shards.
func (hw *HorizontalReshardingWorkflow) executeSplitClonePerShard(perhw *PerShardHorizontalResharding) error {
	sourceKeyspaceShard := topoproto.KeyspaceShardString(perhw.data.Keyspace, perhw.data.SourceShard)
	var destinationKeyspaceShards []string
	for _, destShard := range perhw.data.DestinationShards {
		destinationKeyspaceShards = append(destinationKeyspaceShards, topoproto.KeyspaceShardString(perhw.data.Keyspace, destShard))
	}

	// Reset the vtworker to avoid error if vtworker command has been called elsewhere.
	// This is because vtworker class doesn't cleanup the environment after execution.
	automation.ExecuteVtworker(context.TODO(), perhw.data.Vtworker, []string{"Reset"})
	// The flag min_healthy_rdonly_tablets is set to 1 (default value is 2).
	// Therefore, we can reuse the normal end to end test setting, which has only 1 rdonly tablet.
	// TODO(yipeiw): Add min_healthy_rdonly_tablets as an input argument in UI.
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets=1", sourceKeyspaceShard}
	if _, err := automation.ExecuteVtworker(context.TODO(), perhw.data.Vtworker, args); err != nil {
		errorMessage := fmt.Sprintf("Horizontal resharding: error in SplitClone in keyspace %s:%v.",
			perhw.data.Keyspace, err)
		hw.setUIMessage(errorMessage)
		return err
	}
	hw.setUIMessage("Horizontal resharding: SplitClone is finished.")

	// Wait for filtered replication task.
	for _, d := range destinationKeyspaceShards {
		args := []string{"WaitForFilteredReplication", d}
		if err := vtctl.RunCommand(hw.ctx, hw.wr, args); err != nil {
			hw.setUIMessage(fmt.Sprintf("Horizontal Resharding: error in WaitForFilteredReplication: %v.", err))
			return err
		}
		hw.setUIMessage("Horizontal Resharding:WaitForFilteredReplication is finished on " + d)
	}
	return nil
}

// executeSplitDiffPerShard runs SplitDiff for every destination shard to the source and destination
// to ensure all the data is present and correct.
func (hw *HorizontalReshardingWorkflow) executeSplitDiffPerShard(perhw *PerShardHorizontalResharding) error {
	var destinationKeyspaceShards []string
	for _, destShard := range perhw.data.DestinationShards {
		destinationKeyspaceShards = append(destinationKeyspaceShards, topoproto.KeyspaceShardString(perhw.data.Keyspace, destShard))
	}

	for _, d := range destinationKeyspaceShards {
		automation.ExecuteVtworker(hw.ctx, perhw.data.Vtworker, []string{"Reset"})
		args := []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", d}
		_, err := automation.ExecuteVtworker(context.TODO(), perhw.data.Vtworker, args)
		if err != nil {
			return err
		}
	}
	hw.setUIMessage("Horizontal resharding: SplitDiff is finished.")
	return nil
}

// executeMigratePerShard runs MigrateServedTypes to switch over to serving from the new shards.
func (hw *HorizontalReshardingWorkflow) executeMigratePerShard(perhw *PerShardHorizontalResharding) error {
	sourceKeyspaceShard := topoproto.KeyspaceShardString(perhw.data.Keyspace, perhw.data.SourceShard)
	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		err := hw.wr.MigrateServedTypes(context.TODO(), perhw.data.Keyspace, perhw.data.SourceShard, nil /* cells */, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime)
		if err != nil {
			errorMessage := fmt.Sprintf("Horizontal Resharding: error in MigrateServedTypes on servedType %s: %v.", servedType, err)
			hw.setUIMessage(errorMessage)
			return err
		}
		progressMessage := fmt.Sprintf("Horizontal Resharding: MigrateServedTypes is finished on tablet %s serve type %s.", sourceKeyspaceShard, servedType)
		hw.setUIMessage(progressMessage)
	}
	return nil
}

func (hw *HorizontalReshardingWorkflow) setUIMessage(message string) {
	log.Infof("Horizontal resharding on keyspace %v: %v.", hw.keyspace, message)
	hw.logger.Infof(message)
	hw.rootUINode.Log = hw.logger.String()
	hw.rootUINode.Message = message
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)
}

// WorkflowFactory is the factory to register the HorizontalReshard Workflows.
type WorkflowFactory struct{}

// Register registers horizontal_resharding as a valid factory in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardingFactoryName, &WorkflowFactory{})
}

// Init is part of the workflow.Factory interface.
func (*WorkflowFactory) Init(workflowProto *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(horizontalReshardingFactoryName, flag.ContinueOnError)
	keyspace := subFlags.String("keyspace", "", "Name of keyspace to perform horizontal resharding")
	vtworkerStr := subFlags.String("vtworkers", "", "A list of address of vtworkers (comma-separated)")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *keyspace == "" || *vtworkerStr == "" {
		return fmt.Errorf("Keyspace name, vtworkers information must be provided for horizontal resharding")
	}

	vtworkers := strings.Split(*vtworkerStr, ",")
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
			PathName: "split_clone",
		},
		splitDiffUINode: &workflow.Node{
			Name:     "SplitDiff",
			PathName: "split_diff",
		},
		migrateUINode: &workflow.Node{
			Name:     "MigrateServedType",
			PathName: "mst",
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
