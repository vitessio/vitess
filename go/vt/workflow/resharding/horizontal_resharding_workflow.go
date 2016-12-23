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
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	horizontalReshardingFactoryName = "horizontal_resharding"
)

// HorizontalReshardingWorkflowData is the data structure to store resharding arguments.
type HorizontalReshardingData struct {
	Keyspace  string
	Vtworkers []string
}

// HorizontalReshardingWorkflow contains meta-information and methods to controll horizontal resharding workflow.
type HorizontalReshardingWorkflow struct {
	// ctx is the context of the whole schema swap process. Once this context is cancelled
	// the schema swap process stops.
	ctx context.Context

	wr *wrangler.Wrangler
	// manager is the current Manager.
	manager *workflow.Manager

	topoServer topo.Server
	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode *workflow.Node

	keyspace string

	vtworkers []string

	subWorkflows []*PerShardHorizontalResharding
}

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

	shardUINode *workflow.Node

	copySchemaUINode *workflow.Node
	splitCloneUINode *workflow.Node
	splitDiffUINode  *workflow.Node
	migrateUINode    *workflow.Node

	shardUILogger *logutil.MemoryLogger

	data *PerShardHorizontalReshardingData
}

// createSubWorkflows creates per source shard horizontl resharding workflow for all source shards in the keyspace
func (hw *HorizontalReshardingWorkflow) createSubWorkflows() error {
	// DEBUG
	var progress string
	progress = fmt.Sprintf("createSubWorflow: calling findoverlapping shards, the vtworkerinfo:%v, the keyspace:%v", hw.vtworkers, hw.keyspace)
	hw.setUIMessage(progress)

	overlappingShards, err := topotools.FindOverlappingShards(hw.ctx, hw.topoServer, hw.keyspace)
	if err != nil {
		errMessage := fmt.Sprintf("Horizontal Resharding: createSubWorkflows error in finding overlapping shards:%v", err)
		hw.setUIMessage(errMessage)
		return err
	}

	// DEBUG
	progress = fmt.Sprintf("overlapping shards found: %v", overlappingShards)
	hw.setUIMessage(progress)

	var workerCount = 0
	for _, os := range overlappingShards {
		var perShard *PerShardHorizontalResharding
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

		//DEBUG
		progress = fmt.Sprintf("creating subworkflow for source shard: %v", *sourceShard)
		hw.setUIMessage(progress)

		//issue:  the childnode for subworkflow not showup in the UI
		perShard, err := hw.createWorkflowPerShard(sourceShard, destinationShards, hw.vtworkers[workerCount])
		if err != nil {
			return err
		}
		//DEBUG
		progress = fmt.Sprintf("finished creating subworkflow for source shard: %v", *sourceShard)
		hw.setUIMessage(progress)

		workerCount++
		hw.rootUINode.Children = append(hw.rootUINode.Children, perShard.shardUINode)
		hw.subWorkflows = append(hw.subWorkflows, perShard)
	}
	return nil
}

func (hw *HorizontalReshardingWorkflow) createWorkflowPerShard(sourceShard *topo.ShardInfo, destinationShards []*topo.ShardInfo, vtworker string) (*PerShardHorizontalResharding, error) {
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
		shardUINode: &workflow.Node{
			Name:     fmt.Sprintf("Shard %v", sourceShardName),
			PathName: sourceShardName,
			State:    workflowpb.WorkflowState_Running,
		},
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
			Name:     "MigratedServedType",
			PathName: "mst",
		},
		shardUILogger: logutil.NewMemoryLogger(),
	}
	perShard.shardUINode.Children = []*workflow.Node{
		perShard.copySchemaUINode,
		perShard.splitCloneUINode,
		perShard.splitDiffUINode,
		perShard.migrateUINode,
	}
	return perShard, nil
}

// Run executes the horizontal resharding process and updates UI message.
// It implements the workflow.Workflow interface.
func (hw *HorizontalReshardingWorkflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)

	hw.ctx = ctx
	hw.topoServer = manager.TopoServer()
	hw.wr = wrangler.New(logutil.NewConsoleLogger(), manager.TopoServer(), tmclient.NewTabletManagerClient())
	hw.createSubWorkflows()

	// TODO(yipeiw): run each subworkflow parallely
	// err := hw.runAllSubWorkflows(hw.executeReshardPerSource)
	// TODO(yipeiw): support action button to allow retry, stop, restart

	/*if err != nil {
		return err
	}*/
	return nil
}

// TODO(yipeiw): This function is copied from swap_schema directly, which fails when any job fails and only returns one of the errors when several
// fails. The result I need is to be able to control each workflow separatly for this resharding since they are independent
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

// executeReshard is the main entry point of the horizontal resharding process. It drives the process from start to finish
// and updates the progress of each step.
func (hw *HorizontalReshardingWorkflow) executeReshardPerSource(perhw *PerShardHorizontalResharding) error {
	sourceKeyspaceShard := topoproto.KeyspaceShardString(perhw.parent.keyspace, perhw.data.SourceShard)
	var destinationKeyspaceShards []string
	for _, destShard := range perhw.data.DestinationShards {
		destinationKeyspaceShards = append(destinationKeyspaceShards, topoproto.KeyspaceShardString(perhw.parent.keyspace, destShard))
	}

	// Step 1: Run CopySchemaShard to copy the schema of the first source shard to all destination shards
	// TODO(yipeiw): excludeTable information can be added to UI input parameters, s.t the user can customized excluded tables during resharding.
	for _, d := range destinationKeyspaceShards {
		args := []string{"CopySchemaShard", sourceKeyspaceShard, d}
		if err := vtctl.RunCommand(hw.ctx, hw.wr, args); err != nil {
			errorMessage := fmt.Sprintf("Horizontal Resharding: error in CopySchemaShardFromShard from %s to %s: %v",
				sourceKeyspaceShard, d, err)
			setUIMessage(errorMessage, perhw.copySchemaUINode, perhw.shardUILogger)
			return err
		}
		progressMessage := fmt.Sprintf("Horizontal Resharding: CopySchemaShardFromShard from %s to %s is finished", sourceKeyspaceShard, d)
		setUIMessage(progressMessage, perhw.copySchemaUINode, perhw.shardUILogger)
	}

	// Step 2: Run SplitClone to clone the data within a keyspace from source shards to destination shards.
	// Reset the vtworker to avoid error if vtworker command has been called elsewhere.
	// This is because vtworker class doesn't cleanup the environment after execution.

	automation.ExecuteVtworker(context.TODO(), perhw.data.Vtworker, []string{"Reset"})
	// The flag min_healthy_rdonly_tablets is set to 1 (default value is 2).
	// Therefore, we can reuse the normal end to end test setting, which has only 1 rdonly tablet
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets=1", sourceKeyspaceShard}
	if _, err := automation.ExecuteVtworker(context.TODO(), perhw.data.Vtworker, args); err != nil {
		errorMessage := fmt.Sprintf("Horizontal resharding: error in SplitClone in keyspace %s:%v",
			perhw.parent.keyspace, err)
		setUIMessage(errorMessage, perhw.splitCloneUINode, perhw.shardUILogger)
		return err
	}
	setUIMessage("Horizontal resharding: SplitClone is finished", perhw.splitCloneUINode, perhw.shardUILogger)

	// Wait for filtered replication task
	for _, d := range destinationKeyspaceShards {
		args := []string{"WaitForFilteredReplication", d}
		if err := vtctl.RunCommand(hw.ctx, hw.wr, args); err != nil {
			setUIMessage(fmt.Sprintf("Horizontal Resharding: error in WaitForFilteredReplication: %v", err), perhw.splitCloneUINode, perhw.shardUILogger)
			return err
		}
		setUIMessage("Horizontal Resharding:WaitForFilteredReplication is finished on "+d, perhw.splitCloneUINode, perhw.shardUILogger)
	}

	// Step 3: Run SplitDiff for every destination shard to the source and destination
	// to ensure all the data is present and correct
	for _, d := range destinationKeyspaceShards {
		automation.ExecuteVtworker(hw.ctx, perhw.data.Vtworker, []string{"Reset"})
		args := []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", d}
		_, err := automation.ExecuteVtworker(context.TODO(), perhw.data.Vtworker, args)
		if err != nil {
			return err
		}
	}
	setUIMessage("Horizontal resharding: SplitDiff is finished", perhw.splitDiffUINode, perhw.shardUILogger)

	// Step 4: Run MigrateServedTypes to switch over to serving from the new shards
	servedTypeParams := []string{"rdonly", "replica", "master"}
	for _, servedType := range servedTypeParams {
		args := []string{"MigrateServedTypes", sourceKeyspaceShard, servedType}
		if err := vtctl.RunCommand(hw.ctx, hw.wr, args); err != nil {
			errorMessage := fmt.Sprintf("Horizontal Resharding: error in MigrateServedTypes on servedType %s: %v", servedType, err)
			setUIMessage(errorMessage, perhw.migrateUINode, perhw.shardUILogger)
			return err
		}
		progressMessage := fmt.Sprintf("Horizontal Resharding: MigrateServedTypes is finished on tablet %s serve type %s ", sourceKeyspaceShard, servedType)
		setUIMessage(progressMessage, perhw.migrateUINode, perhw.shardUILogger)
	}

	// Report successful information
	progressMessage := fmt.Sprintf("Horizontal Resharding is finished, source shard %v is resharded into destination shards %v.", sourceKeyspaceShard, destinationKeyspaceShards)
	progressMessage += "The source shard is migrated and the destination shards are serving"
	setUIMessage(progressMessage, perhw.shardUINode, perhw.shardUILogger)
	return nil
}

func (hw *HorizontalReshardingWorkflow) setUIMessage(message string) {
	log.Infof("Schema swap on keyspace %v: %v", hw.keyspace, message)
	hw.logger.Infof(message)
	hw.rootUINode.Log = hw.logger.String()
	hw.rootUINode.Message = message
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)
}

func setUIMessage(message string, uiNode *workflow.Node, logger *logutil.MemoryLogger) {
	log.Infof(message)
	logger.Infof(message)
	uiNode.Log = logger.String()
	uiNode.Message = message
	uiNode.BroadcastChanges(false /* updateChildren */)
}

// WorkflowFactory is the factory to register the HorizontalReshard Workflows
type WorkflowFactory struct{}

// Register registers horizontal_resharding as a valid factory in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardingFactoryName, &WorkflowFactory{})
}

// Init is part of the workflow.Factory interface.
func (*WorkflowFactory) Init(workflowProto *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(horizontalReshardingFactoryName, flag.ContinueOnError)
	keyspace := subFlags.String("keyspace", "", "Name of keyspace to perform horizontal resharding")
	vtworkerStr := subFlags.String("vtworkers", "", "A list of address of vtworkers")

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
	rootNode.Message = "This workflow is a workflow to execute horizontal resharding automatically"
	data := &HorizontalReshardingData{}
	if err := json.Unmarshal(workflowProto.Data, data); err != nil {
		return nil, err
	}

	return &HorizontalReshardingWorkflow{
		keyspace:   data.Keyspace,
		vtworkers:  data.Vtworkers,
		rootUINode: rootNode,
		logger:     logutil.NewMemoryLogger(),
	}, nil
}
