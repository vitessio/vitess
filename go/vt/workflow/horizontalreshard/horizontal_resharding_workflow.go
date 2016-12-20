package horizontalreshard

// Package horizontalreshard contains a workflow for automatic horizontal resharding.
// For implementation simplification, the user need to set up the vtworker and sets sharding key before using this workflow.
// The Input arguments for the workflow UI are:
//   - keyspace: string, keyspace the resharding is working
//   - source shards: string, the source shard name to be resharded (example: '0')
//   - destination shards: string, the destination shards, delimited by ',' (example if resharding to 2 shards: -80,80-)
//   - vtworker address: string, vtworker server address

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/automation"
	"github.com/youtube/vitess/go/vt/logutil"
	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

const (
	horizontalReshardFactoryName = "horizontal_resharding"
)

// ReshardWorkflowData is the data structure to store resharding arguments
type ReshardWorkflowData struct {
	Keyspace          string
	SourceShards      []string
	DestinationShards []string
	VTWorkerServer    string
}

// Workflow contains meta-information and methods controlling horizontal resharding workflow.
type Workflow struct {
	// manager is the current Manager.
	// TO DO: it can be used to save checkpointer
	manager *workflow.Manager

	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode *workflow.Node

	info *ReshardWorkflowData
}

// Run is a part of workflow.Workflow interface. This execute the horizontal resharding process and update UI message
func (hw *Workflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	hw.rootUINode.Display = workflow.NodeDisplayDeterminate
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)

	wr := wrangler.New(logutil.NewConsoleLogger(), topo.GetServer(), tmclient.NewTabletManagerClient())

	err := hw.executeReshard(ctx, wr)
	if err != nil {
		return err
	}
	return nil
}

// executeReshard is the main entry point of the horizontal resharding process. It drived the process from start to finish
// and updates the progress of each step.
func (hw *Workflow) executeReshard(ctx context.Context, wr *wrangler.Wrangler) error {
	sourceKeyspace := hw.info.Keyspace
	destKeyspace := hw.info.Keyspace
	sourceShardInfo := topoproto.KeyspaceShardString(sourceKeyspace, hw.info.SourceShards[0])
	numDestShards := len(hw.info.DestinationShards)
	destShardInfos := make([]string, numDestShards)
	for i := 0; i < numDestShards; i++ {
		destShardInfos[i] = topoproto.KeyspaceShardString(destKeyspace, hw.info.DestinationShards[i])
	}

	// Step 1: Run CopySchemaShard to copy the schema of source shard to all destination shards
	// TODO: excludeTable information can be added to UI input parameters, s.t the user can customized excluded tables during resharding.
	for _, destShardInfo := range destShardInfos {
		args := []string{"CopySchemaShard", sourceShardInfo, destShardInfo}
		if err := vtctl.RunCommand(ctx, wr, args); err != nil {
			errorMessage := fmt.Sprintf("Horizontal Resharding: error in CopySchemaShardFromShard from %s to %s:",
				sourceShardInfo, destShardInfo) + err.Error()
			hw.setUIMessage(errorMessage)
			return err
		}
		progressMessage := fmt.Sprintf("Horizontal Resharding: CopySchemaShardFromShard from %s to %s is finished", sourceShardInfo, destShardInfo)
		hw.setUIMessage(progressMessage)
	}

	// Step 2: Run SplitClone to clone the data within a keyspace from source shards to destination shards.
	vtworkerServer := hw.info.VTWorkerServer
	// Reset the vtworker to avoid error if vtworker command has been called elsewhere.
	// This is because vtworker class doesn't cleanup the environment after execution.
	automation.ExecuteVtworker(context.TODO(), vtworkerServer, []string{"Reset"})
	// The flag min_healthy_rdonly_tablets is set to 1 (default value is 2).
	// Therefore, we can reuse the normal end to end test setting, which has only 1 rdonly tablet
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets=1", sourceShardInfo}
	if _, err := automation.ExecuteVtworker(context.TODO(), vtworkerServer, args); err != nil {
		errorMessage := fmt.Sprintf("Horizontal resharding: error in SplitClone in keyspace %s",
			sourceKeyspace) + err.Error()
		hw.setUIMessage(errorMessage)
		return err
	}
	hw.setUIMessage("Horizontal resharding: SplitClone is finished")

	// Wait for filtered replication task
	for _, destShardInfo := range destShardInfos {
		args := []string{"WaitForFilteredReplication", destShardInfo}
		if err := vtctl.RunCommand(ctx, wr, args); err != nil {
			hw.setUIMessage("Horizontal Resharding: error in WaitForFilteredReplication:" + err.Error())
			return err
		}
		hw.setUIMessage("Horizontal Resharding:WaitForFilteredReplication is finished on " + destShardInfo)
	}

	// Step 3: Run SplitDiff for every destination shard to the source and destination
	// to ensure all the data is present and correct
	for _, destShardInfo := range destShardInfos {
		automation.ExecuteVtworker(context.TODO(), vtworkerServer, []string{"Reset"})
		args := []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", destShardInfo}
		_, err := automation.ExecuteVtworker(context.TODO(), vtworkerServer, args)
		if err != nil {
			return err
		}
	}
	hw.setUIMessage("Horizontal resharding: SplitDiff is finished")

	// Step 4: Run MigrateServedTypes to switch over to serving from the new shards
	servedTypeParams := []string{"rdonly", "replica", "master"}
	for _, servedType := range servedTypeParams {
		args := []string{"MigrateServedTypes", sourceShardInfo, servedType}
		if err := vtctl.RunCommand(ctx, wr, args); err != nil {
			errorMessage := fmt.Sprintf("Horizontal Resharding: error in MigrateServedTypes on servedType %s", servedType) + err.Error()
			hw.setUIMessage(errorMessage)
			return err
		}
		progressMessage := fmt.Sprintf("Horizontal Resharding: MigrateServedTypes is finished on tablet %s serve type %s ", sourceShardInfo, servedType)
		hw.setUIMessage(progressMessage)
	}

	// Report successful information
	progressMessage := fmt.Sprintf("Horizontal Resharding is finished, source shard %v is resharded into destination shards %v.", sourceShardInfo, destShardInfos)
	progressMessage += "The source shard is migrated and the destination shards are serving"
	hw.setUIMessage(progressMessage)
	return nil
}

func (hw *Workflow) setUIMessage(message string) {
	log.Infof("Horizontal resharding on keyspace %v: %v", hw.info.Keyspace, message)
	hw.logger.Infof(message)
	hw.rootUINode.Log = hw.logger.String()
	hw.rootUINode.Message = message
	hw.rootUINode.BroadcastChanges(false /* updateChildren */)
}

// WorkflowFactory is the factory to register the HorizontalReshard Workflows
type WorkflowFactory struct{}

// Register registers horizontal_resharding as a valid factory in the workflow framework.
func Register() {
	workflow.Register(horizontalReshardFactoryName, &WorkflowFactory{})
}

// Init is part of the workflow.Factory interface.
func (*WorkflowFactory) Init(workflowProto *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(horizontalReshardFactoryName, flag.ContinueOnError)
	keyspace := subFlags.String("keyspace", "", "Name of keyspace to perform horizontal resharding")
	// The shards should be separated by ","
	sourceShards := subFlags.String("source_shard_list", "", "list of source shards of resharding")
	destinationShards := subFlags.String("destination_shard_list", "", "list of destination shards of resharding")
	vtworkerServ := subFlags.String("vtworker_server_address", "", "address of vtworker")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *keyspace == "" || *sourceShards == "" || *destinationShards == "" {
		return fmt.Errorf("Keyspace name, source shards, destination shrads query must be provided for horizontal resharding")
	}

	sourceShardList := strings.Split(*sourceShards, ",")
	destShardList := strings.Split(*destinationShards, ",")
	workflowProto.Name = fmt.Sprintf("Horizontal resharding on keyspace %s", *keyspace)
	data := &ReshardWorkflowData{
		Keyspace:          *keyspace,
		SourceShards:      sourceShardList,
		DestinationShards: destShardList,
		VTWorkerServer:    *vtworkerServ,
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
	data := &ReshardWorkflowData{}
	if err := json.Unmarshal(workflowProto.Data, data); err != nil {
		return nil, err
	}

	return &Workflow{
		info:       data,
		rootUINode: rootNode,
		logger:     logutil.NewMemoryLogger(),
	}, nil
}
