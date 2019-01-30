/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schemaswap

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	delayBetweenErrors = flag.Duration("schema_swap_delay_between_errors", time.Minute,
		"time to wait after a retryable error happened in the schema swap process")
	adminQueryTimeout = flag.Duration("schema_swap_admin_query_timeout", 30*time.Second,
		"timeout for SQL queries used to save and retrieve meta information for schema swap process")
	backupConcurrency = flag.Int("schema_swap_backup_concurrency", 4,
		"number of simultaneous compression/checksum jobs to run for seed backup during schema swap")
	reparentTimeout = flag.Duration("schema_swap_reparent_timeout", 30*time.Second,
		"timeout to wait for slaves when doing reparent during schema swap")

	errOnlyMasterLeft   = errors.New("Only master is left to swap schema")
	errNoBackupWithSwap = errors.New("Restore from backup cannot pick up new schema")
)

const (
	lastStartedMetadataName  = "LastStartedSchemaSwap"
	lastFinishedMetadataName = "LastFinishedSchemaSwap"
	currentSQLMetadataName   = "CurrentSchemaSwapSQL"
	lastAppliedMetadataName  = "LastAppliedSchemaSwap"

	workflowFactoryName = "schema_swap"
)

// Swap contains meta-information and methods controlling schema swap process as a whole.
type Swap struct {
	// ctx is the context of the whole schema swap process. Once this context is cancelled
	// the schema swap process stops.
	ctx context.Context

	// workflowManager is the manager for the workflow represented by this Swap object.
	workflowManager *workflow.Manager
	// rootUINode is the root node in workflow UI representing this schema swap.
	rootUINode *workflow.Node
	// uiLogger is the logger collecting logs that will be displayed in the UI.
	uiLogger *logutil.MemoryLogger

	// retryMutex is a mutex protecting access to retryChannel and to Actions in rootUINode.
	retryMutex sync.Mutex
	// retryChannel is a channel that gets closed when Retry button is pressed.
	retryChannel chan struct{}

	// keyspace is the name of the keyspace on which schema swap process operates.
	keyspace string
	// sql is the query representing schema change being propagated via schema swap process.
	sql string
	// swapID is the id of the executed schema swap. This is recorded in the database to
	// distinguish backups from before and after an applied schema change, and to prevent
	// several schema swaps from running at the same time.
	swapID uint64

	// topoServer is the topo server implementation used to discover the topology of the keyspace.
	topoServer *topo.Server
	// tabletClient is the client implementation used by the schema swap process to control
	// all tablets in the keyspace.
	tabletClient tmclient.TabletManagerClient

	// allShards is a list of schema swap objects for each shard in the keyspace.
	allShards []*shardSchemaSwap
}

// shardSchemaSwap contains data related to schema swap happening on a specific shard.
type shardSchemaSwap struct {
	// parent is the structure with meta-information about the whole schema swap process.
	parent *Swap
	// shardName is the name of the shard this struct operates on.
	shardName string

	// The following is a set of UI nodes representing this shard.

	shardUINode       *workflow.Node
	applySchemaUINode *workflow.Node
	backupUINode      *workflow.Node
	propagationUINode *workflow.Node
	reparentUINode    *workflow.Node

	// shardUILogger is the logger collecting logs that will be displayed in the UI
	// for the whole shard.
	shardUILogger *logutil.MemoryLogger
	// propagationUILogger is the logger collecting logs that will be displayed in the UI
	// for the propagation task.
	propagationUILogger *logutil.MemoryLogger

	// numTabletsTotal is the total number of tablets in the shard.
	numTabletsTotal int
	// numTabletsSwapped is number of tablets that have schema swapped already.
	numTabletsSwapped int

	// tabletHealthCheck watches after the healthiness of all tablets in the shard.
	tabletHealthCheck discovery.HealthCheck
	// tabletWatchers contains list of topology watchers monitoring changes in the shard
	// topology. There are several of them because the watchers are per-cell.
	tabletWatchers []*discovery.TopologyWatcher

	// allTabletsLock is a mutex protecting access to contents of health check related
	// variables below.
	allTabletsLock sync.RWMutex
	// allTablets is the list of all tablets on the shard mapped by the key provided
	// by discovery. The contents of the map is guarded by allTabletsLock.
	allTablets map[string]*discovery.TabletStats
	// healthWaitingTablet is a key (the same key as used in allTablets) of a tablet that
	// is currently being waited on to become healthy and to catch up with replication.
	// The variable is guarded by allTabletsLock.
	healthWaitingTablet string
	// healthWaitingChannel is a channel that should be closed when the tablet that is
	// currently being waited on becomes healthy and catches up with replication. The
	// variable is set to nil when nothing is waited on at the moment. The variable is
	// protected by allTabletsLock.
	healthWaitingChannel *chan interface{}
}

// shardSwapMetadata contains full metadata about schema swaps on a certain shard.
type shardSwapMetadata struct {
	err error
	// IDs of last started and last finished schema swap on the shard
	lastStartedSwap, lastFinishedSwap uint64
	// currentSQL is the SQL of the currentlr running schema swap (if there is any)
	currentSQL string
}

// SwapWorkflowFactory is a factory to create Swap objects as workflows.
type SwapWorkflowFactory struct{}

// swapWorkflowData contains info about a new schema swap that is about to be started.
type swapWorkflowData struct {
	Keyspace, SQL string
}

// RegisterWorkflowFactory registers schema swap as a valid factory in the workflow framework.
func RegisterWorkflowFactory() {
	workflow.Register(workflowFactoryName, &SwapWorkflowFactory{})
}

// Init is a part of workflow.Factory interface. It initializes a Workflow protobuf object.
func (*SwapWorkflowFactory) Init(_ *workflow.Manager, workflowProto *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(workflowFactoryName, flag.ContinueOnError)

	keyspace := subFlags.String("keyspace", "", "Name of a keyspace to perform schema swap on")
	sql := subFlags.String("sql", "", "Query representing schema change being pushed by schema swap")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *keyspace == "" || *sql == "" {
		return fmt.Errorf("Keyspace name and SQL query must be provided for schema swap")
	}

	workflowProto.Name = fmt.Sprintf("Schema swap on keyspace %s", *keyspace)
	data := &swapWorkflowData{
		Keyspace: *keyspace,
		SQL:      *sql,
	}
	var err error
	workflowProto.Data, err = json.Marshal(data)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is a part of workflow.Factory interface. It instantiates workflow.Workflow object from
// workflowpb.Workflow protobuf object.
func (*SwapWorkflowFactory) Instantiate(_ *workflow.Manager, workflowProto *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	data := &swapWorkflowData{}
	if err := json.Unmarshal(workflowProto.Data, data); err != nil {
		return nil, err
	}
	rootNode.Message = fmt.Sprintf("Schema swap is executed on the keyspace %s", data.Keyspace)

	return &Swap{
		keyspace:   data.Keyspace,
		sql:        data.SQL,
		rootUINode: rootNode,
		uiLogger:   logutil.NewMemoryLogger(),
	}, nil
}

// Run is a part of workflow.Workflow interface. This is the main entrance point of the schema swap workflow.
func (schemaSwap *Swap) Run(ctx context.Context, manager *workflow.Manager, workflowInfo *topo.WorkflowInfo) error {
	schemaSwap.ctx = ctx
	schemaSwap.workflowManager = manager
	schemaSwap.topoServer = manager.TopoServer()
	schemaSwap.tabletClient = tmclient.NewTabletManagerClient()

	log.Infof("Starting schema swap on keyspace %v with the following SQL: %v", schemaSwap.keyspace, schemaSwap.sql)

	for {
		err := schemaSwap.executeSwap()
		if err == nil {
			return nil
		}
		// If context is cancelled then return right away, otherwise move on to allow
		// user to retry.
		select {
		case <-schemaSwap.ctx.Done():
			schemaSwap.setUIMessage(fmt.Sprintf("Error: %v", err))
			return err
		default:
		}

		schemaSwap.retryMutex.Lock()
		retryAction := &workflow.Action{
			Name:  "Retry",
			State: workflow.ActionStateEnabled,
			Style: workflow.ActionStyleWaiting,
		}
		schemaSwap.rootUINode.Actions = []*workflow.Action{retryAction}
		schemaSwap.rootUINode.Listener = schemaSwap
		schemaSwap.retryChannel = make(chan struct{})
		schemaSwap.retryMutex.Unlock()
		// setUIMessage broadcasts changes to rootUINode.
		schemaSwap.setUIMessage(fmt.Sprintf("Error: %v", err))

		select {
		case <-schemaSwap.retryChannel:
			schemaSwap.setUIMessage("Retrying schema swap after an error")
			continue
		case <-schemaSwap.ctx.Done():
			schemaSwap.closeRetryChannel()
			return schemaSwap.ctx.Err()
		}
	}
}

// closeRetryChannel closes the retryChannel and empties the Actions list in the rootUINode
// to indicate that the channel is closed and Retry action is not waited for anymore.
func (schemaSwap *Swap) closeRetryChannel() {
	schemaSwap.retryMutex.Lock()
	defer schemaSwap.retryMutex.Unlock()

	if len(schemaSwap.rootUINode.Actions) != 0 {
		schemaSwap.rootUINode.Actions = []*workflow.Action{}
		close(schemaSwap.retryChannel)
	}
}

// Action is a part of workflow.ActionListener interface. It registers "Retry" actions
// from UI.
func (schemaSwap *Swap) Action(ctx context.Context, path, name string) error {
	if name != "Retry" {
		return fmt.Errorf("Unknown action on schema swap: %v", name)
	}
	schemaSwap.closeRetryChannel()
	schemaSwap.rootUINode.BroadcastChanges(false /* updateChildren */)
	return nil
}

// executeSwap is the main entry point of the schema swap process. It drives the process from start
// to finish, including possible restart of already started process. In the latter case the
// method should be just called again and it will pick up already started process. The only
// input argument is the SQL statements that comprise the schema change that needs to be
// pushed using the schema swap process.
func (schemaSwap *Swap) executeSwap() error {
	schemaSwap.setUIMessage("Initializing schema swap")
	if len(schemaSwap.allShards) == 0 {
		if err := schemaSwap.createShardObjects(); err != nil {
			return err
		}
		schemaSwap.rootUINode.BroadcastChanges(true /* updateChildren */)
	}
	if err := schemaSwap.initializeSwap(); err != nil {
		return err
	}
	errHealthWatchers := schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.startHealthWatchers()
		})
	// Note: this defer statement is before the error is checked because some shards may
	// succeed while others fail. We should try to stop health watching on all shards no
	// matter if there was some failure or not.
	defer schemaSwap.stopAllHealthWatchers()
	if errHealthWatchers != nil {
		return errHealthWatchers
	}

	schemaSwap.setUIMessage("Applying schema change on seed tablets")
	err := schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			shard.setShardInProgress(true)
			err := shard.applySeedSchemaChange()
			shard.setShardInProgress(false)
			return err
		})
	if err != nil {
		return err
	}
	schemaSwap.setUIMessage("Taking seed backups")
	err = schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			shard.setShardInProgress(true)
			err := shard.takeSeedBackup()
			shard.setShardInProgress(false)
			return err
		})
	if err != nil {
		return err
	}
	schemaSwap.setUIMessage("Propagating backups to non-master tablets")
	err = schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			shard.setShardInProgress(true)
			err := shard.propagateToAllTablets(false /* withMasterReparent */)
			shard.setShardInProgress(false)
			return err
		})
	if err != nil {
		return err
	}
	schemaSwap.setUIMessage("Propagating backups to master tablets")
	err = schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			shard.setShardInProgress(true)
			err := shard.propagateToAllTablets(true /* withMasterReparent */)
			shard.setShardInProgress(false)
			return err
		})
	if err != nil {
		return err
	}
	schemaSwap.setUIMessage("Finalizing schema swap")
	err = schemaSwap.finalizeSwap()
	if err != nil {
		return err
	}
	for _, shard := range schemaSwap.allShards {
		shard.shardUINode.State = workflowpb.WorkflowState_Done
	}
	schemaSwap.rootUINode.BroadcastChanges(true /* updateChildren */)
	schemaSwap.setUIMessage("Schema swap is finished")
	return nil
}

// setUIMessage updates message on the schema swap's root UI node and broadcasts changes.
func (schemaSwap *Swap) setUIMessage(message string) {
	log.Infof("Schema swap on keyspace %v: %v", schemaSwap.keyspace, message)
	schemaSwap.uiLogger.Infof(message)
	schemaSwap.rootUINode.Log = schemaSwap.uiLogger.String()
	schemaSwap.rootUINode.Message = message
	schemaSwap.rootUINode.BroadcastChanges(false /* updateChildren */)
}

// runOnAllShards is a helper method that executes the passed function for all shards in parallel.
// The method returns no error if the function succeeds on all shards. If on any of the shards
// the function fails then the method returns error. If several shards return error then only one
// of them is returned.
func (schemaSwap *Swap) runOnAllShards(shardFunc func(shard *shardSchemaSwap) error) error {
	var errorRecorder concurrency.AllErrorRecorder
	var waitGroup sync.WaitGroup
	for _, shardSwap := range schemaSwap.allShards {
		waitGroup.Add(1)
		go func(shard *shardSchemaSwap) {
			defer waitGroup.Done()
			errorRecorder.RecordError(shardFunc(shard))
		}(shardSwap)
	}
	waitGroup.Wait()
	return errorRecorder.Error()
}

// createShardObjects creates per-shard swap objects for all shards in the keyspace.
func (schemaSwap *Swap) createShardObjects() error {
	shardsList, err := schemaSwap.topoServer.FindAllShardsInKeyspace(schemaSwap.ctx, schemaSwap.keyspace)
	if err != nil {
		return err
	}
	for _, shardInfo := range shardsList {
		shardSwap := &shardSchemaSwap{
			parent:    schemaSwap,
			shardName: shardInfo.ShardName(),
			shardUINode: &workflow.Node{
				Name:     fmt.Sprintf("Shard %v", shardInfo.ShardName()),
				PathName: shardInfo.ShardName(),
				State:    workflowpb.WorkflowState_Running,
			},
			applySchemaUINode: &workflow.Node{
				Name:     "Apply schema on seed tablet",
				PathName: "apply_schema",
			},
			backupUINode: &workflow.Node{
				Name:     "Take seed backup",
				PathName: "backup",
			},
			propagationUINode: &workflow.Node{
				Name:     "Propagate backup",
				PathName: "propagate",
			},
			reparentUINode: &workflow.Node{
				Name:     "Reparent from old master",
				PathName: "reparent",
			},
			shardUILogger:       logutil.NewMemoryLogger(),
			propagationUILogger: logutil.NewMemoryLogger(),
		}
		schemaSwap.rootUINode.Children = append(schemaSwap.rootUINode.Children, shardSwap.shardUINode)
		shardSwap.shardUINode.Children = []*workflow.Node{
			shardSwap.applySchemaUINode,
			shardSwap.backupUINode,
			shardSwap.propagationUINode,
			shardSwap.reparentUINode,
		}
		schemaSwap.allShards = append(schemaSwap.allShards, shardSwap)
	}
	return nil
}

// stopAllHealthWatchers stops watching for health on each shard. It's separated into a separate
// function mainly to make "defer" statement where it's used simpler.
func (schemaSwap *Swap) stopAllHealthWatchers() {
	schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			shard.stopHealthWatchers()
			return nil
		})
}

// initializeSwap starts the schema swap process. If there is already a schema swap process started
// the method just picks up that. Otherwise it starts a new one and writes into the database that
// the process was started.
func (schemaSwap *Swap) initializeSwap() error {
	var waitGroup sync.WaitGroup
	metadataList := make([]shardSwapMetadata, len(schemaSwap.allShards))
	for i, shard := range schemaSwap.allShards {
		waitGroup.Add(1)
		go shard.readShardMetadata(&metadataList[i], &waitGroup)
	}
	waitGroup.Wait()

	var recorder concurrency.AllErrorRecorder
	var lastFinishedSwapID uint64
	for i, metadata := range metadataList {
		if metadata.err != nil {
			recorder.RecordError(metadata.err)
		} else if metadata.lastStartedSwap < metadata.lastFinishedSwap || metadata.lastStartedSwap > metadata.lastFinishedSwap+1 {
			recorder.RecordError(fmt.Errorf(
				"Bad swap metadata on shard %v: LastFinishedSchemaSwap=%v, LastStartedSchemaSwap=%v",
				schemaSwap.allShards[i].shardName, metadata.lastFinishedSwap, metadata.lastStartedSwap))
		} else if metadata.lastStartedSwap != metadata.lastFinishedSwap {
			if metadata.currentSQL != schemaSwap.sql {
				recorder.RecordError(fmt.Errorf(
					"Shard %v has an already started schema swap with a different set of SQL statements",
					schemaSwap.allShards[i].shardName))
			}
		}
		if lastFinishedSwapID == 0 || metadata.lastFinishedSwap < lastFinishedSwapID {
			lastFinishedSwapID = metadata.lastFinishedSwap
		}
	}
	if recorder.HasErrors() {
		return recorder.Error()
	}

	schemaSwap.swapID = lastFinishedSwapID + 1
	var haveNotStartedSwap, haveFinishedSwap bool
	for i, metadata := range metadataList {
		if metadata.lastStartedSwap == metadata.lastFinishedSwap {
			// The shard doesn't have schema swap started yet or it's already finished.
			if schemaSwap.swapID != metadata.lastFinishedSwap && schemaSwap.swapID != metadata.lastFinishedSwap+1 {
				recorder.RecordError(fmt.Errorf(
					"Shard %v has last finished swap id euqal to %v which doesn't align with swap id for the keyspace equal to %v",
					schemaSwap.allShards[i].shardName, metadata.lastFinishedSwap, schemaSwap.swapID))
			} else if schemaSwap.swapID == metadata.lastFinishedSwap {
				haveFinishedSwap = true
			} else {
				haveNotStartedSwap = true
			}
		} else if schemaSwap.swapID != metadata.lastStartedSwap {
			recorder.RecordError(fmt.Errorf(
				"Shard %v has an already started schema swap with an id %v, while for the keyspace it should be equal to %v",
				schemaSwap.allShards[i].shardName, metadata.lastStartedSwap, schemaSwap.swapID))
		}
	}
	if haveNotStartedSwap && haveFinishedSwap {
		recorder.RecordError(errors.New("impossible state: there are shards with finished swap and shards where swap was not started"))
	}
	if recorder.HasErrors() {
		return recorder.Error()
	}

	if haveNotStartedSwap {
		return schemaSwap.runOnAllShards(
			func(shard *shardSchemaSwap) error {
				return shard.writeStartedSwap()
			})
	}
	return nil
}

// finalizeSwap finishes the completed swap process by modifying the database to register the completion.
func (schemaSwap *Swap) finalizeSwap() error {
	return schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.writeFinishedSwap()
		})
}

// setShardInProgress changes the way shard looks in the UI. Either it's "in progress" (has
// animated progress bar) or not (has empty look without progress bar).
func (shardSwap *shardSchemaSwap) setShardInProgress(value bool) {
	if value {
		shardSwap.shardUINode.Display = workflow.NodeDisplayIndeterminate
	} else {
		shardSwap.shardUINode.Display = workflow.NodeDisplayNone
	}
	shardSwap.shardUINode.BroadcastChanges(false /* updateChildren */)
}

// markStepInProgress marks one step of the shard schema swap workflow as running.
func (shardSwap *shardSchemaSwap) markStepInProgress(uiNode *workflow.Node) {
	uiNode.Message = ""
	uiNode.State = workflowpb.WorkflowState_Running
	uiNode.Display = workflow.NodeDisplayIndeterminate
	uiNode.BroadcastChanges(false /* updateChildren */)
}

// addShardLog prints the message into logs and adds it into logs displayed in UI on the
// shard node.
func (shardSwap *shardSchemaSwap) addShardLog(message string) {
	log.Infof("Shard %v: %v", shardSwap.shardName, message)
	shardSwap.shardUILogger.Infof(message)
	shardSwap.shardUINode.Log = shardSwap.shardUILogger.String()
	shardSwap.shardUINode.BroadcastChanges(false /* updateChildren */)
}

// markStepDone marks one step of the shard schema swap workflow as finished successfully
// or with an error.
func (shardSwap *shardSchemaSwap) markStepDone(uiNode *workflow.Node, err *error) {
	if *err != nil {
		msg := fmt.Sprintf("Error: %v", *err)
		shardSwap.addShardLog(msg)
		uiNode.Message = msg
	}
	uiNode.State = workflowpb.WorkflowState_Done
	uiNode.Display = workflow.NodeDisplayNone
	uiNode.BroadcastChanges(false /* updateChildren */)
}

// getMasterTablet returns the tablet that is currently master on the shard.
func (shardSwap *shardSchemaSwap) getMasterTablet() (*topodatapb.Tablet, error) {
	topoServer := shardSwap.parent.topoServer
	shardInfo, err := topoServer.GetShard(shardSwap.parent.ctx, shardSwap.parent.keyspace, shardSwap.shardName)
	if err != nil {
		return nil, err
	}
	tabletInfo, err := topoServer.GetTablet(shardSwap.parent.ctx, shardInfo.MasterAlias)
	if err != nil {
		return nil, err
	}
	return tabletInfo.Tablet, nil
}

// readShardMetadata reads info about schema swaps on this shard from _vt.shard_metadata table.
func (shardSwap *shardSchemaSwap) readShardMetadata(metadata *shardSwapMetadata, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	tablet, err := shardSwap.getMasterTablet()
	if err != nil {
		metadata.err = err
		return
	}
	query := fmt.Sprintf(
		"SELECT name, value FROM _vt.shard_metadata WHERE name in ('%s', '%s', '%s')",
		lastStartedMetadataName, lastFinishedMetadataName, currentSQLMetadataName)
	queryResult, err := shardSwap.executeAdminQuery(tablet, query, 3 /* maxRows */)
	if err != nil {
		metadata.err = err
		return
	}
	for _, row := range queryResult.Rows {
		switch row[0].ToString() {
		case lastStartedMetadataName:
			swapID, err := sqltypes.ToUint64(row[1])
			if err != nil {
				log.Warningf("Could not parse value of last started schema swap id %v, ignoring the value: %v", row[1], err)
			} else {
				metadata.lastStartedSwap = swapID
			}
		case lastFinishedMetadataName:
			swapID, err := sqltypes.ToUint64(row[1])
			if err != nil {
				log.Warningf("Could not parse value of last finished schema swap id %v, ignoring the value: %v", row[1], err)
			} else {
				metadata.lastFinishedSwap = swapID
			}
		case currentSQLMetadataName:
			metadata.currentSQL = row[1].ToString()
		}
	}
}

// writeStartedSwap registers in the _vt.shard_metadata table in the database the information
// about the new schema swap process being started.
func (shardSwap *shardSchemaSwap) writeStartedSwap() error {
	tablet, err := shardSwap.getMasterTablet()
	if err != nil {
		return err
	}
	queryBuf := bytes.Buffer{}
	queryBuf.WriteString("INSERT INTO _vt.shard_metadata (name, value) VALUES ('")
	queryBuf.WriteString(currentSQLMetadataName)
	queryBuf.WriteString("',")
	sqlValue := sqltypes.NewVarChar(shardSwap.parent.sql)
	sqlValue.EncodeSQL(&queryBuf)
	queryBuf.WriteString(") ON DUPLICATE KEY UPDATE value = ")
	sqlValue.EncodeSQL(&queryBuf)
	_, err = shardSwap.executeAdminQuery(tablet, queryBuf.String(), 0 /* maxRows */)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(
		"INSERT INTO _vt.shard_metadata (name, value) VALUES ('%s', '%d') ON DUPLICATE KEY UPDATE value = '%d'",
		lastStartedMetadataName, shardSwap.parent.swapID, shardSwap.parent.swapID)
	_, err = shardSwap.executeAdminQuery(tablet, query, 0 /* maxRows */)
	return err
}

// writeFinishedSwap registers in the _vt.shard_metadata table in the database that the schema
// swap process has finished.
func (shardSwap *shardSchemaSwap) writeFinishedSwap() error {
	tablet, err := shardSwap.getMasterTablet()
	if err != nil {
		return err
	}
	query := fmt.Sprintf(
		"INSERT INTO _vt.shard_metadata (name, value) VALUES ('%s', '%d') ON DUPLICATE KEY UPDATE value = '%d'",
		lastFinishedMetadataName, shardSwap.parent.swapID, shardSwap.parent.swapID)
	_, err = shardSwap.executeAdminQuery(tablet, query, 0 /* maxRows */)
	if err != nil {
		return err
	}
	query = fmt.Sprintf("DELETE FROM _vt.shard_metadata WHERE name = '%s'", currentSQLMetadataName)
	_, err = shardSwap.executeAdminQuery(tablet, query, 0 /* maxRows */)
	return err
}

// startHealthWatchers launches the topology watchers and health checking to monitor
// all tablets on the shard. Function should be called before the start of the schema
// swap process.
func (shardSwap *shardSchemaSwap) startHealthWatchers() error {
	shardSwap.allTablets = make(map[string]*discovery.TabletStats)

	shardSwap.tabletHealthCheck = discovery.NewHealthCheck(*vtctl.HealthcheckRetryDelay, *vtctl.HealthCheckTimeout)
	shardSwap.tabletHealthCheck.SetListener(shardSwap, true /* sendDownEvents */)

	topoServer := shardSwap.parent.topoServer
	cellList, err := topoServer.GetKnownCells(shardSwap.parent.ctx)
	if err != nil {
		return err
	}
	for _, cell := range cellList {
		watcher := discovery.NewShardReplicationWatcher(
			topoServer,
			shardSwap.tabletHealthCheck,
			cell,
			shardSwap.parent.keyspace,
			shardSwap.shardName,
			*vtctl.HealthCheckTimeout,
			discovery.DefaultTopoReadConcurrency)
		shardSwap.tabletWatchers = append(shardSwap.tabletWatchers, watcher)
	}
	for _, watcher := range shardSwap.tabletWatchers {
		if err := watcher.WaitForInitialTopology(); err != nil {
			return err
		}
	}
	shardSwap.tabletHealthCheck.WaitForInitialStatsUpdates()

	// Wait for all health connections to be either established or get broken.
waitHealthChecks:
	for {
		tabletList := shardSwap.getTabletList()
		for _, tabletStats := range tabletList {
			if tabletStats.Stats == nil && tabletStats.LastError == nil {
				select {
				case <-shardSwap.parent.ctx.Done():
					return shardSwap.parent.ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
				continue waitHealthChecks
			}
		}
		break waitHealthChecks
	}
	return nil
}

// stopHealthWatchers stops the health checking and topology monitoring. The function
// should be called when schema swap process has finished (successfully or with error)
// and this struct is going to be disposed.
func (shardSwap *shardSchemaSwap) stopHealthWatchers() {
	for _, watcher := range shardSwap.tabletWatchers {
		watcher.Stop()
	}
	shardSwap.tabletWatchers = nil
	err := shardSwap.tabletHealthCheck.Close()
	if err != nil {
		log.Errorf("Error closing health checking: %v", err)
	}
}

// isTabletHealthy verifies that the given TabletStats represents a healthy tablet that is
// caught up with replication to a serving level.
func isTabletHealthy(tabletStats *discovery.TabletStats) bool {
	return tabletStats.Stats.HealthError == "" && !discovery.IsReplicationLagHigh(tabletStats)
}

// startWaitingOnUnhealthyTablet registers the tablet as being waited on in a way that
// doesn't race with StatsUpdate(). If the tablet is already healthy then the function
// will return nil as the channel and nil as the error. If the tablet is unhealthy now
// then function will return the channel that will be closed once tablet becomes healthy
// and caught up with replication. Note that the channel is returned so that the caller
// could wait on it without necessity to lock allTabletsLock.
func (shardSwap *shardSchemaSwap) startWaitingOnUnhealthyTablet(tablet *topodatapb.Tablet) (*chan interface{}, error) {
	shardSwap.allTabletsLock.Lock()
	defer shardSwap.allTabletsLock.Unlock()

	tabletKey := discovery.TabletToMapKey(tablet)
	tabletStats, tabletFound := shardSwap.allTablets[tabletKey]
	if !tabletFound {
		return nil, fmt.Errorf("Tablet %v has disappeared while doing schema swap", tablet.Alias)
	}
	if isTabletHealthy(tabletStats) {
		return nil, nil
	}
	waitingChannel := make(chan interface{})
	shardSwap.healthWaitingChannel = &waitingChannel
	shardSwap.healthWaitingTablet = tabletKey
	return shardSwap.healthWaitingChannel, nil
}

// checkWaitingTabletHealthiness verifies whether the provided TabletStats represent the
// tablet that is being waited to become healthy, and notifies the waiting go routine if
// it is the tablet and if it is healthy now.
// The function should be called with shardSwap.allTabletsLock mutex locked.
func (shardSwap *shardSchemaSwap) checkWaitingTabletHealthiness(tabletStats *discovery.TabletStats) {
	if shardSwap.healthWaitingTablet == tabletStats.Key && isTabletHealthy(tabletStats) {
		close(*shardSwap.healthWaitingChannel)
		shardSwap.healthWaitingChannel = nil
		shardSwap.healthWaitingTablet = ""
	}
}

// StatsUpdate is the part of discovery.HealthCheckStatsListener interface. It makes sure
// that when a change of tablet health happens it's recorded in allTablets list, and if
// this is the tablet that is being waited for after restore, the function wakes up the
// waiting go routine.
func (shardSwap *shardSchemaSwap) StatsUpdate(newTabletStats *discovery.TabletStats) {
	shardSwap.allTabletsLock.Lock()
	defer shardSwap.allTabletsLock.Unlock()

	existingStats, found := shardSwap.allTablets[newTabletStats.Key]
	if newTabletStats.Up {
		if found {
			*existingStats = *newTabletStats
		} else {
			shardSwap.allTablets[newTabletStats.Key] = newTabletStats
		}
		shardSwap.checkWaitingTabletHealthiness(newTabletStats)
	} else {
		delete(shardSwap.allTablets, newTabletStats.Key)
	}
}

// getTabletList returns the list of all known tablets in the shard so that the caller
// could operate with it without holding the allTabletsLock.
func (shardSwap *shardSchemaSwap) getTabletList() []discovery.TabletStats {
	shardSwap.allTabletsLock.RLock()
	defer shardSwap.allTabletsLock.RUnlock()

	tabletList := make([]discovery.TabletStats, 0, len(shardSwap.allTablets))
	for _, tabletStats := range shardSwap.allTablets {
		tabletList = append(tabletList, *tabletStats)
	}
	return tabletList
}

// orderTabletsForSwap is an alias for the slice of TabletStats. It implements
// sort.Interface interface so that it's possible to sort the array in the order
// in which schema swap will propagate.
type orderTabletsForSwap []discovery.TabletStats

// Len is part of sort.Interface interface.
func (array orderTabletsForSwap) Len() int {
	return len(array)
}

// Swap is part of sort.Interface interface.
func (array orderTabletsForSwap) Swap(i, j int) {
	array[i], array[j] = array[j], array[i]
}

// getTabletTypeFromStats returns the tablet type saved in the TabletStats object. If there is Target
// data in the TabletStats object then the function returns TabletType from it because it will be more
// up-to-date. But if that's not available then it returns Tablet.Type which will contain data read
// from the topology during initialization of health watchers.
func getTabletTypeFromStats(tabletStats *discovery.TabletStats) topodatapb.TabletType {
	if tabletStats.Target == nil || tabletStats.Target.TabletType == topodatapb.TabletType_UNKNOWN {
		return tabletStats.Tablet.Type
	}
	return tabletStats.Target.TabletType
}

// tabletSortIndex returns a number representing the order in which schema swap will
// be propagated to the tablet. The last should be the master, then tablets doing
// backup/restore to not interrupt that process, then unhealthy tablets (so that we
// don't wait for them and the new schema is propagated to healthy tablets faster),
// then will go 'replica' tablets, and the first will be 'rdonly' and all other
// non-replica and non-master types. The sorting order within each of those 5 buckets
// doesn't matter.
func tabletSortIndex(tabletStats *discovery.TabletStats) int {
	tabletType := getTabletTypeFromStats(tabletStats)
	switch {
	case tabletType == topodatapb.TabletType_MASTER:
		return 5
	case tabletType == topodatapb.TabletType_BACKUP || tabletType == topodatapb.TabletType_RESTORE:
		return 4
	case tabletStats.LastError != nil || tabletStats.Stats == nil || tabletStats.Stats.HealthError != "":
		return 3
	case tabletType == topodatapb.TabletType_REPLICA:
		return 2
	default:
		return 1
	}
}

// Less is part of sort.Interface interface. It should compare too elements of the array.
func (array orderTabletsForSwap) Less(i, j int) bool {
	return tabletSortIndex(&array[i]) < tabletSortIndex(&array[j])
}

// executeAdminQuery executes a query on a given tablet as 'allprivs' user. The query is executed
// using timeout value from --schema_swap_admin_query_timeout flag.
func (shardSwap *shardSchemaSwap) executeAdminQuery(tablet *topodatapb.Tablet, query string, maxRows int) (*sqltypes.Result, error) {
	sqlCtx, cancelSQLCtx := context.WithTimeout(shardSwap.parent.ctx, *adminQueryTimeout)
	defer cancelSQLCtx()

	sqlResultProto, err := shardSwap.parent.tabletClient.ExecuteFetchAsAllPrivs(
		sqlCtx,
		tablet,
		[]byte(query),
		maxRows,
		false /* reloadSchema */)
	if err != nil {
		return nil, err
	}
	return sqltypes.Proto3ToResult(sqlResultProto), nil
}

// isSwapApplied verifies whether the schema swap was already applied to the tablet. It's
// considered to be applied if _vt.local_metadata table has the swap id in the row titled
// 'LastAppliedSchemaSwap'.
func (shardSwap *shardSchemaSwap) isSwapApplied(tablet *topodatapb.Tablet) (bool, error) {
	swapIDResult, err := shardSwap.executeAdminQuery(
		tablet,
		fmt.Sprintf("SELECT value FROM _vt.local_metadata WHERE name = '%s'", lastAppliedMetadataName),
		1 /* maxRows */)
	if err != nil {
		return false, err
	}
	if len(swapIDResult.Rows) == 0 {
		// No such row means we need to apply the swap.
		return false, nil
	}
	swapID, err := sqltypes.ToUint64(swapIDResult.Rows[0][0])
	if err != nil {
		return false, err
	}
	return swapID == shardSwap.parent.swapID, nil
}

// findNextTabletToSwap searches for the next tablet where we need to apply the schema swap. The
// tablets are processed in the order described in tabletSortIndex() function above. If
// it hits any error while searching for the tablet it returns the error to the caller. If the
// only tablet that's left without schema swap applied is the master then the function
// returns the master tablet and errOnlyMasterLeft as the error. When all tablets have schema
// swap applied the function returns nil as the tablet. The function also makes sure that
// values of numTabletsTotal and numTabletsSwapped correctly represent current state of the
// shard (it's convenient to do this in this function because it iterates over all tablets in
// the shard).
func (shardSwap *shardSchemaSwap) findNextTabletToSwap() (*topodatapb.Tablet, error) {
	tabletList := shardSwap.getTabletList()
	sort.Sort(orderTabletsForSwap(tabletList))

	shardSwap.numTabletsTotal = len(tabletList)
	var numTabletsSwapped int
	for _, tabletStats := range tabletList {
		tabletType := getTabletTypeFromStats(&tabletStats)
		if tabletType == topodatapb.TabletType_BACKUP || tabletType == topodatapb.TabletType_RESTORE {
			return nil, fmt.Errorf("tablet %v still has type %v", tabletStats.Tablet.Alias, tabletType)
		}
		swapApplied, err := shardSwap.isSwapApplied(tabletStats.Tablet)
		if err != nil {
			return nil, err
		}
		if swapApplied {
			numTabletsSwapped++
		} else {
			if numTabletsSwapped > shardSwap.numTabletsSwapped {
				// We save the calculated value only if it's bigger than the number
				// we actually swapped, because we might not have seen all the tablets
				// that have the schema swapped at this point.
				shardSwap.numTabletsSwapped = numTabletsSwapped
			}
			if tabletType == topodatapb.TabletType_MASTER {
				return tabletStats.Tablet, errOnlyMasterLeft
			}
			return tabletStats.Tablet, nil
		}
	}
	shardSwap.numTabletsSwapped = numTabletsSwapped
	return nil, nil
}

// undrainSeedTablet undrains the tablet that was used to apply seed schema change,
// and returns it to the same type as it was before.
func (shardSwap *shardSchemaSwap) undrainSeedTablet(seedTablet *topodatapb.Tablet, tabletType topodatapb.TabletType) error {
	_, err := shardSwap.parent.topoServer.UpdateTabletFields(shardSwap.parent.ctx, seedTablet.Alias,
		func(tablet *topodatapb.Tablet) error {
			delete(tablet.Tags, "drain_reason")
			return nil
		})
	if err != nil {
		// This is not a critical error, we'll just log it.
		log.Errorf("Got error trying to set drain_reason on tablet %v: %v", seedTablet.Alias, err)
	}
	err = shardSwap.parent.tabletClient.ChangeType(shardSwap.parent.ctx, seedTablet, tabletType)
	if err != nil {
		return err
	}
	return nil
}

// applySeedSchemaChange chooses a healthy tablet as a schema swap seed and applies the schema change
// on it. In the choice of the seed tablet any RDONLY tablet is preferred, but if there are no healthy
// RDONLY tablets the REPLICA one is chosen. If there are any non-MASTER tablets indicating that the
// schema swap was already applied on them, then the method assumes that it's the seed tablet from the
// already started process and doesn't try to apply the schema change on any other tablet.
func (shardSwap *shardSchemaSwap) applySeedSchemaChange() (err error) {
	shardSwap.markStepInProgress(shardSwap.applySchemaUINode)
	defer shardSwap.markStepDone(shardSwap.applySchemaUINode, &err)

	tabletList := shardSwap.getTabletList()
	sort.Sort(orderTabletsForSwap(tabletList))
	for _, tabletStats := range tabletList {
		swapApplied, err := shardSwap.isSwapApplied(tabletStats.Tablet)
		if err != nil {
			return err
		}
		if swapApplied && getTabletTypeFromStats(&tabletStats) != topodatapb.TabletType_MASTER {
			return nil
		}
	}
	seedTablet := tabletList[0].Tablet
	seedTabletType := getTabletTypeFromStats(&tabletList[0])
	if seedTabletType == topodatapb.TabletType_MASTER {
		return fmt.Errorf("the only candidate for a schema swap seed is the master %v, aborting", seedTablet)
	}
	shardSwap.addShardLog(fmt.Sprintf("Applying schema change on the seed tablet %v", seedTablet.Alias))

	// Draining the tablet for it to not be used for execution of user queries.
	err = shardSwap.parent.tabletClient.ChangeType(shardSwap.parent.ctx, seedTablet, topodatapb.TabletType_DRAINED)
	if err != nil {
		return err
	}
	_, err = shardSwap.parent.topoServer.UpdateTabletFields(shardSwap.parent.ctx, seedTablet.Alias,
		func(tablet *topodatapb.Tablet) error {
			if tablet.Tags == nil {
				tablet.Tags = make(map[string]string)
			}
			tablet.Tags["drain_reason"] = "Drained as online schema swap seed"
			return nil
		})
	if err != nil {
		// This is not a critical error, we'll just log it.
		log.Errorf("Got error trying to set drain_reason on tablet %v: %v", seedTablet.Alias, err)
	}

	// TODO: Add support for multi-statement schema swaps.
	_, err = shardSwap.parent.tabletClient.ExecuteFetchAsDba(
		shardSwap.parent.ctx,
		seedTablet,
		true, /* usePool */
		[]byte(shardSwap.parent.sql),
		0,    /* maxRows */
		true, /* disableBinlogs */
		true /* reloadSchema */)
	if err != nil {
		if undrainErr := shardSwap.undrainSeedTablet(seedTablet, seedTabletType); undrainErr != nil {
			// We won't return error of undraining because we already have error of SQL execution.
			log.Errorf("Got error undraining seed tablet: %v", undrainErr)
		}
		return err
	}
	updateAppliedSwapQuery := fmt.Sprintf(
		"INSERT INTO _vt.local_metadata (name, value) VALUES ('%s', '%d') ON DUPLICATE KEY UPDATE value = '%d'",
		lastAppliedMetadataName, shardSwap.parent.swapID, shardSwap.parent.swapID)
	_, err = shardSwap.parent.tabletClient.ExecuteFetchAsDba(
		shardSwap.parent.ctx,
		seedTablet,
		true, /* usePool */
		[]byte(updateAppliedSwapQuery),
		0,    /* maxRows */
		true, /* disableBinlogs */
		false /* reloadSchema */)
	if err != nil {
		if undrainErr := shardSwap.undrainSeedTablet(seedTablet, seedTabletType); undrainErr != nil {
			// We won't return error of undraining because we already have error of SQL execution.
			log.Errorf("Got error undraining seed tablet: %v", undrainErr)
		}
		return err
	}

	if err = shardSwap.undrainSeedTablet(seedTablet, seedTabletType); err != nil {
		return err
	}

	shardSwap.addShardLog("Schema applied on the seed tablet")
	return nil
}

// takeSeedBackup takes backup on the seed tablet. The method assumes that the seed tablet is any tablet that
// has info that schema swap was already applied on it. This way the function can be re-used if the seed backup
// is lost somewhere half way through the schema swap process.
func (shardSwap *shardSchemaSwap) takeSeedBackup() (err error) {
	shardSwap.markStepInProgress(shardSwap.backupUINode)
	defer shardSwap.markStepDone(shardSwap.backupUINode, &err)

	tabletList := shardSwap.getTabletList()
	sort.Sort(orderTabletsForSwap(tabletList))
	var seedTablet *topodatapb.Tablet
	for seedTablet == nil {
		for _, tabletStats := range tabletList {
			swapApplied, err := shardSwap.isSwapApplied(tabletStats.Tablet)
			if err != nil {
				return err
			}
			if swapApplied && getTabletTypeFromStats(&tabletStats) != topodatapb.TabletType_MASTER {
				seedTablet = tabletStats.Tablet
				break
			}
		}
		if seedTablet == nil {
			shardSwap.addShardLog("Cannot find the seed tablet to take backup.")
			if err := shardSwap.applySeedSchemaChange(); err != nil {
				return err
			}
		}
	}

	shardSwap.addShardLog(fmt.Sprintf("Taking backup on the seed tablet %v", seedTablet.Alias))
	eventStream, err := shardSwap.parent.tabletClient.Backup(shardSwap.parent.ctx, seedTablet, *backupConcurrency)
	if err != nil {
		return err
	}
waitForBackup:
	for {
		event, err := eventStream.Recv()
		switch err {
		case nil:
			log.Infof("Backup process on tablet %v: %v", seedTablet.Alias, logutil.EventString(event))
		case io.EOF:
			break waitForBackup
		default:
			return err
		}
	}

	shardSwap.addShardLog(fmt.Sprintf("Waiting for the seed tablet %v to become healthy", seedTablet.Alias))
	return shardSwap.waitForTabletToBeHealthy(seedTablet)
}

// swapOnTablet performs the schema swap on the provided tablet and then waits for it
// to become healthy and to catch up with replication.
func (shardSwap *shardSchemaSwap) swapOnTablet(tablet *topodatapb.Tablet) error {
	shardSwap.addPropagationLog(fmt.Sprintf("Restoring tablet %v from backup", tablet.Alias))
	eventStream, err := shardSwap.parent.tabletClient.RestoreFromBackup(shardSwap.parent.ctx, tablet)
	if err != nil {
		return err
	}

waitForRestore:
	for {
		event, err := eventStream.Recv()
		switch err {
		case nil:
			log.Infof("Restore process on tablet %v: %v", tablet.Alias, logutil.EventString(event))
		case io.EOF:
			break waitForRestore
		default:
			return err
		}
	}
	// Check if the tablet has restored from a backup with schema change applied.
	swapApplied, err := shardSwap.isSwapApplied(tablet)
	if err != nil {
		return err
	}
	if !swapApplied {
		shardSwap.addPropagationLog(fmt.Sprintf("Tablet %v is restored but doesn't have schema swap applied", tablet.Alias))
		return errNoBackupWithSwap
	}

	shardSwap.addPropagationLog(fmt.Sprintf("Waiting for tablet %v to become healthy", tablet.Alias))
	return shardSwap.waitForTabletToBeHealthy(tablet)
}

// waitForTabletToBeHealthy waits until the given tablet becomes healthy and caught up with
// replication. If the tablet is already healthy and caught up when this method is called then
// it returns immediately.
func (shardSwap *shardSchemaSwap) waitForTabletToBeHealthy(tablet *topodatapb.Tablet) error {
	waitChannel, err := shardSwap.startWaitingOnUnhealthyTablet(tablet)
	if err != nil {
		return err
	}
	if waitChannel != nil {
		select {
		case <-shardSwap.parent.ctx.Done():
			return shardSwap.parent.ctx.Err()
		case <-*waitChannel:
			// Tablet has caught up, we can return successfully.
		}
	}
	return nil
}

// updatePropagationProgressUI updates the progress bar in the workflow UI to reflect appropriate
// percentage in line with how many tablets have schema swapped already.
func (shardSwap *shardSchemaSwap) updatePropagationProgressUI() {
	shardSwap.propagationUINode.ProgressMessage = fmt.Sprintf("%v/%v", shardSwap.numTabletsSwapped, shardSwap.numTabletsTotal)
	if shardSwap.numTabletsTotal == 0 {
		shardSwap.propagationUINode.Progress = 0
	} else {
		shardSwap.propagationUINode.Progress = shardSwap.numTabletsSwapped * 100 / shardSwap.numTabletsTotal
	}
	shardSwap.propagationUINode.BroadcastChanges(false /* updateChildren */)
}

// markPropagationInProgress marks the propagation step of the shard schema swap workflow as running.
func (shardSwap *shardSchemaSwap) markPropagationInProgress() {
	shardSwap.propagationUINode.Message = ""
	shardSwap.propagationUINode.State = workflowpb.WorkflowState_Running
	shardSwap.propagationUINode.Display = workflow.NodeDisplayDeterminate
	shardSwap.propagationUINode.BroadcastChanges(false /* updateChildren */)
}

// addPropagationLog prints the message to logs, adds it to logs displayed in the UI on the "Propagate to all tablets"
// node and adds the message to the logs displayed on the overall shard node, so that everything happening to this
// shard was visible in one log.
func (shardSwap *shardSchemaSwap) addPropagationLog(message string) {
	shardSwap.propagationUILogger.Infof(message)
	shardSwap.propagationUINode.Log = shardSwap.propagationUILogger.String()
	shardSwap.propagationUINode.Message = message
	shardSwap.propagationUINode.BroadcastChanges(false /* updateChildren */)
	shardSwap.addShardLog(message)
}

// markPropagationDone marks the propagation step of the shard schema swap workflow as finished successfully
// or with an error.
func (shardSwap *shardSchemaSwap) markPropagationDone(err *error) {
	if *err != nil {
		msg := fmt.Sprintf("Error: %v", *err)
		shardSwap.addPropagationLog(msg)
		shardSwap.propagationUINode.Message = msg
		shardSwap.propagationUINode.State = workflowpb.WorkflowState_Done
	} else if shardSwap.numTabletsSwapped == shardSwap.numTabletsTotal {
		shardSwap.propagationUINode.State = workflowpb.WorkflowState_Done
	}
	shardSwap.propagationUINode.BroadcastChanges(false /* updateChildren */)
}

// propagateToNonMasterTablets propagates the schema change to all non-master tablets
// in the shard. When it returns nil it means that only master can remain left with the
// old schema, everything else is verified to have schema swap applied.
func (shardSwap *shardSchemaSwap) propagateToAllTablets(withMasterReparent bool) (err error) {
	shardSwap.markPropagationInProgress()
	defer shardSwap.markPropagationDone(&err)

	for {
		shardSwap.updatePropagationProgressUI()
		tablet, err := shardSwap.findNextTabletToSwap()
		// Update UI immediately after finding next tablet because the total number of tablets or
		// the number of tablets with swapped schema can change in it.
		shardSwap.updatePropagationProgressUI()
		if err == errOnlyMasterLeft {
			if withMasterReparent {
				err = shardSwap.reparentFromMaster(tablet)
				if err != nil {
					return err
				}
			} else {
				return nil
			}
		}
		if err == nil {
			if tablet == nil {
				shardSwap.propagationUINode.Message = ""
				shardSwap.propagationUINode.BroadcastChanges(false /* updateChildren */)
				return nil
			}
			err = shardSwap.swapOnTablet(tablet)
			if err == nil {
				shardSwap.numTabletsSwapped++
				shardSwap.addPropagationLog(fmt.Sprintf("Schema is successfully swapped on tablet %v.", tablet.Alias))
				continue
			}
			if err == errNoBackupWithSwap {
				if err := shardSwap.takeSeedBackup(); err != nil {
					return err
				}
				continue
			}
			shardSwap.addPropagationLog(fmt.Sprintf("Error swapping schema on tablet %v: %v", tablet.Alias, err))
		} else {
			shardSwap.addPropagationLog(fmt.Sprintf("Error searching for next tablet to swap schema on: %v.", err))
		}

		shardSwap.addPropagationLog(fmt.Sprintf("Sleeping for %v.", *delayBetweenErrors))
		select {
		case <-shardSwap.parent.ctx.Done():
			return shardSwap.parent.ctx.Err()
		case <-time.After(*delayBetweenErrors):
			// Waiting is done going to the next loop.
		}
		shardSwap.propagationUINode.Message = ""
		shardSwap.propagationUINode.BroadcastChanges(false /* updateChildren */)
	}
}

// propagateToMaster propagates the schema change to the master. If the master already has
// the schema change applied then the method does nothing.
func (shardSwap *shardSchemaSwap) reparentFromMaster(masterTablet *topodatapb.Tablet) (err error) {
	shardSwap.markStepInProgress(shardSwap.reparentUINode)
	defer shardSwap.markStepDone(shardSwap.reparentUINode, &err)

	shardSwap.addShardLog(fmt.Sprintf("Reparenting away from master %v", masterTablet.Alias))
	if *mysqlctl.DisableActiveReparents {
		hk := &hook.Hook{
			Name: "reparent_away",
		}
		hookResult, err := shardSwap.parent.tabletClient.ExecuteHook(shardSwap.parent.ctx, masterTablet, hk)
		if err != nil {
			return err
		}
		if hookResult.ExitStatus != hook.HOOK_SUCCESS {
			return fmt.Errorf("Error executing 'reparent_away' hook: %v", hookResult.String())
		}
	} else {
		wr := wrangler.New(logutil.NewConsoleLogger(), shardSwap.parent.topoServer, shardSwap.parent.tabletClient)
		err = wr.PlannedReparentShard(
			shardSwap.parent.ctx,
			shardSwap.parent.keyspace,
			shardSwap.shardName,
			nil,                /* masterElectTabletAlias */
			masterTablet.Alias, /* avoidMasterAlias */
			*reparentTimeout)
		if err != nil {
			return err
		}
	}
	return nil
}
