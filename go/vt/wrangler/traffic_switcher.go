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

package wrangler

import (
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/json2"

	"vitess.io/vitess/go/vt/topotools"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/log"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

const (
	frozenStr      = "FROZEN"
	errorNoStreams = "no streams found in keyspace %s for: %s"
	// use pt-osc's naming convention, this format also ensures vstreamer ignores such tables
	renameTableTemplate = "_%.59s_old" // limit table name to 64 characters
)

// TrafficSwitchDirection specifies the switching direction.
type TrafficSwitchDirection int

// The following constants define the switching direction.
const (
	DirectionForward = TrafficSwitchDirection(iota)
	DirectionBackward
)

// TableRemovalType specifies the way the a table will be removed
type TableRemovalType int

// The following consts define if DropSource will drop or rename the table
const (
	DropTable = TableRemovalType(iota)
	RenameTable
)

func (trt TableRemovalType) String() string {
	types := [...]string{
		"DROP TABLE",
		"RENAME TABLE",
	}
	if trt < DropTable || trt > RenameTable {
		return "Unknown"
	}

	return types[trt]
}

// accessType specifies the type of access for a shard (allow/disallow writes).
type accessType int

const (
	allowWrites = accessType(iota)
	disallowWrites
)

// trafficSwitcher contains the metadata for switching read and write traffic
// for vreplication streams.
type trafficSwitcher struct {
	migrationType binlogdatapb.MigrationType
	wr            *Wrangler
	workflow      string

	// if frozen is true, the rest of the fields are not set.
	frozen          bool
	reverseWorkflow string
	id              int64
	sources         map[string]*tsSource
	targets         map[string]*tsTarget
	sourceKeyspace  string
	targetKeyspace  string
	tables          []string
	sourceKSSchema  *vindexes.KeyspaceSchema
	optCells        string //cells option passed to MoveTables/Reshard
	optTabletTypes  string //tabletTypes option passed to MoveTables/Reshard
	externalCluster string
	externalTopo    *topo.Server
}

// tsTarget contains the metadata for each migration target.
type tsTarget struct {
	si       *topo.ShardInfo
	master   *topo.TabletInfo
	sources  map[uint32]*binlogdatapb.BinlogSource
	position string
}

// tsSource contains the metadata for each migration source.
type tsSource struct {
	si        *topo.ShardInfo
	master    *topo.TabletInfo
	position  string
	journaled bool
}

const (
	workflowTypeReshard    = "Reshard"
	workflowTypeMoveTables = "MoveTables"
)

type workflowState struct {
	Workflow       string
	SourceKeyspace string
	TargetKeyspace string
	WorkflowType   string

	ReplicaCellsSwitched    []string
	ReplicaCellsNotSwitched []string

	RdonlyCellsSwitched    []string
	RdonlyCellsNotSwitched []string

	WritesSwitched bool
}

// For a Reshard, to check whether we have switched reads for a tablet type, we check if any one of the source shards has
// the query service disabled in its tablet control record
func (wr *Wrangler) getCellsWithShardReadsSwitched(ctx context.Context, targetKeyspace string, si *topo.ShardInfo, tabletType string) (
	cellsSwitched, cellsNotSwitched []string, err error) {

	cells, err := wr.ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, cell := range cells {
		srvKeyspace, err := wr.ts.GetSrvKeyspace(ctx, cell, targetKeyspace)
		if err != nil {
			return nil, nil, err
		}
		// Checking one shard is enough.
		var shardServedTypes []string
		found := false
		noControls := true
		for _, partition := range srvKeyspace.GetPartitions() {
			if !strings.EqualFold(partition.GetServedType().String(), tabletType) {
				continue
			}

			// If reads and writes are both switched it is possible that the shard is not in the partition table
			for _, shardReference := range partition.GetShardReferences() {
				if key.KeyRangeEqual(shardReference.GetKeyRange(), si.GetKeyRange()) {
					found = true
					break
				}
			}

			// It is possible that there are no tablet controls if the target shards are not yet serving
			// or once reads and writes are both switched,
			if len(partition.GetShardTabletControls()) == 0 {
				noControls = true
				break
			}
			for _, tabletControl := range partition.GetShardTabletControls() {
				if key.KeyRangeEqual(tabletControl.GetKeyRange(), si.GetKeyRange()) {
					if !tabletControl.GetQueryServiceDisabled() {
						shardServedTypes = append(shardServedTypes, si.ShardName())
					}
					break
				}
			}
		}
		if found && (len(shardServedTypes) > 0 || noControls) {
			cellsNotSwitched = append(cellsNotSwitched, cell)
		} else {
			cellsSwitched = append(cellsSwitched, cell)
		}
	}
	return cellsSwitched, cellsNotSwitched, nil
}

// For MoveTables,  to check whether we have switched reads for a tablet type, we check whether the routing rule
// for the tablet_type is pointing to the target keyspace
func (wr *Wrangler) getCellsWithTableReadsSwitched(ctx context.Context, targetKeyspace, table, tabletType string) (
	cellsSwitched, cellsNotSwitched []string, err error) {

	cells, err := wr.ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	getKeyspace := func(ruleTarget string) (string, error) {
		arr := strings.Split(ruleTarget, ".")
		if len(arr) != 2 {
			return "", fmt.Errorf("rule target is not correctly formatted: %s", ruleTarget)
		}
		return arr[0], nil
	}
	for _, cell := range cells {
		srvVSchema, err := wr.ts.GetSrvVSchema(ctx, cell)
		if err != nil {
			return nil, nil, err
		}
		rules := srvVSchema.RoutingRules.Rules
		found := false
		switched := false
		for _, rule := range rules {
			ruleName := fmt.Sprintf("%s.%s@%s", targetKeyspace, table, tabletType)
			if rule.FromTable == ruleName {
				found = true
				for _, to := range rule.ToTables {
					ks, err := getKeyspace(to)
					if err != nil {
						log.Errorf(err.Error())
						return nil, nil, err
					}
					if ks == targetKeyspace {
						switched = true
						break // if one table in workflow is switched we are done
					}
				}
			}
			if found {
				break
			}
		}
		if switched {
			cellsSwitched = append(cellsSwitched, cell)
		} else {
			cellsNotSwitched = append(cellsNotSwitched, cell)
		}
	}
	return cellsSwitched, cellsNotSwitched, nil
}

func (wr *Wrangler) getWorkflowState(ctx context.Context, targetKeyspace, workflow string) (*trafficSwitcher, *workflowState, error) {
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflow)

	if ts == nil || err != nil {
		if err.Error() == fmt.Sprintf(errorNoStreams, targetKeyspace, workflow) {
			return nil, nil, nil
		}
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, nil, err
	}

	ws := &workflowState{Workflow: workflow, TargetKeyspace: targetKeyspace}
	ws.SourceKeyspace = ts.sourceKeyspace
	var cellsSwitched, cellsNotSwitched []string
	var keyspace string
	var reverse bool

	// we reverse writes by using the source_keyspace.workflowname_reverse workflow spec, so we need to use the
	// source of the reverse workflow, which is the target of the workflow initiated by the user for checking routing rules
	// Similarly we use a target shard of the reverse workflow as the original source to check if writes have been switched
	if strings.HasSuffix(workflow, "_reverse") {
		reverse = true
		keyspace = ws.SourceKeyspace
		workflow = reverseName(workflow)
	} else {
		keyspace = targetKeyspace
	}
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		ws.WorkflowType = workflowTypeMoveTables

		// we assume a consistent state, so only choose routing rule for one table for replica/rdonly
		if len(ts.tables) == 0 {
			return nil, nil, fmt.Errorf("no tables in workflow %s.%s", keyspace, workflow)

		}
		table := ts.tables[0]

		cellsSwitched, cellsNotSwitched, err = wr.getCellsWithTableReadsSwitched(ctx, keyspace, table, "rdonly")
		if err != nil {
			return nil, nil, err
		}
		ws.RdonlyCellsNotSwitched, ws.RdonlyCellsSwitched = cellsNotSwitched, cellsSwitched
		cellsSwitched, cellsNotSwitched, err = wr.getCellsWithTableReadsSwitched(ctx, keyspace, table, "replica")
		if err != nil {
			return nil, nil, err
		}
		ws.ReplicaCellsNotSwitched, ws.ReplicaCellsSwitched = cellsNotSwitched, cellsSwitched
		rules, err := ts.wr.getRoutingRules(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, table := range ts.tables {
			rr := rules[table]
			// if a rule exists for the table and points to the target keyspace, writes have been switched
			if len(rr) > 0 && rr[0] == fmt.Sprintf("%s.%s", keyspace, table) {
				ws.WritesSwitched = true
			}
		}
	} else {
		ws.WorkflowType = workflowTypeReshard

		// we assume a consistent state, so only choose one shard
		var shard *topo.ShardInfo
		if reverse {
			shard = ts.targetShards()[0]
		} else {
			shard = ts.sourceShards()[0]
		}

		cellsSwitched, cellsNotSwitched, err = wr.getCellsWithShardReadsSwitched(ctx, keyspace, shard, "rdonly")
		if err != nil {
			return nil, nil, err
		}
		ws.RdonlyCellsNotSwitched, ws.RdonlyCellsSwitched = cellsNotSwitched, cellsSwitched
		cellsSwitched, cellsNotSwitched, err = wr.getCellsWithShardReadsSwitched(ctx, keyspace, shard, "replica")
		if err != nil {
			return nil, nil, err
		}
		ws.ReplicaCellsNotSwitched, ws.ReplicaCellsSwitched = cellsNotSwitched, cellsSwitched
		if !shard.IsMasterServing {
			ws.WritesSwitched = true
		}
	}

	return ts, ws, nil
}

func (wr *Wrangler) doCellsHaveRdonlyTablets(ctx context.Context, cells []string) (bool, error) {
	areAnyRdonly := func(tablets []*topo.TabletInfo) bool {
		for _, tablet := range tablets {
			if tablet.Type == topodatapb.TabletType_RDONLY {
				return true
			}
		}
		return false
	}

	if len(cells) == 0 {
		tablets, err := topotools.GetAllTabletsAcrossCells(ctx, wr.ts)
		if err != nil {
			return false, err
		}
		if areAnyRdonly(tablets) {
			return true, nil
		}

	} else {
		for _, cell := range cells {
			tablets, err := topotools.GetAllTablets(ctx, wr.ts, cell)
			if err != nil {
				return false, err
			}
			if areAnyRdonly(tablets) {
				return true, nil
			}
		}
	}
	return false, nil
}

// SwitchReads is a generic way of switching read traffic for a resharding workflow.
func (wr *Wrangler) SwitchReads(ctx context.Context, targetKeyspace, workflow string, servedTypes []topodatapb.TabletType,
	cells []string, direction TrafficSwitchDirection, dryRun bool) (*[]string, error) {

	ts, ws, err := wr.getWorkflowState(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("getWorkflowState failed: %v", err)
		return nil, err
	}
	if ts == nil {
		errorMsg := fmt.Sprintf("workflow %s not found in keyspace %s", workflow, targetKeyspace)
		wr.Logger().Errorf(errorMsg)
		return nil, fmt.Errorf(errorMsg)
	}
	log.Infof("SwitchReads: %s.%s tt %+v, cells %+v, workflow state: %+v", targetKeyspace, workflow, servedTypes, cells, ws)
	var switchReplicas, switchRdonly bool
	for _, servedType := range servedTypes {
		if servedType != topodatapb.TabletType_REPLICA && servedType != topodatapb.TabletType_RDONLY {
			return nil, fmt.Errorf("tablet type must be REPLICA or RDONLY: %v", servedType)
		}
		if direction == DirectionBackward && servedType == topodatapb.TabletType_REPLICA && len(ws.ReplicaCellsSwitched) == 0 {
			return nil, fmt.Errorf("requesting reversal of SwitchReads for REPLICAs but REPLICA reads have not been switched")
		}
		if direction == DirectionBackward && servedType == topodatapb.TabletType_RDONLY && len(ws.RdonlyCellsSwitched) == 0 {
			return nil, fmt.Errorf("requesting reversal of SwitchReads for RDONLYs but RDONLY reads have not been switched")
		}
		switch servedType {
		case topodatapb.TabletType_REPLICA:
			switchReplicas = true
		case topodatapb.TabletType_RDONLY:
			switchRdonly = true
		}
	}

	// if there are no rdonly tablets in the cells ask to switch rdonly tablets as well so that routing rules
	// are updated for rdonly as well. Otherwise vitess will not know that the workflow has completed and will
	// incorrectly report that not all reads have been switched. User currently is forced to switch non-existent rdonly tablets
	if switchReplicas && !switchRdonly {
		var err error
		rdonlyTabletsExist, err := wr.doCellsHaveRdonlyTablets(ctx, cells)
		if err != nil {
			return nil, err
		}
		if !rdonlyTabletsExist {
			servedTypes = append(servedTypes, topodatapb.TabletType_RDONLY)
		}
	}

	// If journals exist notify user and fail
	journalsExist, _, err := ts.checkJournals(ctx)
	if err != nil {
		wr.Logger().Errorf("checkJournals failed: %v", err)
		return nil, err
	}
	if journalsExist {
		log.Infof("Found a previous journal entry for %d", ts.id)
	}
	var sw iswitcher
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, wr: wr}
	}

	if err := ts.validate(ctx); err != nil {
		ts.wr.Logger().Errorf("validate failed: %v", err)
		return nil, err
	}

	// For reads, locking the source keyspace is sufficient.
	ctx, unlock, lockErr := sw.lockKeyspace(ctx, ts.sourceKeyspace, "SwitchReads")
	if lockErr != nil {
		ts.wr.Logger().Errorf("LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer unlock(&err)

	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		if err := sw.switchTableReads(ctx, cells, servedTypes, direction); err != nil {
			ts.wr.Logger().Errorf("switchTableReads failed: %v", err)
			return nil, err
		}
		return sw.logs(), nil
	}
	wr.Logger().Infof("About to switchShardReads: %+v, %+v, %+v", cells, servedTypes, direction)
	if err := ts.switchShardReads(ctx, cells, servedTypes, direction); err != nil {
		ts.wr.Logger().Errorf("switchShardReads failed: %v", err)
		return nil, err
	}

	wr.Logger().Infof("switchShardReads Completed: %+v, %+v, %+v", cells, servedTypes, direction)
	if err := wr.ts.ValidateSrvKeyspace(ctx, targetKeyspace, strings.Join(cells, ",")); err != nil {
		err2 := vterrors.Wrapf(err, "After switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			targetKeyspace, strings.Join(cells, ","))
		log.Errorf("%w", err2)
		return nil, err2
	}
	return sw.logs(), nil
}

// SwitchWrites is a generic way of migrating write traffic for a resharding workflow.
func (wr *Wrangler) SwitchWrites(ctx context.Context, targetKeyspace, workflow string, timeout time.Duration, cancel, reverse, reverseReplication bool, dryRun bool) (journalID int64, dryRunResults *[]string, err error) {
	ts, ws, err := wr.getWorkflowState(ctx, targetKeyspace, workflow)
	_ = ws
	if err != nil {
		wr.Logger().Errorf("getWorkflowState failed: %v", err)
		return 0, nil, err
	}
	if ts == nil {
		errorMsg := fmt.Sprintf("workflow %s not found in keyspace %s", workflow, targetKeyspace)
		wr.Logger().Errorf(errorMsg)
		return 0, nil, fmt.Errorf(errorMsg)
	}

	var sw iswitcher
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, wr: wr}
	}

	if ts.frozen {
		ts.wr.Logger().Warningf("Writes have already been switched for workflow %s, nothing to do here", ts.workflow)
		return 0, sw.logs(), nil
	}

	ts.wr.Logger().Infof("Built switching metadata: %+v", ts)
	if err := ts.validate(ctx); err != nil {
		ts.wr.Logger().Errorf("validate failed: %v", err)
		return 0, nil, err
	}

	// Need to lock both source and target keyspaces.
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.sourceKeyspace, "SwitchWrites")
	if lockErr != nil {
		ts.wr.Logger().Errorf("LockKeyspace failed: %v", lockErr)
		return 0, nil, lockErr
	}
	ctx = tctx
	defer sourceUnlock(&err)
	if ts.targetKeyspace != ts.sourceKeyspace {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.targetKeyspace, "SwitchWrites")
		if lockErr != nil {
			ts.wr.Logger().Errorf("LockKeyspace failed: %v", lockErr)
			return 0, nil, lockErr
		}
		ctx = tctx
		defer targetUnlock(&err)
	}

	// If no journals exist, sourceWorkflows will be initialized by sm.MigrateStreams.
	journalsExist, sourceWorkflows, err := ts.checkJournals(ctx)
	if err != nil {
		ts.wr.Logger().Errorf("checkJournals failed: %v", err)
		return 0, nil, err
	}
	if !journalsExist {
		ts.wr.Logger().Infof("No previous journals were found. Proceeding normally.")
		sm, err := buildStreamMigrater(ctx, ts, cancel)
		if err != nil {
			ts.wr.Logger().Errorf("buildStreamMigrater failed: %v", err)
			return 0, nil, err
		}
		if cancel {
			sw.cancelMigration(ctx, sm)
			return 0, sw.logs(), nil
		}
		ts.wr.Logger().Infof("Stopping streams")
		sourceWorkflows, err = sw.stopStreams(ctx, sm)
		if err != nil {
			ts.wr.Logger().Errorf("stopStreams failed: %v", err)
			for key, streams := range sm.streams {
				for _, stream := range streams {
					ts.wr.Logger().Errorf("stream in stopStreams: key %s shard %s stream %+v", key, stream.bls.Shard, stream.bls)
				}
			}
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}
		ts.wr.Logger().Infof("Stopping source writes")
		if err := sw.stopSourceWrites(ctx); err != nil {
			ts.wr.Logger().Errorf("stopSourceWrites failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		ts.wr.Logger().Infof("Waiting for streams to catchup")
		if err := sw.waitForCatchup(ctx, timeout); err != nil {
			ts.wr.Logger().Errorf("waitForCatchup failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		ts.wr.Logger().Infof("Migrating streams")
		if err := sw.migrateStreams(ctx, sm); err != nil {
			ts.wr.Logger().Errorf("migrateStreams failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		ts.wr.Logger().Infof("Creating reverse streams")
		if err := sw.createReverseVReplication(ctx); err != nil {
			ts.wr.Logger().Errorf("createReverseVReplication failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}
	} else {
		if cancel {
			err := fmt.Errorf("traffic switching has reached the point of no return, cannot cancel")
			ts.wr.Logger().Errorf("%v", err)
			return 0, nil, err
		}
		ts.wr.Logger().Infof("Journals were found. Completing the left over steps.")
		// Need to gather positions in case all journals were not created.
		if err := ts.gatherPositions(ctx); err != nil {
			ts.wr.Logger().Errorf("gatherPositions failed: %v", err)
			return 0, nil, err
		}
	}

	// This is the point of no return. Once a journal is created,
	// traffic can be redirected to target shards.
	if err := sw.createJournals(ctx, sourceWorkflows); err != nil {
		ts.wr.Logger().Errorf("createJournals failed: %v", err)
		return 0, nil, err
	}
	if err := sw.allowTargetWrites(ctx); err != nil {
		ts.wr.Logger().Errorf("allowTargetWrites failed: %v", err)
		return 0, nil, err
	}
	if err := sw.changeRouting(ctx); err != nil {
		ts.wr.Logger().Errorf("changeRouting failed: %v", err)
		return 0, nil, err
	}
	if err := sw.streamMigraterfinalize(ctx, ts, sourceWorkflows); err != nil {
		ts.wr.Logger().Errorf("finalize failed: %v", err)
		return 0, nil, err
	}
	if reverseReplication {
		if err := sw.startReverseVReplication(ctx); err != nil {
			ts.wr.Logger().Errorf("startReverseVReplication failed: %v", err)
			return 0, nil, err
		}
	}

	if err := sw.freezeTargetVReplication(ctx); err != nil {
		ts.wr.Logger().Errorf("deleteTargetVReplication failed: %v", err)
		return 0, nil, err
	}

	return ts.id, sw.logs(), nil
}

// DropTargets cleans up target tables, shards and blacklisted tables if a MoveTables/Reshard is cancelled
func (wr *Wrangler) DropTargets(ctx context.Context, targetKeyspace, workflow string, keepData, dryRun bool) (*[]string, error) {
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, err
	}
	var sw iswitcher
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, wr: wr}
	}
	var tctx context.Context
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.sourceKeyspace, "DropTargets")
	if lockErr != nil {
		ts.wr.Logger().Errorf("Source LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer sourceUnlock(&err)
	ctx = tctx
	if ts.targetKeyspace != ts.sourceKeyspace {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.targetKeyspace, "DropTargets")
		if lockErr != nil {
			ts.wr.Logger().Errorf("Target LockKeyspace failed: %v", lockErr)
			return nil, lockErr
		}
		defer targetUnlock(&err)
		ctx = tctx
	}
	if !keepData {
		switch ts.migrationType {
		case binlogdatapb.MigrationType_TABLES:
			log.Infof("Deleting target tables")
			if err := sw.removeTargetTables(ctx); err != nil {
				return nil, err
			}
			if err := sw.dropSourceBlacklistedTables(ctx); err != nil {
				return nil, err
			}
		case binlogdatapb.MigrationType_SHARDS:
			log.Infof("Removing target shards")
			if err := sw.dropTargetShards(ctx); err != nil {
				return nil, err
			}
		}
	}
	if err := wr.dropArtifacts(ctx, sw); err != nil {
		return nil, err
	}
	if err := ts.wr.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		return nil, err
	}
	return sw.logs(), nil
}

func (wr *Wrangler) dropArtifacts(ctx context.Context, sw iswitcher) error {
	if err := sw.dropSourceReverseVReplicationStreams(ctx); err != nil {
		return err
	}
	if err := sw.dropTargetVReplicationStreams(ctx); err != nil {
		return err
	}
	if err := sw.deleteRoutingRules(ctx); err != nil {
		return err
	}

	return nil
}

// finalizeMigrateWorkflow deletes the streams for the Migrate workflow.
// We only cleanup the target for external sources
func (wr *Wrangler) finalizeMigrateWorkflow(ctx context.Context, targetKeyspace, workflow, tableSpecs string,
	cancel, keepData, dryRun bool) (*[]string, error) {
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, err
	}
	var sw iswitcher
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, wr: wr}
	}
	var tctx context.Context
	tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.targetKeyspace, "completeMigrateWorkflow")
	if lockErr != nil {
		ts.wr.Logger().Errorf("Target LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer targetUnlock(&err)
	ctx = tctx
	if err := sw.dropTargetVReplicationStreams(ctx); err != nil {
		return nil, err
	}
	if !cancel {
		sw.addParticipatingTablesToKeyspace(ctx, targetKeyspace, tableSpecs)
		if err := ts.wr.ts.RebuildSrvVSchema(ctx, nil); err != nil {
			return nil, err
		}
	}
	log.Infof("cancel is %t, keepData %t", cancel, keepData)
	if cancel && !keepData {
		if err := sw.removeTargetTables(ctx); err != nil {
			return nil, err
		}
	}
	return sw.logs(), nil
}

// DropSources cleans up source tables, shards and blacklisted tables after a MoveTables/Reshard is completed
func (wr *Wrangler) DropSources(ctx context.Context, targetKeyspace, workflow string, removalType TableRemovalType, keepData, force, dryRun bool) (*[]string, error) {
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, err
	}
	var sw iswitcher
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, wr: wr}
	}
	var tctx context.Context
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.sourceKeyspace, "DropSources")
	if lockErr != nil {
		ts.wr.Logger().Errorf("Source LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer sourceUnlock(&err)
	ctx = tctx
	if ts.targetKeyspace != ts.sourceKeyspace {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.targetKeyspace, "DropSources")
		if lockErr != nil {
			ts.wr.Logger().Errorf("Target LockKeyspace failed: %v", lockErr)
			return nil, lockErr
		}
		defer targetUnlock(&err)
		ctx = tctx
	}
	if !force {
		if err := sw.validateWorkflowHasCompleted(ctx); err != nil {
			wr.Logger().Errorf("Workflow has not completed, cannot DropSources: %v", err)
			return nil, err
		}
	}
	if !keepData {
		switch ts.migrationType {
		case binlogdatapb.MigrationType_TABLES:
			log.Infof("Deleting tables")
			if err := sw.removeSourceTables(ctx, removalType); err != nil {
				return nil, err
			}
			if err := sw.dropSourceBlacklistedTables(ctx); err != nil {
				return nil, err
			}

		case binlogdatapb.MigrationType_SHARDS:
			log.Infof("Removing shards")
			if err := sw.dropSourceShards(ctx); err != nil {
				return nil, err
			}
		}
	}
	if err := wr.dropArtifacts(ctx, sw); err != nil {
		return nil, err
	}
	if err := ts.wr.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		return nil, err
	}

	return sw.logs(), nil
}

func (wr *Wrangler) buildTrafficSwitcher(ctx context.Context, targetKeyspace, workflow string) (*trafficSwitcher, error) {
	tgtInfo, err := wr.buildTargets(ctx, targetKeyspace, workflow)
	if err != nil {
		log.Infof("Error building targets: %s", err)
		return nil, err
	}
	targets, frozen, optCells, optTabletTypes := tgtInfo.targets, tgtInfo.frozen, tgtInfo.optCells, tgtInfo.optTabletTypes

	ts := &trafficSwitcher{
		wr:              wr,
		workflow:        workflow,
		reverseWorkflow: reverseName(workflow),
		id:              hashStreams(targetKeyspace, targets),
		targets:         targets,
		sources:         make(map[string]*tsSource),
		targetKeyspace:  targetKeyspace,
		frozen:          frozen,
		optCells:        optCells,
		optTabletTypes:  optTabletTypes,
	}
	log.Infof("Migration ID for workflow %s: %d", workflow, ts.id)
	sourceTopo := wr.ts

	// Build the sources
	for _, target := range targets {
		for _, bls := range target.sources {
			if ts.sourceKeyspace == "" {
				ts.sourceKeyspace = bls.Keyspace
				ts.externalCluster = bls.ExternalCluster
				if ts.externalCluster != "" {
					externalTopo, err := wr.ts.OpenExternalVitessClusterServer(ctx, ts.externalCluster)
					if err != nil {
						return nil, err
					}
					sourceTopo = externalTopo
					ts.externalTopo = externalTopo
				}
			} else if ts.sourceKeyspace != bls.Keyspace {
				return nil, fmt.Errorf("source keyspaces are mismatched across streams: %v vs %v", ts.sourceKeyspace, bls.Keyspace)
			}

			if ts.tables == nil {
				for _, rule := range bls.Filter.Rules {
					ts.tables = append(ts.tables, rule.Match)
				}
				sort.Strings(ts.tables)
			} else {
				var tables []string
				for _, rule := range bls.Filter.Rules {
					tables = append(tables, rule.Match)
				}
				sort.Strings(tables)
				if !reflect.DeepEqual(ts.tables, tables) {
					return nil, fmt.Errorf("table lists are mismatched across streams: %v vs %v", ts.tables, tables)
				}
			}

			if _, ok := ts.sources[bls.Shard]; ok {
				continue
			}
			sourcesi, err := sourceTopo.GetShard(ctx, bls.Keyspace, bls.Shard)
			if err != nil {
				return nil, err
			}
			sourceMaster, err := sourceTopo.GetTablet(ctx, sourcesi.MasterAlias)
			if err != nil {
				return nil, err
			}
			ts.sources[bls.Shard] = &tsSource{
				si:     sourcesi,
				master: sourceMaster,
			}
		}
	}
	if ts.sourceKeyspace != ts.targetKeyspace || ts.externalCluster != "" {
		ts.migrationType = binlogdatapb.MigrationType_TABLES
	} else {
		// TODO(sougou): for shard migration, validate that source and target combined
		// keyranges match.
		ts.migrationType = binlogdatapb.MigrationType_SHARDS
		for sourceShard := range ts.sources {
			if _, ok := ts.targets[sourceShard]; ok {
				// If shards are overlapping, then this is a table migration.
				ts.migrationType = binlogdatapb.MigrationType_TABLES
				break
			}
		}
	}
	vs, err := sourceTopo.GetVSchema(ctx, ts.sourceKeyspace)
	if err != nil {
		return nil, err
	}
	ts.sourceKSSchema, err = vindexes.BuildKeyspaceSchema(vs, ts.sourceKeyspace)
	if err != nil {
		return nil, err
	}
	return ts, nil
}

type targetInfo struct {
	targets        map[string]*tsTarget
	frozen         bool
	optCells       string
	optTabletTypes string
}

func (wr *Wrangler) buildTargets(ctx context.Context, targetKeyspace, workflow string) (*targetInfo, error) {
	var err error
	var frozen bool
	var optCells, optTabletTypes string
	targets := make(map[string]*tsTarget)
	targetShards, err := wr.ts.GetShardNames(ctx, targetKeyspace)
	if err != nil {
		return nil, err
	}
	// We check all target shards. All of them may not have a stream.
	// For example, if we're splitting -80 to -40,40-80, only those
	// two target shards will have vreplication streams.
	for _, targetShard := range targetShards {
		targetsi, err := wr.ts.GetShard(ctx, targetKeyspace, targetShard)
		if err != nil {
			return nil, err
		}
		if targetsi.MasterAlias == nil {
			// This can happen if bad inputs are given.
			return nil, fmt.Errorf("shard %v:%v doesn't have a master set", targetKeyspace, targetShard)
		}
		targetMaster, err := wr.ts.GetTablet(ctx, targetsi.MasterAlias)
		if err != nil {
			return nil, err
		}
		p3qr, err := wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, fmt.Sprintf("select id, source, message, cell, tablet_types from _vt.vreplication where workflow=%s and db_name=%s", encodeString(workflow), encodeString(targetMaster.DbName())))
		if err != nil {
			return nil, err
		}
		// If there's no vreplication stream, check the next target.
		if len(p3qr.Rows) < 1 {
			continue
		}

		targets[targetShard] = &tsTarget{
			si:      targetsi,
			master:  targetMaster,
			sources: make(map[uint32]*binlogdatapb.BinlogSource),
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		for _, row := range qr.Rows {
			id, err := evalengine.ToInt64(row[0])
			if err != nil {
				return nil, err
			}

			var bls binlogdatapb.BinlogSource
			if err := proto.UnmarshalText(row[1].ToString(), &bls); err != nil {
				return nil, err
			}
			targets[targetShard].sources[uint32(id)] = &bls

			if row[2].ToString() == frozenStr {
				frozen = true
			}
			optCells = row[3].ToString()
			optTabletTypes = row[4].ToString()
		}
	}
	if len(targets) == 0 {
		err2 := fmt.Errorf(errorNoStreams, targetKeyspace, workflow)
		return nil, err2
	}
	tinfo := &targetInfo{targets: targets, frozen: frozen, optCells: optCells, optTabletTypes: optTabletTypes}
	return tinfo, nil
}

// hashStreams produces a reproducible hash based on the input parameters.
func hashStreams(targetKeyspace string, targets map[string]*tsTarget) int64 {
	var expanded []string
	for shard, target := range targets {
		for uid := range target.sources {
			expanded = append(expanded, fmt.Sprintf("%s:%d", shard, uid))
		}
	}

	sort.Strings(expanded)
	hasher := fnv.New64()
	hasher.Write([]byte(targetKeyspace))
	for _, str := range expanded {
		hasher.Write([]byte(str))
	}
	// Convert to int64 after dropping the highest bit.
	return int64(hasher.Sum64() & math.MaxInt64)
}

func (ts *trafficSwitcher) validate(ctx context.Context) error {
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		sourceTopo := ts.wr.ts
		if ts.externalTopo != nil {
			sourceTopo = ts.externalTopo
		}
		// All shards must be present.
		if err := ts.compareShards(ctx, ts.sourceKeyspace, ts.sourceShards(), sourceTopo); err != nil {
			return err
		}
		if err := ts.compareShards(ctx, ts.targetKeyspace, ts.targetShards(), ts.wr.ts); err != nil {
			return err
		}
		// Wildcard table names not allowed.
		for _, table := range ts.tables {
			if strings.HasPrefix(table, "/") {
				return fmt.Errorf("cannot migrate streams with wild card table names: %v", table)
			}
		}
	}
	return nil
}

func (ts *trafficSwitcher) compareShards(ctx context.Context, keyspace string, sis []*topo.ShardInfo, topo *topo.Server) error {
	var shards []string
	for _, si := range sis {
		shards = append(shards, si.ShardName())
	}
	topoShards, err := topo.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}
	sort.Strings(topoShards)
	sort.Strings(shards)
	if !reflect.DeepEqual(topoShards, shards) {
		return fmt.Errorf("mismatched shards for keyspace %s: topo: %v vs switch command: %v", keyspace, topoShards, shards)
	}
	return nil
}

func (ts *trafficSwitcher) switchTableReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction TrafficSwitchDirection) error {
	log.Infof("switchTableReads: servedTypes: %+v, direction %t", servedTypes, direction)
	rules, err := ts.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	// We assume that the following rules were setup when the targets were created:
	// table -> sourceKeyspace.table
	// targetKeyspace.table -> sourceKeyspace.table
	// For forward migration, we add tablet type specific rules to redirect traffic to the target.
	// For backward, we redirect to source
	for _, servedType := range servedTypes {
		tt := strings.ToLower(servedType.String())
		for _, table := range ts.tables {
			if direction == DirectionForward {
				log.Infof("Route direction forward")
				toTarget := []string{ts.targetKeyspace + "." + table}
				rules[table+"@"+tt] = toTarget
				rules[ts.targetKeyspace+"."+table+"@"+tt] = toTarget
				rules[ts.sourceKeyspace+"."+table+"@"+tt] = toTarget
			} else {
				log.Infof("Route direction backwards")
				toSource := []string{ts.sourceKeyspace + "." + table}
				rules[table+"@"+tt] = toSource
				rules[ts.targetKeyspace+"."+table+"@"+tt] = toSource
				rules[ts.sourceKeyspace+"."+table+"@"+tt] = toSource
			}
		}
	}
	if err := ts.wr.saveRoutingRules(ctx, rules); err != nil {
		return err
	}
	return ts.wr.ts.RebuildSrvVSchema(ctx, cells)
}

func (ts *trafficSwitcher) switchShardReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction TrafficSwitchDirection) error {
	var fromShards, toShards []*topo.ShardInfo
	if direction == DirectionForward {
		fromShards, toShards = ts.sourceShards(), ts.targetShards()
	} else {
		fromShards, toShards = ts.targetShards(), ts.sourceShards()
	}
	if err := ts.wr.ts.ValidateSrvKeyspace(ctx, ts.targetKeyspace, strings.Join(cells, ",")); err != nil {
		err2 := vterrors.Wrapf(err, "Before switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			ts.targetKeyspace, strings.Join(cells, ","))
		log.Errorf("%w", err2)
		return err2
	}
	for _, servedType := range servedTypes {
		if err := ts.wr.updateShardRecords(ctx, ts.sourceKeyspace, fromShards, cells, servedType, true /* isFrom */, false /* clearSourceShards */); err != nil {
			return err
		}
		if err := ts.wr.updateShardRecords(ctx, ts.sourceKeyspace, toShards, cells, servedType, false, false); err != nil {
			return err
		}
		err := ts.wr.ts.MigrateServedType(ctx, ts.sourceKeyspace, toShards, fromShards, servedType, cells)
		if err != nil {
			return err
		}
	}
	if err := ts.wr.ts.ValidateSrvKeyspace(ctx, ts.targetKeyspace, strings.Join(cells, ",")); err != nil {
		err2 := vterrors.Wrapf(err, "After switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			ts.targetKeyspace, strings.Join(cells, ","))
		log.Errorf("%w", err2)
		return err2
	}
	return nil
}

func (wr *Wrangler) checkIfJournalExistsOnTablet(ctx context.Context, tablet *topodatapb.Tablet, migrationID int64) (*binlogdatapb.Journal, bool, error) {
	var exists bool
	journal := &binlogdatapb.Journal{}
	query := fmt.Sprintf("select val from _vt.resharding_journal where id=%v", migrationID)
	p3qr, err := wr.tmc.VReplicationExec(ctx, tablet, query)
	if err != nil {
		return nil, false, err
	}
	if len(p3qr.Rows) != 0 {
		qr := sqltypes.Proto3ToResult(p3qr)
		if !exists {
			if err := proto.UnmarshalText(qr.Rows[0][0].ToString(), journal); err != nil {
				return nil, false, err
			}
			exists = true
		}
	}
	return journal, exists, nil

}

// checkJournals returns true if at least one journal has been created.
// If so, it also returns the list of sourceWorkflows that need to be switched.
func (ts *trafficSwitcher) checkJournals(ctx context.Context) (journalsExist bool, sourceWorkflows []string, err error) {
	var mu sync.Mutex
	err = ts.forAllSources(func(source *tsSource) error {
		mu.Lock()
		defer mu.Unlock()
		journal, exists, err := ts.wr.checkIfJournalExistsOnTablet(ctx, source.master.Tablet, ts.id)
		if err != nil {
			return err
		}
		if exists {
			if journal.Id != 0 {
				sourceWorkflows = journal.SourceWorkflows
			}
			source.journaled = true
			journalsExist = true
		}
		return nil
	})
	return journalsExist, sourceWorkflows, err
}

func (ts *trafficSwitcher) stopSourceWrites(ctx context.Context) error {
	var err error
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		err = ts.changeTableSourceWrites(ctx, disallowWrites)
	} else {
		err = ts.changeShardsAccess(ctx, ts.sourceKeyspace, ts.sourceShards(), disallowWrites)
	}
	if err != nil {
		log.Warningf("Error: %s", err)
		return err
	}
	return ts.forAllSources(func(source *tsSource) error {
		var err error
		source.position, err = ts.wr.tmc.MasterPosition(ctx, source.master.Tablet)
		ts.wr.Logger().Infof("Stopped Source Writes. Position for source %v:%v: %v",
			ts.sourceKeyspace, source.si.ShardName(), source.position)
		if err != nil {
			log.Warningf("Error: %s", err)
		}
		return err
	})
}

func (ts *trafficSwitcher) changeTableSourceWrites(ctx context.Context, access accessType) error {
	return ts.forAllSources(func(source *tsSource) error {
		if _, err := ts.wr.ts.UpdateShardFields(ctx, ts.sourceKeyspace, source.si.ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, access == allowWrites /* remove */, ts.tables)
		}); err != nil {
			return err
		}
		return ts.wr.RefreshTabletsByShard(ctx, source.si, nil)
	})
}

func (ts *trafficSwitcher) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()
	// source writes have been stopped, wait for all streams on targets to catch up
	if err := ts.forAllUids(func(target *tsTarget, uid uint32) error {
		ts.wr.Logger().Infof("Before Catchup: uid: %d, target master %s, target position %s, shard %s", uid,
			target.master.AliasString(), target.position, target.si.String())
		bls := target.sources[uid]
		source := ts.sources[bls.Shard]
		ts.wr.Logger().Infof("Before Catchup: waiting for keyspace:shard: %v:%v to reach source position %v, uid %d",
			ts.targetKeyspace, target.si.ShardName(), source.position, uid)
		if err := ts.wr.tmc.VReplicationWaitForPos(ctx, target.master.Tablet, int(uid), source.position); err != nil {
			return err
		}
		log.Infof("After catchup: target keyspace:shard: %v:%v, source position %v, uid %d",
			ts.targetKeyspace, target.si.ShardName(), source.position, uid)
		ts.wr.Logger().Infof("After catchup: position for keyspace:shard: %v:%v reached, uid %d",
			ts.targetKeyspace, target.si.ShardName(), uid)
		if _, err := ts.wr.tmc.VReplicationExec(ctx, target.master.Tablet, binlogplayer.StopVReplication(uid, "stopped for cutover")); err != nil {
			log.Infof("error marking stopped for cutover on %s, uid %d", target.master.AliasString(), uid)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	// all targets have caught up, record their positions for setting up reverse workflows
	return ts.forAllTargets(func(target *tsTarget) error {
		var err error
		target.position, err = ts.wr.tmc.MasterPosition(ctx, target.master.Tablet)
		ts.wr.Logger().Infof("After catchup, position for target master %s, %v", target.master.AliasString(), target.position)
		return err
	})
}

func (ts *trafficSwitcher) cancelMigration(ctx context.Context, sm *streamMigrater) {
	var err error
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		err = ts.changeTableSourceWrites(ctx, allowWrites)
	} else {
		err = ts.changeShardsAccess(ctx, ts.sourceKeyspace, ts.sourceShards(), allowWrites)
	}
	if err != nil {
		ts.wr.Logger().Errorf("Cancel migration failed:", err)
	}

	sm.cancelMigration(ctx)

	err = ts.forAllTargets(func(target *tsTarget) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s", encodeString(target.master.DbName()), encodeString(ts.workflow))
		_, err := ts.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query)
		return err
	})
	if err != nil {
		ts.wr.Logger().Errorf("Cancel migration failed: could not restart vreplication: %v", err)
	}

	err = ts.deleteReverseVReplication(ctx)
	if err != nil {
		ts.wr.Logger().Errorf("Cancel migration failed: could not delete revers vreplication entries: %v", err)
	}
}

func (ts *trafficSwitcher) gatherPositions(ctx context.Context) error {
	err := ts.forAllSources(func(source *tsSource) error {
		var err error
		source.position, err = ts.wr.tmc.MasterPosition(ctx, source.master.Tablet)
		ts.wr.Logger().Infof("Position for source %v:%v: %v", ts.sourceKeyspace, source.si.ShardName(), source.position)
		return err
	})
	if err != nil {
		return err
	}
	return ts.forAllTargets(func(target *tsTarget) error {
		var err error
		target.position, err = ts.wr.tmc.MasterPosition(ctx, target.master.Tablet)
		ts.wr.Logger().Infof("Position for target %v:%v: %v", ts.targetKeyspace, target.si.ShardName(), target.position)
		return err
	})
}

func (ts *trafficSwitcher) createReverseVReplication(ctx context.Context) error {
	if err := ts.deleteReverseVReplication(ctx); err != nil {
		return err
	}
	err := ts.forAllUids(func(target *tsTarget, uid uint32) error {
		bls := target.sources[uid]
		source := ts.sources[bls.Shard]
		reverseBls := &binlogdatapb.BinlogSource{
			Keyspace:   ts.targetKeyspace,
			Shard:      target.si.ShardName(),
			TabletType: bls.TabletType,
			Filter:     &binlogdatapb.Filter{},
			OnDdl:      bls.OnDdl,
		}
		for _, rule := range bls.Filter.Rules {
			if rule.Filter == "exclude" {
				reverseBls.Filter.Rules = append(reverseBls.Filter.Rules, rule)
				continue
			}
			var filter string
			if strings.HasPrefix(rule.Match, "/") {
				if ts.sourceKSSchema.Keyspace.Sharded {
					filter = key.KeyRangeString(source.si.KeyRange)
				}
			} else {
				var inKeyrange string
				if ts.sourceKSSchema.Keyspace.Sharded {
					vtable, ok := ts.sourceKSSchema.Tables[rule.Match]
					if !ok {
						return fmt.Errorf("table %s not found in vschema1", rule.Match)
					}
					// TODO(sougou): handle degenerate cases like sequence, etc.
					// We currently assume the primary vindex is the best way to filter, which may not be true.
					inKeyrange = fmt.Sprintf(" where in_keyrange(%s, '%s', '%s')", sqlparser.String(vtable.ColumnVindexes[0].Columns[0]), vtable.ColumnVindexes[0].Type, key.KeyRangeString(source.si.KeyRange))
				}
				filter = fmt.Sprintf("select * from %s%s", rule.Match, inKeyrange)
			}
			reverseBls.Filter.Rules = append(reverseBls.Filter.Rules, &binlogdatapb.Rule{
				Match:  rule.Match,
				Filter: filter,
			})
		}
		log.Infof("Creating reverse workflow vreplication stream on tablet %s: workflow %s, startPos %s",
			source.master.Alias, ts.reverseWorkflow, target.position)
		_, err := ts.wr.VReplicationExec(ctx, source.master.Alias, binlogplayer.CreateVReplicationState(ts.reverseWorkflow, reverseBls, target.position, binlogplayer.BlpStopped, source.master.DbName()))
		if err != nil {
			return err
		}

		// if user has defined the cell/tablet_types parameters in the forward workflow, update the reverse workflow as well
		updateQuery := ts.getReverseVReplicationUpdateQuery(target.master.Alias.Cell, source.master.Alias.Cell, source.master.DbName())
		if updateQuery != "" {
			log.Infof("Updating vreplication stream entry on %s with: %s", source.master.Alias, updateQuery)
			_, err = ts.wr.VReplicationExec(ctx, source.master.Alias, updateQuery)
			return err
		}
		return nil
	})
	return err
}

func (ts *trafficSwitcher) getReverseVReplicationUpdateQuery(targetCell string, sourceCell string, dbname string) string {
	// we try to be clever to understand what user intends:
	// if target's cell is present in cells but not source's cell we replace it with the source's cell
	if ts.optCells != "" && targetCell != sourceCell && strings.Contains(ts.optCells+",", targetCell+",") &&
		!strings.Contains(ts.optCells+",", sourceCell+",") {
		ts.optCells = strings.Replace(ts.optCells, targetCell, sourceCell, 1)
	}

	if ts.optCells != "" || ts.optTabletTypes != "" {
		query := fmt.Sprintf("update _vt.vreplication set cell = '%s', tablet_types = '%s' where workflow = '%s' and db_name = '%s'",
			ts.optCells, ts.optTabletTypes, ts.reverseWorkflow, dbname)
		return query
	}
	return ""
}

func (ts *trafficSwitcher) deleteReverseVReplication(ctx context.Context) error {
	return ts.forAllSources(func(source *tsSource) error {
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow=%s", encodeString(source.master.DbName()), encodeString(ts.reverseWorkflow))
		_, err := ts.wr.tmc.VReplicationExec(ctx, source.master.Tablet, query)
		return err
	})
}

func (ts *trafficSwitcher) createJournals(ctx context.Context, sourceWorkflows []string) error {
	log.Infof("In createJournals for source workflows %+v", sourceWorkflows)
	return ts.forAllSources(func(source *tsSource) error {
		if source.journaled {
			return nil
		}
		participants := make([]*binlogdatapb.KeyspaceShard, 0)
		participantMap := make(map[string]bool)
		journal := &binlogdatapb.Journal{
			Id:              ts.id,
			MigrationType:   ts.migrationType,
			Tables:          ts.tables,
			LocalPosition:   source.position,
			Participants:    participants,
			SourceWorkflows: sourceWorkflows,
		}
		for targetShard, target := range ts.targets {
			for _, tsource := range target.sources {
				participantMap[tsource.Shard] = true
			}
			journal.ShardGtids = append(journal.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: ts.targetKeyspace,
				Shard:    targetShard,
				Gtid:     target.position,
			})
		}
		shards := make([]string, 0)
		for shard := range participantMap {
			shards = append(shards, shard)
		}
		sort.Sort(vreplication.ShardSorter(shards))
		for _, shard := range shards {
			journal.Participants = append(journal.Participants, &binlogdatapb.KeyspaceShard{
				Keyspace: source.si.Keyspace(),
				Shard:    shard,
			})

		}
		log.Infof("Creating journal %v", journal)
		ts.wr.Logger().Infof("Creating journal: %v", journal)
		statement := fmt.Sprintf("insert into _vt.resharding_journal "+
			"(id, db_name, val) "+
			"values (%v, %v, %v)",
			ts.id, encodeString(source.master.DbName()), encodeString(journal.String()))
		if _, err := ts.wr.tmc.VReplicationExec(ctx, source.master.Tablet, statement); err != nil {
			return err
		}
		return nil
	})
}

func (ts *trafficSwitcher) allowTargetWrites(ctx context.Context) error {
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		return ts.allowTableTargetWrites(ctx)
	}
	return ts.changeShardsAccess(ctx, ts.targetKeyspace, ts.targetShards(), allowWrites)
}

func (ts *trafficSwitcher) allowTableTargetWrites(ctx context.Context) error {
	return ts.forAllTargets(func(target *tsTarget) error {
		if _, err := ts.wr.ts.UpdateShardFields(ctx, ts.targetKeyspace, target.si.ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, true, ts.tables)
		}); err != nil {
			return err
		}
		return ts.wr.RefreshTabletsByShard(ctx, target.si, nil)
	})
}

func (ts *trafficSwitcher) changeRouting(ctx context.Context) error {
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		return ts.changeWriteRoute(ctx)
	}
	return ts.changeShardRouting(ctx)
}

func (ts *trafficSwitcher) changeWriteRoute(ctx context.Context) error {
	rules, err := ts.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	for _, table := range ts.tables {
		delete(rules, ts.targetKeyspace+"."+table)
		ts.wr.Logger().Infof("Delete routing: %v", ts.targetKeyspace+"."+table)
		rules[table] = []string{ts.targetKeyspace + "." + table}
		rules[ts.sourceKeyspace+"."+table] = []string{ts.targetKeyspace + "." + table}
		ts.wr.Logger().Infof("Add routing: %v %v", table, ts.sourceKeyspace+"."+table)
	}
	if err := ts.wr.saveRoutingRules(ctx, rules); err != nil {
		return err
	}
	return ts.wr.ts.RebuildSrvVSchema(ctx, nil)
}

func (ts *trafficSwitcher) changeShardRouting(ctx context.Context) error {
	if err := ts.wr.ts.ValidateSrvKeyspace(ctx, ts.targetKeyspace, ""); err != nil {
		err2 := vterrors.Wrapf(err, "Before changing shard routes, found SrvKeyspace for %s is corrupt", ts.targetKeyspace)
		log.Errorf("%w", err2)
		return err2
	}
	err := ts.forAllSources(func(source *tsSource) error {
		_, err := ts.wr.ts.UpdateShardFields(ctx, ts.sourceKeyspace, source.si.ShardName(), func(si *topo.ShardInfo) error {
			si.IsMasterServing = false
			return nil
		})
		return err
	})
	if err != nil {
		return err
	}
	err = ts.forAllTargets(func(target *tsTarget) error {
		_, err := ts.wr.ts.UpdateShardFields(ctx, ts.targetKeyspace, target.si.ShardName(), func(si *topo.ShardInfo) error {
			si.IsMasterServing = true
			return nil
		})
		return err
	})
	if err != nil {
		return err
	}
	err = ts.wr.ts.MigrateServedType(ctx, ts.targetKeyspace, ts.targetShards(), ts.sourceShards(), topodatapb.TabletType_MASTER, nil)
	if err != nil {
		return err
	}
	if err := ts.wr.ts.ValidateSrvKeyspace(ctx, ts.targetKeyspace, ""); err != nil {
		err2 := vterrors.Wrapf(err, "After changing shard routes, found SrvKeyspace for %s is corrupt", ts.targetKeyspace)
		log.Errorf("%w", err2)
		return err2
	}
	return nil
}

func (ts *trafficSwitcher) startReverseVReplication(ctx context.Context) error {
	return ts.forAllSources(func(source *tsSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s", encodeString(source.master.DbName()))
		_, err := ts.wr.VReplicationExec(ctx, source.master.Alias, query)
		return err
	})
}

func (ts *trafficSwitcher) changeShardsAccess(ctx context.Context, keyspace string, shards []*topo.ShardInfo, access accessType) error {
	if err := ts.wr.ts.UpdateDisableQueryService(ctx, ts.sourceKeyspace, shards, topodatapb.TabletType_MASTER, nil, access == disallowWrites /* disable */); err != nil {
		return err
	}
	return ts.wr.refreshMasters(ctx, shards)
}

func (ts *trafficSwitcher) forAllSources(f func(*tsSource) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, source := range ts.sources {
		wg.Add(1)
		go func(source *tsSource) {
			defer wg.Done()

			if err := f(source); err != nil {
				allErrors.RecordError(err)
			}
		}(source)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (ts *trafficSwitcher) forAllTargets(f func(*tsTarget) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range ts.targets {
		wg.Add(1)
		go func(target *tsTarget) {
			defer wg.Done()

			if err := f(target); err != nil {
				allErrors.RecordError(err)
			}
		}(target)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (ts *trafficSwitcher) forAllUids(f func(target *tsTarget, uid uint32) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range ts.targets {
		for uid := range target.sources {
			wg.Add(1)
			go func(target *tsTarget, uid uint32) {
				defer wg.Done()

				if err := f(target, uid); err != nil {
					allErrors.RecordError(err)
				}
			}(target, uid)
		}
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (ts *trafficSwitcher) sourceShards() []*topo.ShardInfo {
	shards := make([]*topo.ShardInfo, 0, len(ts.sources))
	for _, source := range ts.sources {
		shards = append(shards, source.si)
	}
	return shards
}

func (ts *trafficSwitcher) targetShards() []*topo.ShardInfo {
	shards := make([]*topo.ShardInfo, 0, len(ts.targets))
	for _, target := range ts.targets {
		shards = append(shards, target.si)
	}
	return shards
}

func (ts *trafficSwitcher) dropSourceBlacklistedTables(ctx context.Context) error {
	return ts.forAllSources(func(source *tsSource) error {
		if _, err := ts.wr.ts.UpdateShardFields(ctx, ts.sourceKeyspace, source.si.ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, true, ts.tables)
		}); err != nil {
			return err
		}
		return ts.wr.RefreshTabletsByShard(ctx, source.si, nil)
	})
}

func (ts *trafficSwitcher) validateWorkflowHasCompleted(ctx context.Context) error {
	return doValidateWorkflowHasCompleted(ctx, ts)
}

func doValidateWorkflowHasCompleted(ctx context.Context, ts *trafficSwitcher) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	if ts.migrationType == binlogdatapb.MigrationType_SHARDS {
		_ = ts.forAllSources(func(source *tsSource) error {
			wg.Add(1)
			if source.si.IsMasterServing {
				rec.RecordError(fmt.Errorf(fmt.Sprintf("Shard %s is still serving", source.si.ShardName())))
			}
			wg.Done()
			return nil
		})
	} else {
		_ = ts.forAllTargets(func(target *tsTarget) error {
			wg.Add(1)
			query := fmt.Sprintf("select 1 from _vt.vreplication where db_name='%s' and workflow='%s' and message!='FROZEN'", target.master.DbName(), ts.workflow)
			rs, _ := ts.wr.VReplicationExec(ctx, target.master.Alias, query)
			if len(rs.Rows) > 0 {
				rec.RecordError(fmt.Errorf("vreplication streams are not frozen on tablet %d", target.master.Alias.Uid))
			}
			wg.Done()
			return nil
		})
	}

	//check if table is routable
	wg.Wait()
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		rules, err := ts.wr.getRoutingRules(ctx)
		if err != nil {
			rec.RecordError(fmt.Errorf("could not get RoutingRules"))
		}
		for fromTable, toTables := range rules {
			for _, toTable := range toTables {
				for _, table := range ts.tables {
					if toTable == fmt.Sprintf("%s.%s", ts.sourceKeyspace, table) {
						rec.RecordError(fmt.Errorf("routing still exists from keyspace %s table %s to %s", ts.sourceKeyspace, table, fromTable))
					}
				}
			}
		}
	}
	if rec.HasErrors() {
		return fmt.Errorf("%s", strings.Join(rec.ErrorStrings(), "\n"))
	}
	return nil

}

func getRenameFileName(tableName string) string {
	return fmt.Sprintf(renameTableTemplate, tableName)
}

func (ts *trafficSwitcher) removeSourceTables(ctx context.Context, removalType TableRemovalType) error {
	err := ts.forAllSources(func(source *tsSource) error {
		for _, tableName := range ts.tables {
			query := fmt.Sprintf("drop table %s.%s", source.master.DbName(), tableName)
			if removalType == DropTable {
				ts.wr.Logger().Infof("Dropping table %s.%s\n", source.master.DbName(), tableName)
			} else {
				renameName := getRenameFileName(tableName)
				ts.wr.Logger().Infof("Renaming table %s.%s to %s.%s\n", source.master.DbName(), tableName, source.master.DbName(), renameName)
				query = fmt.Sprintf("rename table %s.%s TO %s.%s", source.master.DbName(), tableName, source.master.DbName(), renameName)
			}
			_, err := ts.wr.ExecuteFetchAsDba(ctx, source.master.Alias, query, 1, false, true)
			if err != nil {
				ts.wr.Logger().Errorf("Error removing table %s: %v", tableName, err)
				return err
			}
			ts.wr.Logger().Infof("Removed table %s.%s\n", source.master.DbName(), tableName)

		}
		return nil
	})
	if err != nil {
		return err
	}

	return ts.dropParticipatingTablesFromKeyspace(ctx, ts.sourceKeyspace)
}

func (ts *trafficSwitcher) dropParticipatingTablesFromKeyspace(ctx context.Context, keyspace string) error {
	vschema, err := ts.wr.ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return err
	}
	for _, tableName := range ts.tables {
		delete(vschema.Tables, tableName)
	}
	return ts.wr.ts.SaveVSchema(ctx, keyspace, vschema)
}

// FIXME: even after dropSourceShards there are still entries in the topo, need to research and fix
func (ts *trafficSwitcher) dropSourceShards(ctx context.Context) error {
	return ts.forAllSources(func(source *tsSource) error {
		ts.wr.Logger().Infof("Deleting shard %s.%s\n", source.si.Keyspace(), source.si.ShardName())
		err := ts.wr.DeleteShard(ctx, source.si.Keyspace(), source.si.ShardName(), true, false)
		if err != nil {
			ts.wr.Logger().Errorf("Error deleting shard %s: %v", source.si.ShardName(), err)
			return err
		}
		ts.wr.Logger().Infof("Deleted shard %s.%s\n", source.si.Keyspace(), source.si.ShardName())
		return nil
	})
}

func (ts *trafficSwitcher) freezeTargetVReplication(ctx context.Context) error {
	// Mark target streams as frozen before deleting. If SwitchWrites gets
	// re-invoked after a freeze, it will skip all the previous steps
	err := ts.forAllTargets(func(target *tsTarget) error {
		ts.wr.Logger().Infof("Marking target streams frozen for workflow %s db_name %s", ts.workflow, target.master.DbName())
		query := fmt.Sprintf("update _vt.vreplication set message = '%s' where db_name=%s and workflow=%s", frozenStr, encodeString(target.master.DbName()), encodeString(ts.workflow))
		_, err := ts.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query)
		return err
	})
	if err != nil {
		return err
	}
	return nil
}

func (ts *trafficSwitcher) dropTargetVReplicationStreams(ctx context.Context) error {
	return ts.forAllTargets(func(target *tsTarget) error {
		ts.wr.Logger().Infof("Deleting target streams for workflow %s db_name %s", ts.workflow, target.master.DbName())
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow=%s", encodeString(target.master.DbName()), encodeString(ts.workflow))
		_, err := ts.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query)
		return err
	})
}

func (ts *trafficSwitcher) dropSourceReverseVReplicationStreams(ctx context.Context) error {
	return ts.forAllSources(func(source *tsSource) error {
		ts.wr.Logger().Infof("Deleting reverse streams for workflow %s db_name %s", ts.workflow, source.master.DbName())
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow=%s",
			encodeString(source.master.DbName()), encodeString(reverseName(ts.workflow)))
		_, err := ts.wr.tmc.VReplicationExec(ctx, source.master.Tablet, query)
		return err
	})
}

func (ts *trafficSwitcher) removeTargetTables(ctx context.Context) error {
	log.Infof("removeTargetTables")
	err := ts.forAllTargets(func(target *tsTarget) error {
		for _, tableName := range ts.tables {
			query := fmt.Sprintf("drop table %s.%s", target.master.DbName(), tableName)
			ts.wr.Logger().Infof("Dropping table %s.%s\n", target.master.DbName(), tableName)
			_, err := ts.wr.ExecuteFetchAsDba(ctx, target.master.Alias, query, 1, false, true)
			if err != nil {
				ts.wr.Logger().Errorf("Error removing table %s: %v", tableName, err)
				return err
			}
			ts.wr.Logger().Infof("Removed table %s.%s\n", target.master.DbName(), tableName)

		}
		return nil
	})
	if err != nil {
		return err
	}

	return ts.dropParticipatingTablesFromKeyspace(ctx, ts.targetKeyspace)

}

func (ts *trafficSwitcher) dropTargetShards(ctx context.Context) error {
	return ts.forAllTargets(func(target *tsTarget) error {
		ts.wr.Logger().Infof("Deleting shard %s.%s\n", target.si.Keyspace(), target.si.ShardName())
		err := ts.wr.DeleteShard(ctx, target.si.Keyspace(), target.si.ShardName(), true, false)
		if err != nil {
			ts.wr.Logger().Errorf("Error deleting shard %s: %v", target.si.ShardName(), err)
			return err
		}
		ts.wr.Logger().Infof("Deleted shard %s.%s\n", target.si.Keyspace(), target.si.ShardName())
		return nil
	})
}

func (ts *trafficSwitcher) deleteRoutingRules(ctx context.Context) error {
	rules, err := ts.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	for _, table := range ts.tables {
		delete(rules, table)
		delete(rules, table+"@replica")
		delete(rules, table+"@rdonly")
		delete(rules, ts.targetKeyspace+"."+table)
		delete(rules, ts.targetKeyspace+"."+table+"@replica")
		delete(rules, ts.targetKeyspace+"."+table+"@rdonly")
		delete(rules, ts.sourceKeyspace+"."+table)
		delete(rules, ts.sourceKeyspace+"."+table+"@replica")
		delete(rules, ts.sourceKeyspace+"."+table+"@rdonly")
	}
	if err := ts.wr.saveRoutingRules(ctx, rules); err != nil {
		return err
	}
	return nil
}

func (wr *Wrangler) getRoutingRules(ctx context.Context) (map[string][]string, error) {
	rrs, err := wr.ts.GetRoutingRules(ctx)
	if err != nil {
		return nil, err
	}
	rules := make(map[string][]string, len(rrs.Rules))
	for _, rr := range rrs.Rules {
		rules[rr.FromTable] = rr.ToTables
	}
	return rules, nil
}

func (wr *Wrangler) saveRoutingRules(ctx context.Context, rules map[string][]string) error {
	log.Infof("Saving routing rules %v\n", rules)
	rrs := &vschemapb.RoutingRules{Rules: make([]*vschemapb.RoutingRule, 0, len(rules))}
	for from, to := range rules {
		rrs.Rules = append(rrs.Rules, &vschemapb.RoutingRule{
			FromTable: from,
			ToTables:  to,
		})
	}
	return wr.ts.SaveRoutingRules(ctx, rrs)
}

func reverseName(workflow string) string {
	const reverse = "_reverse"
	if strings.HasSuffix(workflow, reverse) {
		return workflow[:len(workflow)-len(reverse)]
	}
	return workflow + reverse
}

// addParticipatingTablesToKeyspace updates the vschema with the new tables that were created as part of the
// Migrate flow. It is called when the Migrate flow is Completed
func (ts *trafficSwitcher) addParticipatingTablesToKeyspace(ctx context.Context, keyspace, tableSpecs string) error {
	var err error
	var vschema *vschemapb.Keyspace
	vschema, err = ts.wr.ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return err
	}
	if vschema == nil {
		return fmt.Errorf("no vschema found for keyspace %s", keyspace)
	}
	if vschema.Tables == nil {
		vschema.Tables = make(map[string]*vschemapb.Table)
	}
	if strings.HasPrefix(tableSpecs, "{") { // user defined the vschema snippet, typically for a sharded target
		wrap := fmt.Sprintf(`{"tables": %s}`, tableSpecs)
		ks := &vschemapb.Keyspace{}
		if err := json2.Unmarshal([]byte(wrap), ks); err != nil {
			return err
		}
		if err != nil {
			return err
		}
		for table, vtab := range ks.Tables {
			vschema.Tables[table] = vtab
		}
	} else {
		if vschema.Sharded {
			return fmt.Errorf("no sharded vschema was provided, so you will need to update the vschema of the target manually for the moved tables")
		}
		for _, table := range ts.tables {
			vschema.Tables[table] = &vschemapb.Table{}
		}
	}
	return ts.wr.ts.SaveVSchema(ctx, keyspace, vschema)
}
