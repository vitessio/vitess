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
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

const (
	frozenStr      = "FROZEN"
	errorNoStreams = "no streams found in keyspace %s for: %s"
	// use pt-osc's naming convention, this format also ensures vstreamer ignores such tables
	renameTableTemplate = "_%.59s_old" // limit table name to 64 characters
)

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
	sources         map[string]*workflow.MigrationSource
	targets         map[string]*workflow.MigrationTarget
	sourceKeyspace  string
	targetKeyspace  string
	tables          []string
	sourceKSSchema  *vindexes.KeyspaceSchema
	optCells        string //cells option passed to MoveTables/Reshard
	optTabletTypes  string //tabletTypes option passed to MoveTables/Reshard
	externalCluster string
	externalTopo    *topo.Server
}

/*
begin: implementation of workflow.ITrafficSwitcher

(NOTE:@ajm188) Please see comments on that interface type for why this exists.
This is temporary to allow workflow.StreamMigrator to use this trafficSwitcher
code and should be removed in the very near-term when we move trafficSwitcher to
package workflow as well.
*/

var _ workflow.ITrafficSwitcher = (*trafficSwitcher)(nil)

func (ts *trafficSwitcher) TopoServer() *topo.Server                          { return ts.wr.ts }
func (ts *trafficSwitcher) TabletManagerClient() tmclient.TabletManagerClient { return ts.wr.tmc }
func (ts *trafficSwitcher) Logger() logutil.Logger                            { return ts.wr.logger }
func (ts *trafficSwitcher) VReplicationExec(ctx context.Context, alias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error) {
	return ts.wr.VReplicationExec(ctx, alias, query)
}

func (ts *trafficSwitcher) MigrationType() binlogdatapb.MigrationType      { return ts.migrationType }
func (ts *trafficSwitcher) ReverseWorkflowName() string                    { return ts.reverseWorkflow }
func (ts *trafficSwitcher) SourceKeyspaceName() string                     { return ts.sourceKSSchema.Keyspace.Name }
func (ts *trafficSwitcher) SourceKeyspaceSchema() *vindexes.KeyspaceSchema { return ts.sourceKSSchema }
func (ts *trafficSwitcher) WorkflowName() string                           { return ts.workflow }

func (ts *trafficSwitcher) ForAllSources(f func(source *workflow.MigrationSource) error) error {
	return ts.forAllSources(f)
}
func (ts *trafficSwitcher) ForAllTargets(f func(source *workflow.MigrationTarget) error) error {
	return ts.forAllTargets(f)
}

/* end: implementation of workflow.ITrafficSwitcher */

// For a Reshard, to check whether we have switched reads for a tablet type, we check if any one of the source shards has
// the query service disabled in its tablet control record
func (wr *Wrangler) getCellsWithShardReadsSwitched(ctx context.Context, targetKeyspace string, si *topo.ShardInfo, tabletTypeStr string) (
	cellsSwitched, cellsNotSwitched []string, err error) {

	tabletType, err := topoproto.ParseTabletType(tabletTypeStr)
	if err != nil {
		return nil, nil, err
	}

	s := workflow.NewServer(wr.ts, wr.tmc)
	return s.GetCellsWithShardReadsSwitched(ctx, targetKeyspace, si, tabletType)
}

// For MoveTables,  to check whether we have switched reads for a tablet type, we check whether the routing rule
// for the tablet_type is pointing to the target keyspace
func (wr *Wrangler) getCellsWithTableReadsSwitched(ctx context.Context, targetKeyspace, table, tabletTypeStr string) (
	cellsSwitched, cellsNotSwitched []string, err error) {

	tabletType, err := topoproto.ParseTabletType(tabletTypeStr)
	if err != nil {
		return nil, nil, err
	}

	s := workflow.NewServer(wr.ts, wr.tmc)
	return s.GetCellsWithTableReadsSwitched(ctx, targetKeyspace, table, tabletType)
}

func (wr *Wrangler) getWorkflowState(ctx context.Context, targetKeyspace, workflowName string) (*trafficSwitcher, *workflow.State, error) {
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflowName)

	if ts == nil || err != nil {
		if errors.Is(err, workflow.ErrNoStreams) || err.Error() == fmt.Sprintf(errorNoStreams, targetKeyspace, workflowName) {
			return nil, nil, nil
		}
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, nil, err
	}

	ws := &workflow.State{Workflow: workflowName, TargetKeyspace: targetKeyspace}
	ws.SourceKeyspace = ts.sourceKeyspace
	var cellsSwitched, cellsNotSwitched []string
	var keyspace string
	var reverse bool

	// we reverse writes by using the source_keyspace.workflowname_reverse workflow spec, so we need to use the
	// source of the reverse workflow, which is the target of the workflow initiated by the user for checking routing rules
	// Similarly we use a target shard of the reverse workflow as the original source to check if writes have been switched
	if strings.HasSuffix(workflowName, "_reverse") {
		reverse = true
		keyspace = ws.SourceKeyspace
		workflowName = workflow.ReverseWorkflowName(workflowName)
	} else {
		keyspace = targetKeyspace
	}
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		ws.WorkflowType = workflow.TypeMoveTables

		// we assume a consistent state, so only choose routing rule for one table for replica/rdonly
		if len(ts.tables) == 0 {
			return nil, nil, fmt.Errorf("no tables in workflow %s.%s", keyspace, workflowName)

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
		rules, err := topotools.GetRoutingRules(ctx, ts.wr.ts)
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
		ws.WorkflowType = workflow.TypeReshard

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
func (wr *Wrangler) SwitchReads(ctx context.Context, targetKeyspace, workflowName string, servedTypes []topodatapb.TabletType,
	cells []string, direction workflow.TrafficSwitchDirection, dryRun bool) (*[]string, error) {

	ts, ws, err := wr.getWorkflowState(ctx, targetKeyspace, workflowName)
	if err != nil {
		wr.Logger().Errorf("getWorkflowState failed: %v", err)
		return nil, err
	}
	if ts == nil {
		errorMsg := fmt.Sprintf("workflow %s not found in keyspace %s", workflowName, targetKeyspace)
		wr.Logger().Errorf(errorMsg)
		return nil, fmt.Errorf(errorMsg)
	}
	log.Infof("SwitchReads: %s.%s tt %+v, cells %+v, workflow state: %+v", targetKeyspace, workflowName, servedTypes, cells, ws)
	var switchReplicas, switchRdonly bool
	for _, servedType := range servedTypes {
		if servedType != topodatapb.TabletType_REPLICA && servedType != topodatapb.TabletType_RDONLY {
			return nil, fmt.Errorf("tablet type must be REPLICA or RDONLY: %v", servedType)
		}
		if direction == workflow.DirectionBackward && servedType == topodatapb.TabletType_REPLICA && len(ws.ReplicaCellsSwitched) == 0 {
			return nil, fmt.Errorf("requesting reversal of SwitchReads for REPLICAs but REPLICA reads have not been switched")
		}
		if direction == workflow.DirectionBackward && servedType == topodatapb.TabletType_RDONLY && len(ws.RdonlyCellsSwitched) == 0 {
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
func (wr *Wrangler) SwitchWrites(ctx context.Context, targetKeyspace, workflowName string, timeout time.Duration, cancel, reverse, reverseReplication bool, dryRun bool) (journalID int64, dryRunResults *[]string, err error) {
	ts, ws, err := wr.getWorkflowState(ctx, targetKeyspace, workflowName)
	_ = ws
	if err != nil {
		wr.Logger().Errorf("getWorkflowState failed: %v", err)
		return 0, nil, err
	}
	if ts == nil {
		errorMsg := fmt.Sprintf("workflow %s not found in keyspace %s", workflowName, targetKeyspace)
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
		sm, err := workflow.BuildStreamMigrator(ctx, ts, cancel)
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
			for key, streams := range sm.Streams() {
				for _, stream := range streams {
					ts.wr.Logger().Errorf("stream in stopStreams: key %s shard %s stream %+v", key, stream.BinlogSource.Shard, stream.BinlogSource)
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
func (wr *Wrangler) DropSources(ctx context.Context, targetKeyspace, workflowName string, removalType workflow.TableRemovalType, keepData, force, dryRun bool) (*[]string, error) {
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflowName)
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

func (wr *Wrangler) buildTrafficSwitcher(ctx context.Context, targetKeyspace, workflowName string) (*trafficSwitcher, error) {
	tgtInfo, err := workflow.BuildTargets(ctx, wr.ts, wr.tmc, targetKeyspace, workflowName)
	if err != nil {
		log.Infof("Error building targets: %s", err)
		return nil, err
	}
	targets, frozen, optCells, optTabletTypes := tgtInfo.Targets, tgtInfo.Frozen, tgtInfo.OptCells, tgtInfo.OptTabletTypes

	ts := &trafficSwitcher{
		wr:              wr,
		workflow:        workflowName,
		reverseWorkflow: workflow.ReverseWorkflowName(workflowName),
		id:              workflow.HashStreams(targetKeyspace, targets),
		targets:         targets,
		sources:         make(map[string]*workflow.MigrationSource),
		targetKeyspace:  targetKeyspace,
		frozen:          frozen,
		optCells:        optCells,
		optTabletTypes:  optTabletTypes,
	}
	log.Infof("Migration ID for workflow %s: %d", workflowName, ts.id)
	sourceTopo := wr.ts

	// Build the sources
	for _, target := range targets {
		for _, bls := range target.Sources {
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
			sourcePrimary, err := sourceTopo.GetTablet(ctx, sourcesi.MasterAlias)
			if err != nil {
				return nil, err
			}
			ts.sources[bls.Shard] = workflow.NewMigrationSource(sourcesi, sourcePrimary)
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

func (ts *trafficSwitcher) switchTableReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction workflow.TrafficSwitchDirection) error {
	log.Infof("switchTableReads: servedTypes: %+v, direction %t", servedTypes, direction)
	rules, err := topotools.GetRoutingRules(ctx, ts.wr.ts)
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
			if direction == workflow.DirectionForward {
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
	if err := topotools.SaveRoutingRules(ctx, ts.wr.ts, rules); err != nil {
		return err
	}
	return ts.wr.ts.RebuildSrvVSchema(ctx, cells)
}

func (ts *trafficSwitcher) switchShardReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction workflow.TrafficSwitchDirection) error {
	var fromShards, toShards []*topo.ShardInfo
	if direction == workflow.DirectionForward {
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

// checkJournals returns true if at least one journal has been created.
// If so, it also returns the list of sourceWorkflows that need to be switched.
func (ts *trafficSwitcher) checkJournals(ctx context.Context) (journalsExist bool, sourceWorkflows []string, err error) {
	var (
		ws = workflow.NewServer(ts.wr.ts, ts.wr.tmc)
		mu sync.Mutex
	)

	err = ts.forAllSources(func(source *workflow.MigrationSource) error {
		mu.Lock()
		defer mu.Unlock()
		journal, exists, err := ws.CheckReshardingJournalExistsOnTablet(ctx, source.GetPrimary().Tablet, ts.id)
		if err != nil {
			return err
		}
		if exists {
			if journal.Id != 0 {
				sourceWorkflows = journal.SourceWorkflows
			}
			source.Journaled = true
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
	return ts.forAllSources(func(source *workflow.MigrationSource) error {
		var err error
		source.Position, err = ts.wr.tmc.MasterPosition(ctx, source.GetPrimary().Tablet)
		ts.wr.Logger().Infof("Stopped Source Writes. Position for source %v:%v: %v",
			ts.sourceKeyspace, source.GetShard().ShardName(), source.Position)
		if err != nil {
			log.Warningf("Error: %s", err)
		}
		return err
	})
}

func (ts *trafficSwitcher) changeTableSourceWrites(ctx context.Context, access accessType) error {
	return ts.forAllSources(func(source *workflow.MigrationSource) error {
		if _, err := ts.wr.ts.UpdateShardFields(ctx, ts.sourceKeyspace, source.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, access == allowWrites /* remove */, ts.tables)
		}); err != nil {
			return err
		}
		return ts.wr.RefreshTabletsByShard(ctx, source.GetShard(), nil)
	})
}

func (ts *trafficSwitcher) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()
	// source writes have been stopped, wait for all streams on targets to catch up
	if err := ts.forAllUids(func(target *workflow.MigrationTarget, uid uint32) error {
		ts.wr.Logger().Infof("Before Catchup: uid: %d, target master %s, target position %s, shard %s", uid,
			target.GetPrimary().AliasString(), target.Position, target.GetShard().String())
		bls := target.Sources[uid]
		source := ts.sources[bls.Shard]
		ts.wr.Logger().Infof("Before Catchup: waiting for keyspace:shard: %v:%v to reach source position %v, uid %d",
			ts.targetKeyspace, target.GetShard().ShardName(), source.Position, uid)
		if err := ts.wr.tmc.VReplicationWaitForPos(ctx, target.GetPrimary().Tablet, int(uid), source.Position); err != nil {
			return err
		}
		log.Infof("After catchup: target keyspace:shard: %v:%v, source position %v, uid %d",
			ts.targetKeyspace, target.GetShard().ShardName(), source.Position, uid)
		ts.wr.Logger().Infof("After catchup: position for keyspace:shard: %v:%v reached, uid %d",
			ts.targetKeyspace, target.GetShard().ShardName(), uid)
		if _, err := ts.wr.tmc.VReplicationExec(ctx, target.GetPrimary().Tablet, binlogplayer.StopVReplication(uid, "stopped for cutover")); err != nil {
			log.Infof("error marking stopped for cutover on %s, uid %d", target.GetPrimary().AliasString(), uid)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	// all targets have caught up, record their positions for setting up reverse workflows
	return ts.forAllTargets(func(target *workflow.MigrationTarget) error {
		var err error
		target.Position, err = ts.wr.tmc.MasterPosition(ctx, target.GetPrimary().Tablet)
		ts.wr.Logger().Infof("After catchup, position for target master %s, %v", target.GetPrimary().AliasString(), target.Position)
		return err
	})
}

func (ts *trafficSwitcher) cancelMigration(ctx context.Context, sm *workflow.StreamMigrator) {
	var err error
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		err = ts.changeTableSourceWrites(ctx, allowWrites)
	} else {
		err = ts.changeShardsAccess(ctx, ts.sourceKeyspace, ts.sourceShards(), allowWrites)
	}
	if err != nil {
		ts.wr.Logger().Errorf("Cancel migration failed:", err)
	}

	sm.CancelMigration(ctx)

	err = ts.forAllTargets(func(target *workflow.MigrationTarget) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s", encodeString(target.GetPrimary().DbName()), encodeString(ts.workflow))
		_, err := ts.wr.tmc.VReplicationExec(ctx, target.GetPrimary().Tablet, query)
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
	err := ts.forAllSources(func(source *workflow.MigrationSource) error {
		var err error
		source.Position, err = ts.wr.tmc.MasterPosition(ctx, source.GetPrimary().Tablet)
		ts.wr.Logger().Infof("Position for source %v:%v: %v", ts.sourceKeyspace, source.GetShard().ShardName(), source.Position)
		return err
	})
	if err != nil {
		return err
	}
	return ts.forAllTargets(func(target *workflow.MigrationTarget) error {
		var err error
		target.Position, err = ts.wr.tmc.MasterPosition(ctx, target.GetPrimary().Tablet)
		ts.wr.Logger().Infof("Position for target %v:%v: %v", ts.targetKeyspace, target.GetShard().ShardName(), target.Position)
		return err
	})
}

func (ts *trafficSwitcher) createReverseVReplication(ctx context.Context) error {
	if err := ts.deleteReverseVReplication(ctx); err != nil {
		return err
	}
	err := ts.forAllUids(func(target *workflow.MigrationTarget, uid uint32) error {
		bls := target.Sources[uid]
		source := ts.sources[bls.Shard]
		reverseBls := &binlogdatapb.BinlogSource{
			Keyspace:   ts.targetKeyspace,
			Shard:      target.GetShard().ShardName(),
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
					filter = key.KeyRangeString(source.GetShard().KeyRange)
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
					inKeyrange = fmt.Sprintf(" where in_keyrange(%s, '%s', '%s')", sqlparser.String(vtable.ColumnVindexes[0].Columns[0]), vtable.ColumnVindexes[0].Type, key.KeyRangeString(source.GetShard().KeyRange))
				}
				filter = fmt.Sprintf("select * from %s%s", rule.Match, inKeyrange)
			}
			reverseBls.Filter.Rules = append(reverseBls.Filter.Rules, &binlogdatapb.Rule{
				Match:  rule.Match,
				Filter: filter,
			})
		}
		log.Infof("Creating reverse workflow vreplication stream on tablet %s: workflow %s, startPos %s",
			source.GetPrimary().Alias, ts.reverseWorkflow, target.Position)
		_, err := ts.wr.VReplicationExec(ctx, source.GetPrimary().Alias, binlogplayer.CreateVReplicationState(ts.reverseWorkflow, reverseBls, target.Position, binlogplayer.BlpStopped, source.GetPrimary().DbName()))
		if err != nil {
			return err
		}

		// if user has defined the cell/tablet_types parameters in the forward workflow, update the reverse workflow as well
		updateQuery := ts.getReverseVReplicationUpdateQuery(target.GetPrimary().Alias.Cell, source.GetPrimary().Alias.Cell, source.GetPrimary().DbName())
		if updateQuery != "" {
			log.Infof("Updating vreplication stream entry on %s with: %s", source.GetPrimary().Alias, updateQuery)
			_, err = ts.wr.VReplicationExec(ctx, source.GetPrimary().Alias, updateQuery)
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
	return ts.forAllSources(func(source *workflow.MigrationSource) error {
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow=%s", encodeString(source.GetPrimary().DbName()), encodeString(ts.reverseWorkflow))
		_, err := ts.wr.tmc.VReplicationExec(ctx, source.GetPrimary().Tablet, query)
		return err
	})
}

func (ts *trafficSwitcher) createJournals(ctx context.Context, sourceWorkflows []string) error {
	log.Infof("In createJournals for source workflows %+v", sourceWorkflows)
	return ts.forAllSources(func(source *workflow.MigrationSource) error {
		if source.Journaled {
			return nil
		}
		participants := make([]*binlogdatapb.KeyspaceShard, 0)
		participantMap := make(map[string]bool)
		journal := &binlogdatapb.Journal{
			Id:              ts.id,
			MigrationType:   ts.migrationType,
			Tables:          ts.tables,
			LocalPosition:   source.Position,
			Participants:    participants,
			SourceWorkflows: sourceWorkflows,
		}
		for targetShard, target := range ts.targets {
			for _, tsource := range target.Sources {
				participantMap[tsource.Shard] = true
			}
			journal.ShardGtids = append(journal.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: ts.targetKeyspace,
				Shard:    targetShard,
				Gtid:     target.Position,
			})
		}
		shards := make([]string, 0)
		for shard := range participantMap {
			shards = append(shards, shard)
		}
		sort.Sort(vreplication.ShardSorter(shards))
		for _, shard := range shards {
			journal.Participants = append(journal.Participants, &binlogdatapb.KeyspaceShard{
				Keyspace: source.GetShard().Keyspace(),
				Shard:    shard,
			})

		}
		log.Infof("Creating journal %v", journal)
		ts.wr.Logger().Infof("Creating journal: %v", journal)
		statement := fmt.Sprintf("insert into _vt.resharding_journal "+
			"(id, db_name, val) "+
			"values (%v, %v, %v)",
			ts.id, encodeString(source.GetPrimary().DbName()), encodeString(journal.String()))
		if _, err := ts.wr.tmc.VReplicationExec(ctx, source.GetPrimary().Tablet, statement); err != nil {
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
	return ts.forAllTargets(func(target *workflow.MigrationTarget) error {
		if _, err := ts.wr.ts.UpdateShardFields(ctx, ts.targetKeyspace, target.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, true, ts.tables)
		}); err != nil {
			return err
		}
		return ts.wr.RefreshTabletsByShard(ctx, target.GetShard(), nil)
	})
}

func (ts *trafficSwitcher) changeRouting(ctx context.Context) error {
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		return ts.changeWriteRoute(ctx)
	}
	return ts.changeShardRouting(ctx)
}

func (ts *trafficSwitcher) changeWriteRoute(ctx context.Context) error {
	rules, err := topotools.GetRoutingRules(ctx, ts.wr.ts)
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
	if err := topotools.SaveRoutingRules(ctx, ts.wr.ts, rules); err != nil {
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
	err := ts.forAllSources(func(source *workflow.MigrationSource) error {
		_, err := ts.wr.ts.UpdateShardFields(ctx, ts.sourceKeyspace, source.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			si.IsMasterServing = false
			return nil
		})
		return err
	})
	if err != nil {
		return err
	}
	err = ts.forAllTargets(func(target *workflow.MigrationTarget) error {
		_, err := ts.wr.ts.UpdateShardFields(ctx, ts.targetKeyspace, target.GetShard().ShardName(), func(si *topo.ShardInfo) error {
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
	return ts.forAllSources(func(source *workflow.MigrationSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s", encodeString(source.GetPrimary().DbName()))
		_, err := ts.wr.VReplicationExec(ctx, source.GetPrimary().Alias, query)
		return err
	})
}

func (ts *trafficSwitcher) changeShardsAccess(ctx context.Context, keyspace string, shards []*topo.ShardInfo, access accessType) error {
	if err := ts.wr.ts.UpdateDisableQueryService(ctx, keyspace, shards, topodatapb.TabletType_MASTER, nil, access == disallowWrites /* disable */); err != nil {
		return err
	}
	return ts.wr.refreshMasters(ctx, shards)
}

func (ts *trafficSwitcher) forAllSources(f func(*workflow.MigrationSource) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, source := range ts.sources {
		wg.Add(1)
		go func(source *workflow.MigrationSource) {
			defer wg.Done()

			if err := f(source); err != nil {
				allErrors.RecordError(err)
			}
		}(source)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (ts *trafficSwitcher) forAllTargets(f func(*workflow.MigrationTarget) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range ts.targets {
		wg.Add(1)
		go func(target *workflow.MigrationTarget) {
			defer wg.Done()

			if err := f(target); err != nil {
				allErrors.RecordError(err)
			}
		}(target)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (ts *trafficSwitcher) forAllUids(f func(target *workflow.MigrationTarget, uid uint32) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range ts.targets {
		for uid := range target.Sources {
			wg.Add(1)
			go func(target *workflow.MigrationTarget, uid uint32) {
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
		shards = append(shards, source.GetShard())
	}
	return shards
}

func (ts *trafficSwitcher) targetShards() []*topo.ShardInfo {
	shards := make([]*topo.ShardInfo, 0, len(ts.targets))
	for _, target := range ts.targets {
		shards = append(shards, target.GetShard())
	}
	return shards
}

func (ts *trafficSwitcher) dropSourceBlacklistedTables(ctx context.Context) error {
	return ts.forAllSources(func(source *workflow.MigrationSource) error {
		if _, err := ts.wr.ts.UpdateShardFields(ctx, ts.sourceKeyspace, source.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, true, ts.tables)
		}); err != nil {
			return err
		}
		return ts.wr.RefreshTabletsByShard(ctx, source.GetShard(), nil)
	})
}

func (ts *trafficSwitcher) validateWorkflowHasCompleted(ctx context.Context) error {
	return doValidateWorkflowHasCompleted(ctx, ts)
}

func doValidateWorkflowHasCompleted(ctx context.Context, ts *trafficSwitcher) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	if ts.migrationType == binlogdatapb.MigrationType_SHARDS {
		_ = ts.forAllSources(func(source *workflow.MigrationSource) error {
			wg.Add(1)
			if source.GetShard().IsMasterServing {
				rec.RecordError(fmt.Errorf(fmt.Sprintf("Shard %s is still serving", source.GetShard().ShardName())))
			}
			wg.Done()
			return nil
		})
	} else {
		_ = ts.forAllTargets(func(target *workflow.MigrationTarget) error {
			wg.Add(1)
			query := fmt.Sprintf("select 1 from _vt.vreplication where db_name='%s' and workflow='%s' and message!='FROZEN'", target.GetPrimary().DbName(), ts.workflow)
			rs, _ := ts.wr.VReplicationExec(ctx, target.GetPrimary().Alias, query)
			if len(rs.Rows) > 0 {
				rec.RecordError(fmt.Errorf("vreplication streams are not frozen on tablet %d", target.GetPrimary().Alias.Uid))
			}
			wg.Done()
			return nil
		})
	}

	//check if table is routable
	wg.Wait()
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		rules, err := topotools.GetRoutingRules(ctx, ts.wr.ts)
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

func (ts *trafficSwitcher) removeSourceTables(ctx context.Context, removalType workflow.TableRemovalType) error {
	err := ts.forAllSources(func(source *workflow.MigrationSource) error {
		for _, tableName := range ts.tables {
			query := fmt.Sprintf("drop table %s.%s", source.GetPrimary().DbName(), tableName)
			if removalType == workflow.DropTable {
				ts.wr.Logger().Infof("Dropping table %s.%s\n", source.GetPrimary().DbName(), tableName)
			} else {
				renameName := getRenameFileName(tableName)
				ts.wr.Logger().Infof("Renaming table %s.%s to %s.%s\n", source.GetPrimary().DbName(), tableName, source.GetPrimary().DbName(), renameName)
				query = fmt.Sprintf("rename table %s.%s TO %s.%s", source.GetPrimary().DbName(), tableName, source.GetPrimary().DbName(), renameName)
			}
			_, err := ts.wr.ExecuteFetchAsDba(ctx, source.GetPrimary().Alias, query, 1, false, true)
			if err != nil {
				ts.wr.Logger().Errorf("Error removing table %s: %v", tableName, err)
				return err
			}
			ts.wr.Logger().Infof("Removed table %s.%s\n", source.GetPrimary().DbName(), tableName)

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
	return ts.forAllSources(func(source *workflow.MigrationSource) error {
		ts.wr.Logger().Infof("Deleting shard %s.%s\n", source.GetShard().Keyspace(), source.GetShard().ShardName())
		err := ts.wr.DeleteShard(ctx, source.GetShard().Keyspace(), source.GetShard().ShardName(), true, false)
		if err != nil {
			ts.wr.Logger().Errorf("Error deleting shard %s: %v", source.GetShard().ShardName(), err)
			return err
		}
		ts.wr.Logger().Infof("Deleted shard %s.%s\n", source.GetShard().Keyspace(), source.GetShard().ShardName())
		return nil
	})
}

func (ts *trafficSwitcher) freezeTargetVReplication(ctx context.Context) error {
	// Mark target streams as frozen before deleting. If SwitchWrites gets
	// re-invoked after a freeze, it will skip all the previous steps
	err := ts.forAllTargets(func(target *workflow.MigrationTarget) error {
		ts.wr.Logger().Infof("Marking target streams frozen for workflow %s db_name %s", ts.workflow, target.GetPrimary().DbName())
		query := fmt.Sprintf("update _vt.vreplication set message = '%s' where db_name=%s and workflow=%s", frozenStr, encodeString(target.GetPrimary().DbName()), encodeString(ts.workflow))
		_, err := ts.wr.tmc.VReplicationExec(ctx, target.GetPrimary().Tablet, query)
		return err
	})
	if err != nil {
		return err
	}
	return nil
}

func (ts *trafficSwitcher) dropTargetVReplicationStreams(ctx context.Context) error {
	return ts.forAllTargets(func(target *workflow.MigrationTarget) error {
		ts.wr.Logger().Infof("Deleting target streams for workflow %s db_name %s", ts.workflow, target.GetPrimary().DbName())
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow=%s", encodeString(target.GetPrimary().DbName()), encodeString(ts.workflow))
		_, err := ts.wr.tmc.VReplicationExec(ctx, target.GetPrimary().Tablet, query)
		return err
	})
}

func (ts *trafficSwitcher) dropSourceReverseVReplicationStreams(ctx context.Context) error {
	return ts.forAllSources(func(source *workflow.MigrationSource) error {
		ts.wr.Logger().Infof("Deleting reverse streams for workflow %s db_name %s", ts.workflow, source.GetPrimary().DbName())
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow=%s",
			encodeString(source.GetPrimary().DbName()), encodeString(workflow.ReverseWorkflowName(ts.workflow)))
		_, err := ts.wr.tmc.VReplicationExec(ctx, source.GetPrimary().Tablet, query)
		return err
	})
}

func (ts *trafficSwitcher) removeTargetTables(ctx context.Context) error {
	log.Infof("removeTargetTables")
	err := ts.forAllTargets(func(target *workflow.MigrationTarget) error {
		for _, tableName := range ts.tables {
			query := fmt.Sprintf("drop table %s.%s", target.GetPrimary().DbName(), tableName)
			ts.wr.Logger().Infof("Dropping table %s.%s\n", target.GetPrimary().DbName(), tableName)
			_, err := ts.wr.ExecuteFetchAsDba(ctx, target.GetPrimary().Alias, query, 1, false, true)
			if err != nil {
				ts.wr.Logger().Errorf("Error removing table %s: %v", tableName, err)
				return err
			}
			ts.wr.Logger().Infof("Removed table %s.%s\n", target.GetPrimary().DbName(), tableName)

		}
		return nil
	})
	if err != nil {
		return err
	}

	return ts.dropParticipatingTablesFromKeyspace(ctx, ts.targetKeyspace)

}

func (ts *trafficSwitcher) dropTargetShards(ctx context.Context) error {
	return ts.forAllTargets(func(target *workflow.MigrationTarget) error {
		ts.wr.Logger().Infof("Deleting shard %s.%s\n", target.GetShard().Keyspace(), target.GetShard().ShardName())
		err := ts.wr.DeleteShard(ctx, target.GetShard().Keyspace(), target.GetShard().ShardName(), true, false)
		if err != nil {
			ts.wr.Logger().Errorf("Error deleting shard %s: %v", target.GetShard().ShardName(), err)
			return err
		}
		ts.wr.Logger().Infof("Deleted shard %s.%s\n", target.GetShard().Keyspace(), target.GetShard().ShardName())
		return nil
	})
}

func (ts *trafficSwitcher) deleteRoutingRules(ctx context.Context) error {
	rules, err := topotools.GetRoutingRules(ctx, ts.wr.ts)
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
	if err := topotools.SaveRoutingRules(ctx, ts.wr.ts, rules); err != nil {
		return err
	}
	return nil
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
