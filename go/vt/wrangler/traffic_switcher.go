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

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
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
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	errorNoStreams = "no streams found in keyspace %s for: %s"
	// Use pt-osc's naming convention, this format also ensures vstreamer ignores such tables.
	renameTableTemplate = "_%.59s_old" // limit table name to 64 characters

	sqlDeleteWorkflow = "delete from _vt.vreplication where db_name = %s and workflow = %s"

	sqlGetMaxSequenceVal = "select max(%a) as maxval from %a.%a"
	sqlInitSequenceTable = "insert into %a.%a (id, next_id, cache) values (0, %d, 1000) on duplicate key update next_id = if(next_id < %d, %d, next_id)"
)

// accessType specifies the type of access for a shard (allow/disallow writes).
type accessType int

// sequenceMetadata contains all of the relevant metadata for a sequence that
// is being used by a table involved in a vreplication workflow.
type sequenceMetadata struct {
	// The name of the sequence table.
	backingTableName string
	// The keyspace where the backing table lives.
	backingTableKeyspace string
	// The dbName in use by the keyspace where the backing table lives.
	backingTableDBName string
	// The name of the table using the sequence.
	usingTableName string
	// The dbName in use by the keyspace where the using table lives.
	usingTableDBName string
	// The using table definition.
	usingTableDefinition *vschemapb.Table
}

const (
	allowWrites = accessType(iota)
	disallowWrites

	// Number of LOCK TABLES cycles to perform on the sources during SwitchWrites.
	lockTablesCycles = 2
	// Time to wait between LOCK TABLES cycles on the sources during SwitchWrites.
	lockTablesCycleDelay = time.Duration(100 * time.Millisecond)

	// How long to wait when refreshing the state of each tablet in a shard. Note that these
	// are refreshed in parallel, non-topo errors are ignored (in the error handling) and we
	// may only do a partial refresh. Because in some cases it's unsafe to switch the traffic
	// if some tablets do not refresh, we may need to look for partial results and produce
	// an error (with the provided details of WHY) if we see them.
	// Side note: the default lock/lease TTL in etcd is 60s so the default tablet refresh
	// timeout of 60s can cause us to lose our keyspace lock before completing the
	// operation too.
	shardTabletRefreshTimeout = time.Duration(30 * time.Second)
)

// trafficSwitcher contains the metadata for switching read and write traffic
// for vreplication streams.
type trafficSwitcher struct {
	migrationType      binlogdatapb.MigrationType
	isPartialMigration bool
	wr                 *Wrangler
	workflow           string

	// If frozen is true, the rest of the fields are not set.
	frozen           bool
	reverseWorkflow  string
	id               int64
	sources          map[string]*workflow.MigrationSource
	targets          map[string]*workflow.MigrationTarget
	sourceKeyspace   string
	targetKeyspace   string
	tables           []string
	keepRoutingRules bool
	sourceKSSchema   *vindexes.KeyspaceSchema
	optCells         string // cells option passed to MoveTables/Reshard
	optTabletTypes   string // tabletTypes option passed to MoveTables/Reshard
	externalCluster  string
	externalTopo     *topo.Server
	sourceTimeZone   string
	targetTimeZone   string
	workflowType     binlogdatapb.VReplicationWorkflowType
	workflowSubType  binlogdatapb.VReplicationWorkflowSubType
}

func (ts *trafficSwitcher) TopoServer() *topo.Server                          { return ts.wr.ts }
func (ts *trafficSwitcher) TabletManagerClient() tmclient.TabletManagerClient { return ts.wr.tmc }
func (ts *trafficSwitcher) Logger() logutil.Logger                            { return ts.wr.logger }
func (ts *trafficSwitcher) VReplicationExec(ctx context.Context, alias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error) {
	return ts.wr.VReplicationExec(ctx, alias, query)
}

func (ts *trafficSwitcher) ExternalTopo() *topo.Server                     { return ts.externalTopo }
func (ts *trafficSwitcher) MigrationType() binlogdatapb.MigrationType      { return ts.migrationType }
func (ts *trafficSwitcher) IsPartialMigration() bool                       { return ts.isPartialMigration }
func (ts *trafficSwitcher) ReverseWorkflowName() string                    { return ts.reverseWorkflow }
func (ts *trafficSwitcher) SourceKeyspaceName() string                     { return ts.sourceKSSchema.Keyspace.Name }
func (ts *trafficSwitcher) SourceKeyspaceSchema() *vindexes.KeyspaceSchema { return ts.sourceKSSchema }
func (ts *trafficSwitcher) Sources() map[string]*workflow.MigrationSource  { return ts.sources }
func (ts *trafficSwitcher) Tables() []string                               { return ts.tables }
func (ts *trafficSwitcher) TargetKeyspaceName() string                     { return ts.targetKeyspace }
func (ts *trafficSwitcher) Targets() map[string]*workflow.MigrationTarget  { return ts.targets }
func (ts *trafficSwitcher) WorkflowName() string                           { return ts.workflow }
func (ts *trafficSwitcher) SourceTimeZone() string                         { return ts.sourceTimeZone }
func (ts *trafficSwitcher) TargetTimeZone() string                         { return ts.targetTimeZone }

func (ts *trafficSwitcher) ForAllSources(f func(source *workflow.MigrationSource) error) error {
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

func (ts *trafficSwitcher) ForAllTargets(f func(source *workflow.MigrationTarget) error) error {
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

func (ts *trafficSwitcher) ForAllUIDs(f func(target *workflow.MigrationTarget, uid int32) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range ts.Targets() {
		for uid := range target.Sources {
			wg.Add(1)
			go func(target *workflow.MigrationTarget, uid int32) {
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

/* end: implementation of workflow.ITrafficSwitcher */

func (wr *Wrangler) getWorkflowState(ctx context.Context, targetKeyspace, workflowName string) (*trafficSwitcher, *workflow.State, error) {
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflowName)

	if ts == nil || err != nil {
		if errors.Is(err, workflow.ErrNoStreams) || err.Error() == fmt.Sprintf(errorNoStreams, targetKeyspace, workflowName) {
			return nil, nil, nil
		}
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, nil, err
	}

	ws := workflow.NewServer(wr.env, wr.ts, wr.tmc)
	state := &workflow.State{
		Workflow:           workflowName,
		SourceKeyspace:     ts.SourceKeyspaceName(),
		TargetKeyspace:     targetKeyspace,
		IsPartialMigration: ts.isPartialMigration,
	}

	var (
		reverse        bool
		sourceKeyspace string
	)

	// We reverse writes by using the source_keyspace.workflowname_reverse workflow
	// spec, so we need to use the source of the reverse workflow, which is the
	// target of the workflow initiated by the user for checking routing rules.
	// Similarly we use a target shard of the reverse workflow as the original
	// source to check if writes have been switched.
	if strings.HasSuffix(workflowName, "_reverse") {
		reverse = true
		// Flip the source and target keyspaces.
		sourceKeyspace = state.TargetKeyspace
		targetKeyspace = state.SourceKeyspace
		workflowName = workflow.ReverseWorkflowName(workflowName)
	} else {
		sourceKeyspace = state.SourceKeyspace
	}
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		state.WorkflowType = workflow.TypeMoveTables

		// We assume a consistent state, so only choose routing rule for one table.
		if len(ts.Tables()) == 0 {
			return nil, nil, fmt.Errorf("no tables in workflow %s.%s", targetKeyspace, workflowName)

		}
		table := ts.Tables()[0]

		if ts.isPartialMigration { // shard level traffic switching is all or nothing
			shardRoutingRules, err := wr.ts.GetShardRoutingRules(ctx)
			if err != nil {
				return nil, nil, err
			}

			rules := shardRoutingRules.Rules
			for _, rule := range rules {
				switch rule.ToKeyspace {
				case sourceKeyspace:
					state.ShardsNotYetSwitched = append(state.ShardsNotYetSwitched, rule.Shard)
				case targetKeyspace:
					state.ShardsAlreadySwitched = append(state.ShardsAlreadySwitched, rule.Shard)
				default:
					// Not a relevant rule.
				}
			}
		} else {
			state.RdonlyCellsSwitched, state.RdonlyCellsNotSwitched, err = ws.GetCellsWithTableReadsSwitched(ctx, targetKeyspace, table, topodatapb.TabletType_RDONLY)
			if err != nil {
				return nil, nil, err
			}

			state.ReplicaCellsSwitched, state.ReplicaCellsNotSwitched, err = ws.GetCellsWithTableReadsSwitched(ctx, targetKeyspace, table, topodatapb.TabletType_REPLICA)
			if err != nil {
				return nil, nil, err
			}
			globalRules, err := topotools.GetRoutingRules(ctx, ts.TopoServer())
			if err != nil {
				return nil, nil, err
			}
			for _, table := range ts.Tables() {
				rr := globalRules[table]
				// If a rule exists for the table and points to the target keyspace, writes
				// have been switched.
				if len(rr) > 0 && rr[0] == fmt.Sprintf("%s.%s", targetKeyspace, table) {
					state.WritesSwitched = true
					break
				}
			}
		}
	} else {
		state.WorkflowType = workflow.TypeReshard

		// We assume a consistent state, so only choose one shard.
		var shard *topo.ShardInfo
		if reverse {
			shard = ts.TargetShards()[0]
		} else {
			shard = ts.SourceShards()[0]
		}

		state.RdonlyCellsSwitched, state.RdonlyCellsNotSwitched, err = ws.GetCellsWithShardReadsSwitched(ctx, targetKeyspace, shard, topodatapb.TabletType_RDONLY)
		if err != nil {
			return nil, nil, err
		}

		state.ReplicaCellsSwitched, state.ReplicaCellsNotSwitched, err = ws.GetCellsWithShardReadsSwitched(ctx, targetKeyspace, shard, topodatapb.TabletType_REPLICA)
		if err != nil {
			return nil, nil, err
		}

		if !shard.IsPrimaryServing {
			state.WritesSwitched = true
		}
	}

	return ts, state, nil
}

// SwitchReads is a generic way of switching read traffic for a resharding workflow.
func (wr *Wrangler) SwitchReads(ctx context.Context, targetKeyspace, workflowName string, servedTypes []topodatapb.TabletType,
	cells []string, direction workflow.TrafficSwitchDirection, dryRun bool) (*[]string, error) {
	// Consistently handle errors by logging and returning them.
	handleError := func(message string, err error) (*[]string, error) {
		werr := vterrors.Errorf(vtrpcpb.Code_INTERNAL, fmt.Sprintf("%s: %v", message, err))
		wr.Logger().Error(werr)
		return nil, werr
	}

	ts, ws, err := wr.getWorkflowState(ctx, targetKeyspace, workflowName)
	if err != nil {
		return handleError("failed to get the current state of the workflow", err)
	}
	if ts == nil {
		errorMsg := fmt.Sprintf("workflow %s not found in keyspace %s", workflowName, targetKeyspace)
		return handleError("failed to get the current state of the workflow", fmt.Errorf(errorMsg))
	}
	log.Infof("Switching reads: %s.%s tt %+v, cells %+v, workflow state: %+v", targetKeyspace, workflowName, servedTypes, cells, ws)
	var switchReplicas, switchRdonly bool
	for _, servedType := range servedTypes {
		if servedType != topodatapb.TabletType_REPLICA && servedType != topodatapb.TabletType_RDONLY {
			return handleError("invalid tablet type", fmt.Errorf("tablet type must be REPLICA or RDONLY: %v", servedType))
		}
		if !ts.isPartialMigration { // shard level traffic switching is all or nothing
			if direction == workflow.DirectionBackward && servedType == topodatapb.TabletType_REPLICA && len(ws.ReplicaCellsSwitched) == 0 {
				return handleError("invalid request", fmt.Errorf("requesting reversal of read traffic for REPLICAs but REPLICA reads have not been switched"))
			}
			if direction == workflow.DirectionBackward && servedType == topodatapb.TabletType_RDONLY && len(ws.RdonlyCellsSwitched) == 0 {
				return handleError("invalid request", fmt.Errorf("requesting reversal of SwitchReads for RDONLYs but RDONLY reads have not been switched"))
			}
		}
		switch servedType {
		case topodatapb.TabletType_REPLICA:
			switchReplicas = true
		case topodatapb.TabletType_RDONLY:
			switchRdonly = true
		}
	}

	// If there are no rdonly tablets in the cells ask to switch rdonly tablets as well so that routing rules
	// are updated for rdonly as well. Otherwise vitess will not know that the workflow has completed and will
	// incorrectly report that not all reads have been switched. User currently is forced to switch
	// non-existent rdonly tablets.
	if switchReplicas && !switchRdonly {
		var err error
		rdonlyTabletsExist, err := topotools.DoCellsHaveRdonlyTablets(ctx, wr.ts, cells)
		if err != nil {
			return nil, err
		}
		if !rdonlyTabletsExist {
			servedTypes = append(servedTypes, topodatapb.TabletType_RDONLY)
		}
	}

	// If journals exist notify user and fail.
	journalsExist, _, err := ts.checkJournals(ctx)
	if err != nil {
		return handleError(fmt.Sprintf("failed to read journal in the %s keyspace", ts.SourceKeyspaceName()), err)
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
		return handleError("workflow validation failed", err)
	}

	// For reads, locking the source keyspace is sufficient.
	ctx, unlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "SwitchReads")
	if lockErr != nil {
		return handleError(fmt.Sprintf("failed to lock the %s keyspace", ts.SourceKeyspaceName()), lockErr)
	}
	defer unlock(&err)

	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		if ts.isPartialMigration {
			ts.Logger().Infof("Partial migration, skipping switchTableReads as traffic is all or nothing per shard and overridden for reads AND writes in the ShardRoutingRule created when switching writes.")
		} else if err := sw.switchTableReads(ctx, cells, servedTypes, direction); err != nil {
			return handleError("failed to switch read traffic for the tables", err)
		}
		return sw.logs(), nil
	}
	wr.Logger().Infof("About to switchShardReads: %+v, %+v, %+v", cells, servedTypes, direction)
	if err := sw.switchShardReads(ctx, cells, servedTypes, direction); err != nil {
		return handleError("failed to switch read traffic for the shards", err)
	}

	wr.Logger().Infof("switchShardReads Completed: %+v, %+v, %+v", cells, servedTypes, direction)
	if err := wr.ts.ValidateSrvKeyspace(ctx, targetKeyspace, strings.Join(cells, ",")); err != nil {
		err2 := vterrors.Wrapf(err, "After switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			targetKeyspace, strings.Join(cells, ","))
		return handleError("failed to validate SrvKeyspace record", err2)
	}
	return sw.logs(), nil
}

func (wr *Wrangler) areTabletsAvailableToStreamFrom(ctx context.Context, ts *trafficSwitcher, keyspace string, shards []*topo.ShardInfo) error {
	var cells []string
	tabletTypes := ts.optTabletTypes
	if ts.optCells != "" {
		cells = strings.Split(ts.optCells, ",")
	}
	if tabletTypes == "" {
		tabletTypes = "in_order:REPLICA,PRIMARY" // default
	}

	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, shard := range shards {
		wg.Add(1)
		go func(cells []string, keyspace string, shard *topo.ShardInfo) {
			defer wg.Done()
			if cells == nil {
				cells = append(cells, shard.PrimaryAlias.Cell)
			}
			tp, err := discovery.NewTabletPicker(ctx, wr.ts, cells, shard.PrimaryAlias.Cell, keyspace, shard.ShardName(), tabletTypes, discovery.TabletPickerOptions{})
			if err != nil {
				allErrors.RecordError(err)
				return
			}
			tablets := tp.GetMatchingTablets(ctx)
			if len(tablets) == 0 {
				allErrors.RecordError(fmt.Errorf("no tablet found to source data in keyspace %s, shard %s", keyspace, shard.ShardName()))
				return
			}
		}(cells, keyspace, shard)
	}

	wg.Wait()
	if allErrors.HasErrors() {
		log.Errorf("%s", allErrors.Error())
		return allErrors.Error()
	}
	return nil
}

// SwitchWrites is a generic way of migrating write traffic for a resharding workflow.
func (wr *Wrangler) SwitchWrites(ctx context.Context, targetKeyspace, workflowName string, timeout time.Duration,
	cancel, reverse, reverseReplication bool, dryRun, initializeTargetSequences bool) (journalID int64, dryRunResults *[]string, err error) {
	// Consistently handle errors by logging and returning them.
	handleError := func(message string, err error) (int64, *[]string, error) {
		werr := vterrors.Errorf(vtrpcpb.Code_INTERNAL, fmt.Sprintf("%s: %v", message, err))
		wr.Logger().Error(werr)
		return 0, nil, werr
	}

	ts, ws, err := wr.getWorkflowState(ctx, targetKeyspace, workflowName)
	_ = ws
	if err != nil {
		return handleError("failed to get the current workflow state", err)
	}
	if ts == nil {
		errorMsg := fmt.Sprintf("workflow %s not found in keyspace %s", workflowName, targetKeyspace)
		return handleError("failed to get the current workflow state", fmt.Errorf(errorMsg))
	}

	var sw iswitcher
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, wr: wr}
	}

	if ts.frozen {
		ts.Logger().Warningf("Writes have already been switched for workflow %s, nothing to do here", ts.WorkflowName())
		return 0, sw.logs(), nil
	}

	ts.Logger().Infof("Built switching metadata: %+v", ts)
	if err := ts.validate(ctx); err != nil {
		return handleError("workflow validation failed", err)
	}

	if reverseReplication {
		err := wr.areTabletsAvailableToStreamFrom(ctx, ts, ts.TargetKeyspaceName(), ts.TargetShards())
		if err != nil {
			return handleError(fmt.Sprintf("no tablets were available to stream from in the %s keyspace", ts.SourceKeyspaceName()), err)
		}
	}

	// Need to lock both source and target keyspaces.
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "SwitchWrites")
	if lockErr != nil {
		return handleError(fmt.Sprintf("failed to lock the %s keyspace", ts.SourceKeyspaceName()), lockErr)
	}
	ctx = tctx
	defer sourceUnlock(&err)
	if ts.TargetKeyspaceName() != ts.SourceKeyspaceName() {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "SwitchWrites")
		if lockErr != nil {
			return handleError(fmt.Sprintf("failed to lock the %s keyspace", ts.TargetKeyspaceName()), lockErr)
		}
		ctx = tctx
		defer targetUnlock(&err)
	}

	// Find out if the target is using any sequence tables for auto_increment
	// value generation. If so, then we'll need to ensure that they are
	// initialized properly before allowing new writes on the target.
	sequenceMetadata := make(map[string]*sequenceMetadata)
	// For sharded to sharded migrations the sequence must already be setup.
	// For reshards the sequence usage is not changed.
	if initializeTargetSequences && ts.workflowType == binlogdatapb.VReplicationWorkflowType_MoveTables &&
		ts.SourceKeyspaceSchema() != nil && ts.SourceKeyspaceSchema().Keyspace != nil &&
		!ts.SourceKeyspaceSchema().Keyspace.Sharded {
		sequenceMetadata, err = ts.getTargetSequenceMetadata(ctx)
		if err != nil {
			return handleError(fmt.Sprintf("failed to get the sequence information in the %s keyspace", ts.TargetKeyspaceName()), err)
		}
	}

	// If no journals exist, sourceWorkflows will be initialized by sm.MigrateStreams.
	journalsExist, sourceWorkflows, err := ts.checkJournals(ctx)
	if err != nil {
		return handleError(fmt.Sprintf("failed to read journal in the %s keyspace", ts.SourceKeyspaceName()), err)
	}
	if !journalsExist {
		ts.Logger().Infof("No previous journals were found. Proceeding normally.")
		sm, err := workflow.BuildLegacyStreamMigrator(ctx, ts, cancel, wr.env.Parser())
		if err != nil {
			return handleError("failed to migrate the workflow streams", err)
		}
		if cancel {
			sw.cancelMigration(ctx, sm)
			return 0, sw.logs(), nil
		}

		ts.Logger().Infof("Stopping streams")
		sourceWorkflows, err = sw.stopStreams(ctx, sm)
		if err != nil {
			for key, streams := range sm.Streams() {
				for _, stream := range streams {
					ts.Logger().Errorf("stream in stopStreams: key %s shard %s stream %+v", key, stream.BinlogSource.Shard, stream.BinlogSource)
				}
			}
			sw.cancelMigration(ctx, sm)
			return handleError("failed to stop the workflow streams", err)
		}

		ts.Logger().Infof("Stopping source writes")
		if err := sw.stopSourceWrites(ctx); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError(fmt.Sprintf("failed to stop writes in the %s keyspace", ts.SourceKeyspaceName()), err)
		}

		if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
			ts.Logger().Infof("Executing LOCK TABLES on source tables %d times", lockTablesCycles)
			// Doing this twice with a pause in-between to catch any writes that may have raced in between
			// the tablet's deny list check and the first mysqld side table lock.
			for cnt := 1; cnt <= lockTablesCycles; cnt++ {
				if err := ts.executeLockTablesOnSource(ctx); err != nil {
					sw.cancelMigration(ctx, sm)
					return handleError(fmt.Sprintf("failed to execute LOCK TABLES (attempt %d of %d) on sources", cnt, lockTablesCycles), err)
				}
				// No need to UNLOCK the tables as the connection was closed once the locks were acquired
				// and thus the locks released.
				time.Sleep(lockTablesCycleDelay)
			}
		}

		ts.Logger().Infof("Waiting for streams to catchup")
		if err := sw.waitForCatchup(ctx, timeout); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError("failed to sync up replication between the source and target", err)
		}

		ts.Logger().Infof("Migrating streams")
		if err := sw.migrateStreams(ctx, sm); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError("failed to migrate the workflow streams", err)
		}

		ts.Logger().Infof("Resetting sequences")
		if err := sw.resetSequences(ctx); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError("failed to reset the sequences", err)
		}

		ts.Logger().Infof("Creating reverse streams")
		if err := sw.createReverseVReplication(ctx); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError("failed to create the reverse vreplication streams", err)
		}

		// Initialize any target sequences, if there are any, before allowing new writes.
		if initializeTargetSequences && len(sequenceMetadata) > 0 {
			ts.Logger().Infof("Initializing target sequences")
			// Writes are blocked so we can safely initialize the sequence tables but
			// we also want to use a shorter timeout than the parent context.
			// We use at most half of the overall timeout.
			initSeqCtx, cancel := context.WithTimeout(ctx, timeout/2)
			defer cancel()
			if err := sw.initializeTargetSequences(initSeqCtx, sequenceMetadata); err != nil {
				sw.cancelMigration(ctx, sm)
				return handleError(fmt.Sprintf("failed to initialize the sequences used in the %s keyspace", ts.TargetKeyspaceName()), err)
			}
		}
	} else {
		if cancel {
			return handleError("invalid cancel", fmt.Errorf("traffic switching has reached the point of no return, cannot cancel"))
		}
		ts.Logger().Infof("Journals were found. Completing the left over steps.")
		// Need to gather positions in case all journals were not created.
		if err := ts.gatherPositions(ctx); err != nil {
			return handleError("failed to gather replication positions", err)
		}
	}

	// This is the point of no return. Once a journal is created,
	// traffic can be redirected to target shards.
	if err := sw.createJournals(ctx, sourceWorkflows); err != nil {
		return handleError("failed to create the journal", err)
	}
	if err := sw.allowTargetWrites(ctx); err != nil {
		return handleError(fmt.Sprintf("failed to allow writes in the %s keyspace", ts.TargetKeyspaceName()), err)
	}
	if err := sw.changeRouting(ctx); err != nil {
		return handleError("failed to update the routing rules", err)
	}
	if err := sw.streamMigraterfinalize(ctx, ts, sourceWorkflows); err != nil {
		return handleError("failed to finalize the traffic switch", err)
	}
	if reverseReplication {
		if err := sw.startReverseVReplication(ctx); err != nil {
			return handleError("failed to start the reverse workflow", err)
		}
	}

	if err := sw.freezeTargetVReplication(ctx); err != nil {
		return handleError(fmt.Sprintf("failed to freeze the workflow in the %s keyspace", ts.TargetKeyspaceName()), err)
	}

	return ts.id, sw.logs(), nil
}

// DropTargets cleans up target tables, shards and denied tables if a
// MoveTables/Reshard is cancelled.
func (wr *Wrangler) DropTargets(ctx context.Context, targetKeyspace, workflow string, keepData, keepRoutingRules, dryRun bool) (*[]string, error) {
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, err
	}
	ts.keepRoutingRules = keepRoutingRules
	var sw iswitcher
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, wr: wr}
	}
	var tctx context.Context
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "DropTargets")
	if lockErr != nil {
		ts.Logger().Errorf("Source LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer sourceUnlock(&err)
	ctx = tctx
	if ts.TargetKeyspaceName() != ts.SourceKeyspaceName() {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "DropTargets")
		if lockErr != nil {
			ts.Logger().Errorf("Target LockKeyspace failed: %v", lockErr)
			return nil, lockErr
		}
		defer targetUnlock(&err)
		ctx = tctx
	}
	if !keepData {
		switch ts.MigrationType() {
		case binlogdatapb.MigrationType_TABLES:
			log.Infof("Deleting target tables")
			if err := sw.removeTargetTables(ctx); err != nil {
				return nil, err
			}
			if err := sw.dropSourceDeniedTables(ctx); err != nil {
				return nil, err
			}
			if err := sw.dropTargetDeniedTables(ctx); err != nil {
				return nil, err
			}
		case binlogdatapb.MigrationType_SHARDS:
			log.Infof("Removing target shards")
			if err := sw.dropTargetShards(ctx); err != nil {
				return nil, err
			}
		}
	}
	if err := wr.dropArtifacts(ctx, keepRoutingRules, sw); err != nil {
		return nil, err
	}
	if err := ts.TopoServer().RebuildSrvVSchema(ctx, nil); err != nil {
		return nil, err
	}
	return sw.logs(), nil
}

func (wr *Wrangler) dropArtifacts(ctx context.Context, keepRoutingRules bool, sw iswitcher) error {
	if err := sw.dropSourceReverseVReplicationStreams(ctx); err != nil {
		return err
	}
	if err := sw.dropTargetVReplicationStreams(ctx); err != nil {
		return err
	}
	if !keepRoutingRules {
		if err := sw.deleteRoutingRules(ctx); err != nil {
			return err
		}
		if err := sw.deleteShardRoutingRules(ctx); err != nil {
			return err
		}
	}

	return nil
}

// finalizeMigrateWorkflow deletes the streams for the Migrate workflow.
// We only cleanup the target for external sources.
func (wr *Wrangler) finalizeMigrateWorkflow(ctx context.Context, targetKeyspace, workflow, tableSpecs string,
	cancel, keepData, keepRoutingRules, dryRun bool) (*[]string, error) {
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
	tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "completeMigrateWorkflow")
	if lockErr != nil {
		ts.Logger().Errorf("Target LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer targetUnlock(&err)
	ctx = tctx
	if err := sw.dropTargetVReplicationStreams(ctx); err != nil {
		return nil, err
	}
	if !cancel {
		sw.addParticipatingTablesToKeyspace(ctx, targetKeyspace, tableSpecs)
		if err := ts.TopoServer().RebuildSrvVSchema(ctx, nil); err != nil {
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

// DropSources cleans up source tables, shards and denied tables after a
// MoveTables/Reshard is completed.
func (wr *Wrangler) DropSources(ctx context.Context, targetKeyspace, workflowName string, removalType workflow.TableRemovalType, keepData, keepRoutingRules, force, dryRun bool) (*[]string, error) {
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
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "DropSources")
	if lockErr != nil {
		ts.Logger().Errorf("Source LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer sourceUnlock(&err)
	ctx = tctx
	if ts.TargetKeyspaceName() != ts.SourceKeyspaceName() {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "DropSources")
		if lockErr != nil {
			ts.Logger().Errorf("Target LockKeyspace failed: %v", lockErr)
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
		switch ts.MigrationType() {
		case binlogdatapb.MigrationType_TABLES:
			log.Infof("Deleting tables")
			if err := sw.removeSourceTables(ctx, removalType); err != nil {
				return nil, err
			}
			if err := sw.dropSourceDeniedTables(ctx); err != nil {
				return nil, err
			}
			if err := sw.dropTargetDeniedTables(ctx); err != nil {
				return nil, err
			}

		case binlogdatapb.MigrationType_SHARDS:
			log.Infof("Removing shards")
			if err := sw.dropSourceShards(ctx); err != nil {
				return nil, err
			}
		}
	}
	if err := wr.dropArtifacts(ctx, keepRoutingRules, sw); err != nil {
		return nil, err
	}
	if err := ts.TopoServer().RebuildSrvVSchema(ctx, nil); err != nil {
		return nil, err
	}

	return sw.logs(), nil
}

func (wr *Wrangler) getShardSubset(ctx context.Context, keyspace string, shardSubset []string) ([]string, error) {
	if wr.WorkflowParams != nil && len(wr.WorkflowParams.ShardSubset) > 0 {
		shardSubset = wr.WorkflowParams.ShardSubset
	}
	allShards, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return nil, err
	}
	if len(allShards) == 0 {
		return nil, fmt.Errorf("no shards found in keyspace %s", keyspace)
	}

	if len(shardSubset) == 0 {
		return allShards, nil
	}

	existingShards := make(map[string]bool, len(allShards))
	for _, shard := range allShards {
		existingShards[shard] = true
	}
	// Validate that the provided shards are part of the keyspace.
	for _, shard := range shardSubset {
		_, found := existingShards[shard]
		if !found {
			return nil, fmt.Errorf("shard %s not found in keyspace %s", shard, keyspace)
		}
	}
	log.Infof("Selecting subset of shards in keyspace %s: %d from %d :: %+v",
		keyspace, len(shardSubset), len(allShards), shardSubset)
	return shardSubset, nil

}

func (wr *Wrangler) buildTrafficSwitcher(ctx context.Context, targetKeyspace, workflowName string) (*trafficSwitcher, error) {
	shardSubset, err := wr.getShardSubset(ctx, targetKeyspace, nil)
	if err != nil {
		return nil, err
	}
	tgtInfo, err := workflow.LegacyBuildTargets(ctx, wr.ts, wr.tmc, targetKeyspace, workflowName, shardSubset)
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
		workflowType:    tgtInfo.WorkflowType,
		workflowSubType: tgtInfo.WorkflowSubType,
	}
	log.Infof("Migration ID for workflow %s: %d", workflowName, ts.id)
	sourceTopo := wr.ts

	// Build the sources.
	for _, target := range targets {
		for _, bls := range target.Sources {
			if ts.sourceKeyspace == "" {
				ts.sourceKeyspace = bls.Keyspace
				ts.sourceTimeZone = bls.SourceTimeZone
				ts.targetTimeZone = bls.TargetTimeZone
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
			if sourcesi.PrimaryAlias == nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "source shard %s/%s currently has no primary tablet",
					bls.Keyspace, bls.Shard)
			}
			sourcePrimary, err := sourceTopo.GetTablet(ctx, sourcesi.PrimaryAlias)
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
	ts.sourceKSSchema, err = vindexes.BuildKeyspaceSchema(vs, ts.sourceKeyspace, wr.env.Parser())
	if err != nil {
		return nil, err
	}

	sourceShards, targetShards := ts.getSourceAndTargetShardsNames()

	ts.isPartialMigration, err = ts.isPartialMoveTables(sourceShards, targetShards)
	if err != nil {
		return nil, err
	}
	if ts.isPartialMigration {
		log.Infof("Migration is partial, for shards %+v", sourceShards)
	}
	return ts, nil
}

func (ts *trafficSwitcher) getSourceAndTargetShardsNames() ([]string, []string) {
	var sourceShards, targetShards []string
	for _, si := range ts.SourceShards() {
		sourceShards = append(sourceShards, si.ShardName())
	}
	for _, si := range ts.TargetShards() {
		targetShards = append(targetShards, si.ShardName())
	}
	return sourceShards, targetShards
}

// isPartialMoveTables returns true if whe workflow is MoveTables, has the
// same number of shards, is not covering the entire shard range, and has
// one-to-one shards in source and target.
func (ts *trafficSwitcher) isPartialMoveTables(sourceShards, targetShards []string) (bool, error) {

	if ts.MigrationType() != binlogdatapb.MigrationType_TABLES {
		return false, nil
	}

	skr, tkr, err := getSourceAndTargetKeyRanges(sourceShards, targetShards)
	if err != nil {
		return false, err
	}

	if key.KeyRangeIsComplete(skr) || key.KeyRangeIsComplete(tkr) || len(sourceShards) != len(targetShards) {
		return false, nil
	}

	return key.KeyRangeEqual(skr, tkr), nil
}

func getSourceAndTargetKeyRanges(sourceShards, targetShards []string) (*topodatapb.KeyRange, *topodatapb.KeyRange, error) {
	if len(sourceShards) == 0 || len(targetShards) == 0 {
		return nil, nil, fmt.Errorf("either source or target shards are missing")
	}

	getKeyRange := func(shard string) (*topodatapb.KeyRange, error) {
		krs, err := key.ParseShardingSpec(shard)
		if err != nil {
			return nil, err
		}
		return krs[0], nil
	}

	// Happily string sorting of shards also sorts them in the ascending order of
	// key ranges in vitess.
	sort.Strings(sourceShards)
	sort.Strings(targetShards)
	getFullKeyRange := func(shards []string) (*topodatapb.KeyRange, error) {
		// Expect sorted shards.
		kr1, err := getKeyRange(sourceShards[0])
		if err != nil {
			return nil, err
		}
		kr2, err := getKeyRange(sourceShards[len(sourceShards)-1])
		if err != nil {
			return nil, err
		}
		return &topodatapb.KeyRange{
			Start: kr1.Start,
			End:   kr2.End,
		}, nil
	}

	skr, err := getFullKeyRange(sourceShards)
	if err != nil {
		return nil, nil, err
	}
	tkr, err := getFullKeyRange(targetShards)
	if err != nil {
		return nil, nil, err
	}

	return skr, tkr, nil
}

func (ts *trafficSwitcher) validate(ctx context.Context) error {
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		if ts.isPartialMigration {
			return nil
		}
		sourceTopo := ts.wr.ts
		if ts.externalTopo != nil {
			sourceTopo = ts.externalTopo
		}

		// All shards must be present.
		if err := workflow.CompareShards(ctx, ts.SourceKeyspaceName(), ts.SourceShards(), sourceTopo); err != nil {
			return err
		}
		if err := workflow.CompareShards(ctx, ts.TargetKeyspaceName(), ts.TargetShards(), ts.wr.ts); err != nil {
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

func (ts *trafficSwitcher) switchTableReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction workflow.TrafficSwitchDirection) error {
	log.Infof("switchTableReads: servedTypes: %+v, direction %t", servedTypes, direction)
	rules, err := topotools.GetRoutingRules(ctx, ts.TopoServer())
	if err != nil {
		return err
	}
	// We assume that the following rules were setup when the targets were created:
	// table -> sourceKeyspace.table
	// targetKeyspace.table -> sourceKeyspace.table
	// For forward migration, we add tablet type specific rules to redirect traffic to the target.
	// For backward, we redirect to source.
	for _, servedType := range servedTypes {
		tt := strings.ToLower(servedType.String())
		for _, table := range ts.Tables() {
			if direction == workflow.DirectionForward {
				log.Infof("Route direction forward")
				toTarget := []string{ts.TargetKeyspaceName() + "." + table}
				rules[table+"@"+tt] = toTarget
				rules[ts.TargetKeyspaceName()+"."+table+"@"+tt] = toTarget
				rules[ts.SourceKeyspaceName()+"."+table+"@"+tt] = toTarget
			} else {
				log.Infof("Route direction backwards")
				toSource := []string{ts.SourceKeyspaceName() + "." + table}
				rules[table+"@"+tt] = toSource
				rules[ts.TargetKeyspaceName()+"."+table+"@"+tt] = toSource
				rules[ts.SourceKeyspaceName()+"."+table+"@"+tt] = toSource
			}
		}
	}
	if err := topotools.SaveRoutingRules(ctx, ts.TopoServer(), rules); err != nil {
		return err
	}
	return ts.TopoServer().RebuildSrvVSchema(ctx, cells)
}

func (ts *trafficSwitcher) switchShardReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction workflow.TrafficSwitchDirection) error {
	var fromShards, toShards []*topo.ShardInfo
	if direction == workflow.DirectionForward {
		fromShards, toShards = ts.SourceShards(), ts.TargetShards()
	} else {
		fromShards, toShards = ts.TargetShards(), ts.SourceShards()
	}
	if err := ts.TopoServer().ValidateSrvKeyspace(ctx, ts.TargetKeyspaceName(), strings.Join(cells, ",")); err != nil {
		err2 := vterrors.Wrapf(err, "Before switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			ts.TargetKeyspaceName(), strings.Join(cells, ","))
		log.Errorf("%w", err2)
		return err2
	}
	for _, servedType := range servedTypes {
		if err := ts.wr.updateShardRecords(ctx, ts.SourceKeyspaceName(), fromShards, cells, servedType, true /* isFrom */, false /* clearSourceShards */); err != nil {
			return err
		}
		if err := ts.wr.updateShardRecords(ctx, ts.SourceKeyspaceName(), toShards, cells, servedType, false, false); err != nil {
			return err
		}
		err := ts.TopoServer().MigrateServedType(ctx, ts.SourceKeyspaceName(), toShards, fromShards, servedType, cells)
		if err != nil {
			return err
		}
	}
	if err := ts.TopoServer().ValidateSrvKeyspace(ctx, ts.TargetKeyspaceName(), strings.Join(cells, ",")); err != nil {
		err2 := vterrors.Wrapf(err, "After switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			ts.TargetKeyspaceName(), strings.Join(cells, ","))
		log.Errorf("%w", err2)
		return err2
	}
	return nil
}

// checkJournals returns true if at least one journal has been created.
// If so, it also returns the list of sourceWorkflows that need to be switched.
func (ts *trafficSwitcher) checkJournals(ctx context.Context) (journalsExist bool, sourceWorkflows []string, err error) {
	var (
		ws = workflow.NewServer(ts.wr.env, ts.TopoServer(), ts.TabletManagerClient())
		mu sync.Mutex
	)

	err = ts.ForAllSources(func(source *workflow.MigrationSource) error {
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
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		err = ts.changeTableSourceWrites(ctx, disallowWrites)
	} else {
		err = ts.changeShardsAccess(ctx, ts.SourceKeyspaceName(), ts.SourceShards(), disallowWrites)
	}
	if err != nil {
		log.Warningf("Error: %s", err)
		return err
	}
	return ts.ForAllSources(func(source *workflow.MigrationSource) error {
		var err error
		source.Position, err = ts.TabletManagerClient().PrimaryPosition(ctx, source.GetPrimary().Tablet)
		ts.wr.Logger().Infof("Stopped Source Writes. Position for source %v:%v: %v",
			ts.SourceKeyspaceName(), source.GetShard().ShardName(), source.Position)
		if err != nil {
			log.Warningf("Error: %s", err)
		}
		return err
	})
}

func (ts *trafficSwitcher) changeTableSourceWrites(ctx context.Context, access accessType) error {
	err := ts.ForAllSources(func(source *workflow.MigrationSource) error {
		if _, err := ts.TopoServer().UpdateShardFields(ctx, ts.SourceKeyspaceName(), source.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateDeniedTables(ctx, topodatapb.TabletType_PRIMARY, nil, access == allowWrites /* remove */, ts.Tables())
		}); err != nil {
			return err
		}
		rtbsCtx, cancel := context.WithTimeout(ctx, shardTabletRefreshTimeout)
		defer cancel()
		isPartial, partialDetails, err := topotools.RefreshTabletsByShard(rtbsCtx, ts.TopoServer(), ts.TabletManagerClient(), source.GetShard(), nil, ts.Logger())
		if isPartial {
			err = fmt.Errorf("failed to successfully refresh all tablets in the %s/%s source shard (%v):\n  %v",
				source.GetShard().Keyspace(), source.GetShard().ShardName(), err, partialDetails)
		}
		return err
	})
	if err != nil {
		log.Warningf("Error in changeTableSourceWrites: %s", err)
		return err
	}
	// Note that the denied tables, which are being updated in this method, are not part of the SrvVSchema in the topo.
	// However, we are using the notification of a SrvVSchema change in VTGate to recompute the state of a
	// MoveTables workflow (which also looks up denied tables from the topo). So we need to trigger a SrvVSchema change here.
	return ts.TopoServer().RebuildSrvVSchema(ctx, nil)
}

// executeLockTablesOnSource executes a LOCK TABLES tb1 READ, tbl2 READ,... statement on each
// source shard's primary tablet using a non-pooled connection as the DBA user. The connection
// is closed when the LOCK TABLES statement returns, so we immediately release the LOCKs.
func (ts *trafficSwitcher) executeLockTablesOnSource(ctx context.Context) error {
	ts.Logger().Infof("Locking (and then immediately unlocking) the following tables on source keyspace %v: %v", ts.SourceKeyspaceName(), ts.Tables())
	if len(ts.Tables()) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no tables found in the source keyspace %v associated with the %s workflow", ts.SourceKeyspaceName(), ts.WorkflowName())
	}

	sb := strings.Builder{}
	sb.WriteString("LOCK TABLES ")
	for _, tableName := range ts.Tables() {
		sb.WriteString(fmt.Sprintf("%s READ,", sqlescape.EscapeID(tableName)))
	}
	// Trim extra trailing comma.
	lockStmt := sb.String()[:sb.Len()-1]

	return ts.ForAllSources(func(source *workflow.MigrationSource) error {
		primary := source.GetPrimary()
		if primary == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no primary found for source shard %s", source.GetShard())
		}
		tablet := primary.Tablet
		_, err := ts.wr.ExecuteFetchAsDba(ctx, tablet.Alias, lockStmt, 1, false, true)
		if err != nil {
			ts.Logger().Errorf("Error executing %s on source tablet %v: %v", lockStmt, tablet, err)
			return err
		}
		return err
	})
}

func (ts *trafficSwitcher) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()
	// Source writes have been stopped, wait for all streams on targets to catch up.
	if err := ts.ForAllUIDs(func(target *workflow.MigrationTarget, uid int32) error {
		ts.Logger().Infof("Before Catchup: uid: %d, target primary %s, target position %s, shard %s", uid,
			target.GetPrimary().AliasString(), target.Position, target.GetShard().String())
		bls := target.Sources[uid]
		source := ts.Sources()[bls.Shard]
		ts.Logger().Infof("Before Catchup: waiting for keyspace:shard: %v:%v to reach source position %v, uid %d",
			ts.TargetKeyspaceName(), target.GetShard().ShardName(), source.Position, uid)
		if err := ts.TabletManagerClient().VReplicationWaitForPos(ctx, target.GetPrimary().Tablet, uid, source.Position); err != nil {
			return err
		}
		log.Infof("After catchup: target keyspace:shard: %v:%v, source position %v, uid %d",
			ts.TargetKeyspaceName(), target.GetShard().ShardName(), source.Position, uid)
		ts.Logger().Infof("After catchup: position for keyspace:shard: %v:%v reached, uid %d",
			ts.TargetKeyspaceName(), target.GetShard().ShardName(), uid)
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, target.GetPrimary().Tablet, binlogplayer.StopVReplication(uid, "stopped for cutover")); err != nil {
			log.Infof("error marking stopped for cutover on %s, uid %d", target.GetPrimary().AliasString(), uid)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	// all targets have caught up, record their positions for setting up reverse workflows
	return ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		var err error
		target.Position, err = ts.TabletManagerClient().PrimaryPosition(ctx, target.GetPrimary().Tablet)
		ts.Logger().Infof("After catchup, position for target primary %s, %v", target.GetPrimary().AliasString(), target.Position)
		return err
	})
}

func (ts *trafficSwitcher) cancelMigration(ctx context.Context, sm *workflow.StreamMigrator) {
	var err error
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		err = ts.changeTableSourceWrites(ctx, allowWrites)
	} else {
		err = ts.changeShardsAccess(ctx, ts.SourceKeyspaceName(), ts.SourceShards(), allowWrites)
	}
	if err != nil {
		ts.Logger().Errorf("Cancel migration failed:", err)
	}

	sm.CancelStreamMigrations(ctx)

	err = ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s", encodeString(target.GetPrimary().DbName()), encodeString(ts.WorkflowName()))
		_, err := ts.TabletManagerClient().VReplicationExec(ctx, target.GetPrimary().Tablet, query)
		return err
	})
	if err != nil {
		ts.Logger().Errorf("Cancel migration failed: could not restart vreplication: %v", err)
	}

	err = ts.deleteReverseVReplication(ctx)
	if err != nil {
		ts.Logger().Errorf("Cancel migration failed: could not delete revers vreplication entries: %v", err)
	}
}

func (ts *trafficSwitcher) gatherPositions(ctx context.Context) error {
	err := ts.ForAllSources(func(source *workflow.MigrationSource) error {
		var err error
		source.Position, err = ts.TabletManagerClient().PrimaryPosition(ctx, source.GetPrimary().Tablet)
		ts.Logger().Infof("Position for source %v:%v: %v", ts.SourceKeyspaceName(), source.GetShard().ShardName(), source.Position)
		return err
	})
	if err != nil {
		return err
	}
	return ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		var err error
		target.Position, err = ts.TabletManagerClient().PrimaryPosition(ctx, target.GetPrimary().Tablet)
		ts.Logger().Infof("Position for target %v:%v: %v", ts.TargetKeyspaceName(), target.GetShard().ShardName(), target.Position)
		return err
	})
}

func (ts *trafficSwitcher) createReverseVReplication(ctx context.Context) error {
	if err := ts.deleteReverseVReplication(ctx); err != nil {
		return err
	}
	err := ts.ForAllUIDs(func(target *workflow.MigrationTarget, uid int32) error {
		bls := target.Sources[uid]
		source := ts.Sources()[bls.Shard]
		reverseBls := &binlogdatapb.BinlogSource{
			Keyspace:       ts.TargetKeyspaceName(),
			Shard:          target.GetShard().ShardName(),
			TabletType:     bls.TabletType,
			Filter:         &binlogdatapb.Filter{},
			OnDdl:          bls.OnDdl,
			SourceTimeZone: bls.TargetTimeZone,
			TargetTimeZone: bls.SourceTimeZone,
		}

		for _, rule := range bls.Filter.Rules {
			if rule.Filter == "exclude" {
				reverseBls.Filter.Rules = append(reverseBls.Filter.Rules, rule)
				continue
			}
			var filter string
			if strings.HasPrefix(rule.Match, "/") {
				if ts.SourceKeyspaceSchema().Keyspace.Sharded {
					filter = key.KeyRangeString(source.GetShard().KeyRange)
				}
			} else {
				var inKeyrange string
				if ts.SourceKeyspaceSchema().Keyspace.Sharded {
					vtable, ok := ts.SourceKeyspaceSchema().Tables[rule.Match]
					if !ok {
						return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table %s not found in vschema", rule.Match)
					}
					// We currently assume the primary vindex is the best way to filter rows
					// for the table, which may not always be true.
					// TODO: handle more of these edge cases explicitly, e.g. sequence tables.
					switch vtable.Type {
					case vindexes.TypeReference:
						// For reference tables there are no vindexes and thus no filter to apply.
					default:
						// For non-reference tables we return an error if there's no primary
						// vindex as it's not clear what to do.
						if len(vtable.ColumnVindexes) > 0 && len(vtable.ColumnVindexes[0].Columns) > 0 {
							inKeyrange = fmt.Sprintf(" where in_keyrange(%s, '%s.%s', '%s')", sqlparser.String(vtable.ColumnVindexes[0].Columns[0]),
								ts.SourceKeyspaceName(), vtable.ColumnVindexes[0].Name, key.KeyRangeString(source.GetShard().KeyRange))
						} else {
							return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no primary vindex found for the %s table in the %s keyspace",
								vtable.Name.String(), ts.SourceKeyspaceName())
						}
					}
				}
				filter = fmt.Sprintf("select * from %s%s", sqlescape.EscapeID(rule.Match), inKeyrange)
			}
			reverseBls.Filter.Rules = append(reverseBls.Filter.Rules, &binlogdatapb.Rule{
				Match:  rule.Match,
				Filter: filter,
			})
		}
		log.Infof("Creating reverse workflow vreplication stream on tablet %s: workflow %s, startPos %s for target %s:%s, uid %d",
			source.GetPrimary().Alias, ts.ReverseWorkflowName(), target.Position, ts.TargetKeyspaceName(), target.GetShard().ShardName(), uid)
		_, err := ts.VReplicationExec(ctx, source.GetPrimary().Alias,
			binlogplayer.CreateVReplicationState(ts.ReverseWorkflowName(), reverseBls, target.Position,
				binlogdatapb.VReplicationWorkflowState_Stopped, source.GetPrimary().DbName(), ts.workflowType, ts.workflowSubType))
		if err != nil {
			return err
		}

		// if user has defined the cell/tablet_types parameters in the forward workflow, update the reverse workflow as well
		updateQuery := ts.getReverseVReplicationUpdateQuery(target.GetPrimary().Alias.Cell, source.GetPrimary().Alias.Cell, source.GetPrimary().DbName())
		if updateQuery != "" {
			log.Infof("Updating vreplication stream entry on %s with: %s", source.GetPrimary().Alias, updateQuery)
			_, err = ts.VReplicationExec(ctx, source.GetPrimary().Alias, updateQuery)
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
			ts.optCells, ts.optTabletTypes, ts.ReverseWorkflowName(), dbname)
		return query
	}
	return ""
}

func (ts *trafficSwitcher) deleteReverseVReplication(ctx context.Context) error {
	return ts.ForAllSources(func(source *workflow.MigrationSource) error {
		query := fmt.Sprintf(sqlDeleteWorkflow, encodeString(source.GetPrimary().DbName()), encodeString(ts.reverseWorkflow))
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, source.GetPrimary().Tablet, query); err != nil {
			return err
		}
		ts.wr.deleteWorkflowVDiffData(ctx, source.GetPrimary().Tablet, ts.reverseWorkflow)
		ts.wr.optimizeCopyStateTable(source.GetPrimary().Tablet)
		return nil
	})
}

func (ts *trafficSwitcher) createJournals(ctx context.Context, sourceWorkflows []string) error {
	log.Infof("In createJournals for source workflows %+v", sourceWorkflows)
	return ts.ForAllSources(func(source *workflow.MigrationSource) error {
		if source.Journaled {
			return nil
		}
		participants := make([]*binlogdatapb.KeyspaceShard, 0)
		participantMap := make(map[string]bool)
		journal := &binlogdatapb.Journal{
			Id:              ts.id,
			MigrationType:   ts.MigrationType(),
			Tables:          ts.Tables(),
			LocalPosition:   source.Position,
			Participants:    participants,
			SourceWorkflows: sourceWorkflows,
		}
		for targetShard, target := range ts.Targets() {
			for _, tsource := range target.Sources {
				participantMap[tsource.Shard] = true
			}
			journal.ShardGtids = append(journal.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: ts.TargetKeyspaceName(),
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
		ts.Logger().Infof("Creating journal: %v", journal)
		statement := fmt.Sprintf("insert into _vt.resharding_journal "+
			"(id, db_name, val) "+
			"values (%v, %v, %v)",
			ts.id, encodeString(source.GetPrimary().DbName()), encodeString(journal.String()))
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, source.GetPrimary().Tablet, statement); err != nil {
			return err
		}
		return nil
	})
}

func (ts *trafficSwitcher) allowTargetWrites(ctx context.Context) error {
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		return ts.allowTableTargetWrites(ctx)
	}
	return ts.changeShardsAccess(ctx, ts.TargetKeyspaceName(), ts.TargetShards(), allowWrites)
}

func (ts *trafficSwitcher) allowTableTargetWrites(ctx context.Context) error {
	return ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		if _, err := ts.TopoServer().UpdateShardFields(ctx, ts.TargetKeyspaceName(), target.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateDeniedTables(ctx, topodatapb.TabletType_PRIMARY, nil, true, ts.Tables())
		}); err != nil {
			return err
		}
		rtbsCtx, cancel := context.WithTimeout(ctx, shardTabletRefreshTimeout)
		defer cancel()
		_, _, err := topotools.RefreshTabletsByShard(rtbsCtx, ts.TopoServer(), ts.TabletManagerClient(), target.GetShard(), nil, ts.Logger())
		return err
	})
}

func (ts *trafficSwitcher) changeRouting(ctx context.Context) error {
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		return ts.changeWriteRoute(ctx)
	}
	return ts.changeShardRouting(ctx)
}

func (ts *trafficSwitcher) changeWriteRoute(ctx context.Context) error {
	if ts.isPartialMigration {
		srr, err := topotools.GetShardRoutingRules(ctx, ts.TopoServer())
		if err != nil {
			return err
		}
		for _, si := range ts.SourceShards() {
			delete(srr, fmt.Sprintf("%s.%s", ts.TargetKeyspaceName(), si.ShardName()))
			ts.Logger().Infof("Deleted shard routing: %v:%v", ts.TargetKeyspaceName(), si.ShardName())
			srr[fmt.Sprintf("%s.%s", ts.SourceKeyspaceName(), si.ShardName())] = ts.TargetKeyspaceName()
			ts.Logger().Infof("Added shard routing: %v:%v", ts.SourceKeyspaceName(), si.ShardName())
		}
		if err := topotools.SaveShardRoutingRules(ctx, ts.TopoServer(), srr); err != nil {
			return err
		}
	} else {
		rules, err := topotools.GetRoutingRules(ctx, ts.TopoServer())
		if err != nil {
			return err
		}
		for _, table := range ts.Tables() {
			targetKsTable := fmt.Sprintf("%s.%s", ts.TargetKeyspaceName(), table)
			sourceKsTable := fmt.Sprintf("%s.%s", ts.SourceKeyspaceName(), table)
			delete(rules, targetKsTable)
			ts.Logger().Infof("Deleted routing: %s", targetKsTable)
			rules[table] = []string{targetKsTable}
			rules[sourceKsTable] = []string{targetKsTable}
			ts.Logger().Infof("Added routing: %v %v", table, sourceKsTable)
		}
		if err := topotools.SaveRoutingRules(ctx, ts.TopoServer(), rules); err != nil {
			return err
		}
	}
	return ts.TopoServer().RebuildSrvVSchema(ctx, nil)
}

func (ts *trafficSwitcher) changeShardRouting(ctx context.Context) error {
	if err := ts.TopoServer().ValidateSrvKeyspace(ctx, ts.TargetKeyspaceName(), ""); err != nil {
		err2 := vterrors.Wrapf(err, "Before changing shard routes, found SrvKeyspace for %s is corrupt", ts.TargetKeyspaceName())
		log.Errorf("%w", err2)
		return err2
	}
	err := ts.ForAllSources(func(source *workflow.MigrationSource) error {
		_, err := ts.TopoServer().UpdateShardFields(ctx, ts.SourceKeyspaceName(), source.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			si.IsPrimaryServing = false
			return nil
		})
		return err
	})
	if err != nil {
		return err
	}
	err = ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		_, err := ts.TopoServer().UpdateShardFields(ctx, ts.TargetKeyspaceName(), target.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			si.IsPrimaryServing = true
			return nil
		})
		return err
	})
	if err != nil {
		return err
	}
	err = ts.TopoServer().MigrateServedType(ctx, ts.TargetKeyspaceName(), ts.TargetShards(), ts.SourceShards(), topodatapb.TabletType_PRIMARY, nil)
	if err != nil {
		return err
	}
	if err := ts.TopoServer().ValidateSrvKeyspace(ctx, ts.TargetKeyspaceName(), ""); err != nil {
		err2 := vterrors.Wrapf(err, "After changing shard routes, found SrvKeyspace for %s is corrupt", ts.TargetKeyspaceName())
		log.Errorf("%w", err2)
		return err2
	}
	return nil
}

func (ts *trafficSwitcher) deleteShardRoutingRules(ctx context.Context) error {
	if !ts.isPartialMigration {
		return nil
	}
	srr, err := topotools.GetShardRoutingRules(ctx, ts.TopoServer())
	if err != nil {
		return err
	}
	for _, si := range ts.TargetShards() {
		delete(srr, fmt.Sprintf("%s.%s", ts.targetKeyspace, si.ShardName()))
	}
	if err := topotools.SaveShardRoutingRules(ctx, ts.TopoServer(), srr); err != nil {
		return err
	}
	return nil
}

func (ts *trafficSwitcher) startReverseVReplication(ctx context.Context) error {
	return ts.ForAllSources(func(source *workflow.MigrationSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s",
			encodeString(source.GetPrimary().DbName()), encodeString(ts.ReverseWorkflowName()))
		_, err := ts.VReplicationExec(ctx, source.GetPrimary().Alias, query)
		return err
	})
}

func (ts *trafficSwitcher) changeShardsAccess(ctx context.Context, keyspace string, shards []*topo.ShardInfo, access accessType) error {
	if err := ts.TopoServer().UpdateDisableQueryService(ctx, keyspace, shards, topodatapb.TabletType_PRIMARY, nil, access == disallowWrites /* disable */); err != nil {
		return err
	}
	return ts.wr.refreshPrimaryTablets(ctx, shards)
}

func (ts *trafficSwitcher) SourceShards() []*topo.ShardInfo {
	shards := make([]*topo.ShardInfo, 0, len(ts.Sources()))
	for _, source := range ts.Sources() {
		shards = append(shards, source.GetShard())
	}
	return shards
}

func (ts *trafficSwitcher) TargetShards() []*topo.ShardInfo {
	shards := make([]*topo.ShardInfo, 0, len(ts.Targets()))
	for _, target := range ts.Targets() {
		shards = append(shards, target.GetShard())
	}
	return shards
}

func (ts *trafficSwitcher) dropSourceDeniedTables(ctx context.Context) error {
	return ts.ForAllSources(func(source *workflow.MigrationSource) error {
		if _, err := ts.TopoServer().UpdateShardFields(ctx, ts.SourceKeyspaceName(), source.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateDeniedTables(ctx, topodatapb.TabletType_PRIMARY, nil, true, ts.Tables())
		}); err != nil {
			return err
		}
		rtbsCtx, cancel := context.WithTimeout(ctx, shardTabletRefreshTimeout)
		defer cancel()
		_, _, err := topotools.RefreshTabletsByShard(rtbsCtx, ts.TopoServer(), ts.TabletManagerClient(), source.GetShard(), nil, ts.Logger())
		return err
	})
}

func (ts *trafficSwitcher) dropTargetDeniedTables(ctx context.Context) error {
	return ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		if _, err := ts.TopoServer().UpdateShardFields(ctx, ts.TargetKeyspaceName(), target.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateDeniedTables(ctx, topodatapb.TabletType_PRIMARY, nil, true, ts.Tables())
		}); err != nil {
			return err
		}
		rtbsCtx, cancel := context.WithTimeout(ctx, shardTabletRefreshTimeout)
		defer cancel()
		_, _, err := topotools.RefreshTabletsByShard(rtbsCtx, ts.TopoServer(), ts.TabletManagerClient(), target.GetShard(), nil, ts.Logger())
		return err
	})
}

func (ts *trafficSwitcher) validateWorkflowHasCompleted(ctx context.Context) error {
	return doValidateWorkflowHasCompleted(ctx, ts)
}

func doValidateWorkflowHasCompleted(ctx context.Context, ts *trafficSwitcher) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	if ts.MigrationType() == binlogdatapb.MigrationType_SHARDS {
		_ = ts.ForAllSources(func(source *workflow.MigrationSource) error {
			wg.Add(1)
			if source.GetShard().IsPrimaryServing {
				rec.RecordError(fmt.Errorf(fmt.Sprintf("Shard %s is still serving", source.GetShard().ShardName())))
			}
			wg.Done()
			return nil
		})
	} else {
		_ = ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
			wg.Add(1)
			query := fmt.Sprintf("select 1 from _vt.vreplication where db_name='%s' and workflow='%s' and message!='FROZEN'", target.GetPrimary().DbName(), ts.WorkflowName())
			rs, _ := ts.VReplicationExec(ctx, target.GetPrimary().Alias, query)
			if len(rs.Rows) > 0 {
				rec.RecordError(fmt.Errorf("vreplication streams are not frozen on tablet %d", target.GetPrimary().Alias.Uid))
			}
			wg.Done()
			return nil
		})
	}
	wg.Wait()

	if !ts.keepRoutingRules {
		//check if table is routable
		if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
			rules, err := topotools.GetRoutingRules(ctx, ts.TopoServer())
			if err != nil {
				rec.RecordError(fmt.Errorf("could not get RoutingRules"))
			}
			for fromTable, toTables := range rules {
				for _, toTable := range toTables {
					for _, table := range ts.Tables() {
						if toTable == fmt.Sprintf("%s.%s", ts.SourceKeyspaceName(), table) {
							rec.RecordError(fmt.Errorf("routing still exists from keyspace %s table %s to %s", ts.SourceKeyspaceName(), table, fromTable))
						}
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
	err := ts.ForAllSources(func(source *workflow.MigrationSource) error {
		for _, tableName := range ts.Tables() {
			primaryDbName, err := sqlescape.EnsureEscaped(source.GetPrimary().DbName())
			if err != nil {
				return err
			}
			tableNameEscaped, err := sqlescape.EnsureEscaped(tableName)
			if err != nil {
				return err
			}
			query := fmt.Sprintf("drop table %s.%s", primaryDbName, tableNameEscaped)
			if removalType == workflow.DropTable {
				ts.Logger().Infof("%s: Dropping table %s.%s\n",
					source.GetPrimary().String(), source.GetPrimary().DbName(), tableName)
			} else {
				renameName, err := sqlescape.EnsureEscaped(getRenameFileName(tableName))
				if err != nil {
					return err
				}
				ts.Logger().Infof("%s: Renaming table %s.%s to %s.%s\n",
					source.GetPrimary().String(), source.GetPrimary().DbName(), tableName, source.GetPrimary().DbName(), renameName)
				query = fmt.Sprintf("rename table %s.%s TO %s.%s", primaryDbName, tableNameEscaped, primaryDbName, renameName)
			}
			_, err = ts.wr.tmc.ExecuteFetchAsDba(ctx, source.GetPrimary().Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
				Query:                   []byte(query),
				MaxRows:                 1,
				ReloadSchema:            true,
				DisableForeignKeyChecks: true,
			})
			if err != nil {
				ts.Logger().Errorf("%s: Error removing table %s: %v", source.GetPrimary().String(), tableName, err)
				return err
			}
			ts.Logger().Infof("%s: Removed table %s.%s\n", source.GetPrimary().String(), source.GetPrimary().DbName(), tableName)

		}
		return nil
	})
	if err != nil {
		return err
	}

	return ts.dropParticipatingTablesFromKeyspace(ctx, ts.SourceKeyspaceName())
}

func (ts *trafficSwitcher) dropParticipatingTablesFromKeyspace(ctx context.Context, keyspace string) error {
	vschema, err := ts.TopoServer().GetVSchema(ctx, keyspace)
	if err != nil {
		return err
	}
	// VReplication does NOT create the vschema entries in SHARDED
	// TARGET keyspaces -- as we cannot know the proper vindex
	// definitions to use -- and we should not delete them either
	// (on workflow Cancel) as the user must create them separately
	// and they contain information about the vindex definitions, etc.
	if vschema.Sharded && keyspace == ts.TargetKeyspaceName() {
		return nil
	}
	for _, tableName := range ts.Tables() {
		delete(vschema.Tables, tableName)
	}
	return ts.TopoServer().SaveVSchema(ctx, keyspace, vschema)
}

// FIXME: even after dropSourceShards there are still entries in the topo, need to research and fix
func (ts *trafficSwitcher) dropSourceShards(ctx context.Context) error {
	return ts.ForAllSources(func(source *workflow.MigrationSource) error {
		ts.Logger().Infof("Deleting shard %s.%s\n", source.GetShard().Keyspace(), source.GetShard().ShardName())
		err := ts.wr.DeleteShard(ctx, source.GetShard().Keyspace(), source.GetShard().ShardName(), true, false)
		if err != nil {
			ts.Logger().Errorf("Error deleting shard %s: %v", source.GetShard().ShardName(), err)
			return err
		}
		ts.Logger().Infof("Deleted shard %s.%s\n", source.GetShard().Keyspace(), source.GetShard().ShardName())
		return nil
	})
}

func (ts *trafficSwitcher) freezeTargetVReplication(ctx context.Context) error {
	// Mark target streams as frozen before deleting. If SwitchWrites gets
	// re-invoked after a freeze, it will skip all the previous steps
	err := ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		ts.Logger().Infof("Marking target streams frozen for workflow %s db_name %s", ts.WorkflowName(), target.GetPrimary().DbName())
		query := fmt.Sprintf("update _vt.vreplication set message = '%s' where db_name=%s and workflow=%s", workflow.Frozen, encodeString(target.GetPrimary().DbName()), encodeString(ts.WorkflowName()))
		_, err := ts.TabletManagerClient().VReplicationExec(ctx, target.GetPrimary().Tablet, query)
		return err
	})
	if err != nil {
		return err
	}
	return nil
}

func (ts *trafficSwitcher) dropTargetVReplicationStreams(ctx context.Context) error {
	return ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		ts.Logger().Infof("Deleting target streams and related data for workflow %s db_name %s", ts.WorkflowName(), target.GetPrimary().DbName())
		query := fmt.Sprintf(sqlDeleteWorkflow, encodeString(target.GetPrimary().DbName()), encodeString(ts.WorkflowName()))
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, target.GetPrimary().Tablet, query); err != nil {
			return err
		}
		ts.wr.deleteWorkflowVDiffData(ctx, target.GetPrimary().Tablet, ts.WorkflowName())
		ts.wr.optimizeCopyStateTable(target.GetPrimary().Tablet)
		return nil
	})
}

func (ts *trafficSwitcher) dropSourceReverseVReplicationStreams(ctx context.Context) error {
	return ts.ForAllSources(func(source *workflow.MigrationSource) error {
		ts.Logger().Infof("Deleting reverse streams and related data for workflow %s db_name %s", ts.WorkflowName(), source.GetPrimary().DbName())
		query := fmt.Sprintf(sqlDeleteWorkflow, encodeString(source.GetPrimary().DbName()), encodeString(workflow.ReverseWorkflowName(ts.WorkflowName())))
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, source.GetPrimary().Tablet, query); err != nil {
			return err
		}
		ts.wr.deleteWorkflowVDiffData(ctx, source.GetPrimary().Tablet, workflow.ReverseWorkflowName(ts.WorkflowName()))
		ts.wr.optimizeCopyStateTable(source.GetPrimary().Tablet)
		return nil
	})
}

func (ts *trafficSwitcher) removeTargetTables(ctx context.Context) error {
	log.Infof("removeTargetTables")
	err := ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		for _, tableName := range ts.Tables() {
			primaryDbName, err := sqlescape.EnsureEscaped(target.GetPrimary().DbName())
			if err != nil {
				return err
			}
			tableName, err := sqlescape.EnsureEscaped(tableName)
			if err != nil {
				return err
			}
			query := fmt.Sprintf("drop table %s.%s", primaryDbName, tableName)
			ts.Logger().Infof("%s: Dropping table %s.%s\n",
				target.GetPrimary().String(), target.GetPrimary().DbName(), tableName)
			_, err = ts.wr.tmc.ExecuteFetchAsDba(ctx, target.GetPrimary().Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
				Query:                   []byte(query),
				MaxRows:                 1,
				ReloadSchema:            true,
				DisableForeignKeyChecks: true,
			})
			if err != nil {
				ts.Logger().Errorf("%s: Error removing table %s: %v",
					target.GetPrimary().String(), tableName, err)
				return err
			}
			ts.Logger().Infof("%s: Removed table %s.%s\n",
				target.GetPrimary().String(), target.GetPrimary().DbName(), tableName)

		}
		return nil
	})
	if err != nil {
		return err
	}

	return ts.dropParticipatingTablesFromKeyspace(ctx, ts.TargetKeyspaceName())
}

func (ts *trafficSwitcher) dropTargetShards(ctx context.Context) error {
	return ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
		ts.Logger().Infof("Deleting shard %s.%s\n", target.GetShard().Keyspace(), target.GetShard().ShardName())
		err := ts.wr.DeleteShard(ctx, target.GetShard().Keyspace(), target.GetShard().ShardName(), true, false)
		if err != nil {
			ts.Logger().Errorf("Error deleting shard %s: %v", target.GetShard().ShardName(), err)
			return err
		}
		ts.Logger().Infof("Deleted shard %s.%s\n", target.GetShard().Keyspace(), target.GetShard().ShardName())
		return nil
	})
}

func (ts *trafficSwitcher) deleteRoutingRules(ctx context.Context) error {
	rules, err := topotools.GetRoutingRules(ctx, ts.TopoServer())
	if err != nil {
		return err
	}
	for _, table := range ts.Tables() {
		delete(rules, table)
		delete(rules, table+"@replica")
		delete(rules, table+"@rdonly")
		delete(rules, ts.TargetKeyspaceName()+"."+table)
		delete(rules, ts.TargetKeyspaceName()+"."+table+"@replica")
		delete(rules, ts.TargetKeyspaceName()+"."+table+"@rdonly")
		delete(rules, ts.SourceKeyspaceName()+"."+table)
		delete(rules, ts.SourceKeyspaceName()+"."+table+"@replica")
		delete(rules, ts.SourceKeyspaceName()+"."+table+"@rdonly")
	}
	if err := topotools.SaveRoutingRules(ctx, ts.TopoServer(), rules); err != nil {
		return err
	}
	return nil
}

// addParticipatingTablesToKeyspace updates the vschema with the new tables that were created as part of the
// Migrate flow. It is called when the Migrate flow is Completed
func (ts *trafficSwitcher) addParticipatingTablesToKeyspace(ctx context.Context, keyspace, tableSpecs string) error {
	vschema, err := ts.TopoServer().GetVSchema(ctx, keyspace)
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
	return ts.TopoServer().SaveVSchema(ctx, keyspace, vschema)
}

func (ts *trafficSwitcher) isSequenceParticipating(ctx context.Context) (bool, error) {
	vschema, err := ts.TopoServer().GetVSchema(ctx, ts.targetKeyspace)
	if err != nil {
		return false, err
	}
	if vschema == nil || len(vschema.Tables) == 0 {
		return false, nil
	}
	sequenceFound := false
	for _, table := range ts.Tables() {
		vs, ok := vschema.Tables[table]
		if !ok || vs == nil {
			continue
		}
		if vs.Type == vindexes.TypeSequence {
			sequenceFound = true
			break
		}
	}
	return sequenceFound, nil
}

// getTargetSequenceMetadata returns a map of sequence metadata keyed by the
// backing sequence table name. If the target keyspace has no tables
// defined that use sequences for auto_increment generation then a nil
// map will be returned.
func (ts *trafficSwitcher) getTargetSequenceMetadata(ctx context.Context) (map[string]*sequenceMetadata, error) {
	vschema, err := ts.TopoServer().GetVSchema(ctx, ts.targetKeyspace)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get vschema for target keyspace %s: %v",
			ts.targetKeyspace, err)
	}
	if vschema == nil || len(vschema.Tables) == 0 { // Nothing to do
		return nil, nil
	}

	sequencesByBackingTable, backingTablesFound, err := ts.findSequenceUsageInKeyspace(vschema)
	if err != nil {
		return nil, err
	}
	// If all of the sequence tables were defined using qualified table
	// names then we don't need to search for them in other keyspaces.
	if len(sequencesByBackingTable) == 0 || backingTablesFound {
		return sequencesByBackingTable, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Now we need to locate the backing sequence table(s) which will
	// be in another unsharded keyspace.
	smMu := sync.Mutex{}
	tableCount := len(sequencesByBackingTable)
	tablesFound := 0 // Used to short circuit the search
	// Define the function used to search each keyspace.
	searchKeyspace := func(sctx context.Context, done chan struct{}, keyspace string) error {
		kvs, kerr := ts.TopoServer().GetVSchema(sctx, keyspace)
		if kerr != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get vschema for keyspace %s: %v",
				keyspace, kerr)
		}
		if kvs == nil || kvs.Sharded || len(kvs.Tables) == 0 {
			return nil
		}
		for tableName, tableDef := range kvs.Tables {
			select {
			case <-sctx.Done():
				return sctx.Err()
			case <-done: // We've found everything we need in other goroutines
				return nil
			default:
			}
			if complete := func() bool {
				smMu.Lock() // Prevent concurrent access to the map
				defer smMu.Unlock()
				sm := sequencesByBackingTable[tableName]
				if tableDef != nil && tableDef.Type == vindexes.TypeSequence &&
					sm != nil && tableName == sm.backingTableName {
					tablesFound++ // This is also protected by the mutex
					sm.backingTableKeyspace = keyspace
					// Set the default keyspace name. We will later check to
					// see if the tablet we send requests to is using a dbname
					// override and use that if it is.
					sm.backingTableDBName = "vt_" + keyspace
					if tablesFound == tableCount { // Short circuit the search
						select {
						case <-done: // It's already been closed
							return true
						default:
							close(done) // Mark the search as completed
							return true
						}
					}
				}
				return false
			}(); complete {
				return nil
			}
		}
		return nil
	}
	keyspaces, err := ts.TopoServer().GetKeyspaces(ctx)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get keyspaces: %v", err)
	}
	searchGroup, gctx := errgroup.WithContext(ctx)
	searchCompleted := make(chan struct{})
	for _, keyspace := range keyspaces {
		keyspace := keyspace // https://golang.org/doc/faq#closures_and_goroutines
		searchGroup.Go(func() error {
			return searchKeyspace(gctx, searchCompleted, keyspace)
		})
	}
	if err := searchGroup.Wait(); err != nil {
		return nil, err
	}

	if tablesFound != tableCount {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to locate all of the backing sequence tables being used; sequence table metadata: %+v",
			sequencesByBackingTable)
	}
	return sequencesByBackingTable, nil
}

// findSequenceUsageInKeyspace searches the keyspace's vschema for usage
// of sequences. It returns a map of sequence metadata keyed by the backing
// sequence table name -- if any usage is found -- along with a boolean to
// indicate if all of the backing sequence tables were defined using
// qualified table names (so we know where they all live) along with an
// error if any is seen.
func (ts *trafficSwitcher) findSequenceUsageInKeyspace(vschema *vschemapb.Keyspace) (map[string]*sequenceMetadata, bool, error) {
	allFullyQualified := true
	targets := maps.Values(ts.Targets())
	if len(targets) == 0 || targets[0].GetPrimary() == nil { // This should never happen
		return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no primary tablet found for target keyspace %s", ts.targetKeyspace)
	}
	targetDBName := targets[0].GetPrimary().DbName()
	sequencesByBackingTable := make(map[string]*sequenceMetadata)

	for _, table := range ts.Tables() {
		vs, ok := vschema.Tables[table]
		if !ok || vs == nil || vs.AutoIncrement == nil || vs.AutoIncrement.Sequence == "" {
			continue
		}
		sm := &sequenceMetadata{
			backingTableName:     vs.AutoIncrement.Sequence,
			usingTableName:       table,
			usingTableDefinition: vs,
			usingTableDBName:     targetDBName,
		}
		// If the sequence table is fully qualified in the vschema then
		// we don't need to find it later.
		if strings.Contains(vs.AutoIncrement.Sequence, ".") {
			keyspace, tableName, found := strings.Cut(vs.AutoIncrement.Sequence, ".")
			if !found {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence table name %s defined in the %s keyspace",
					vs.AutoIncrement.Sequence, ts.targetKeyspace)
			}
			sm.backingTableName = tableName
			sm.backingTableKeyspace = keyspace
			// Set the default keyspace name. We will later check to
			// see if the tablet we send requests to is using a dbname
			// override and use that if it is.
			sm.backingTableDBName = "vt_" + keyspace
		} else {
			allFullyQualified = false
		}
		sequencesByBackingTable[sm.backingTableName] = sm
	}

	return sequencesByBackingTable, allFullyQualified, nil
}

// initializeTargetSequences initializes the backing sequence tables
// using a map keyed by the backing sequence table name.
//
// The backing tables must have already been created. This function will
// then ensure that the next value is set to a value greater than any
// currently stored in the using table on the target keyspace. If the
// backing table is updated to a new higher value then it will also tell
// the primary tablet serving the sequence to refresh/reset its cache to
// be sure that it does not provide a value that is less than the current max.
func (ts *trafficSwitcher) initializeTargetSequences(ctx context.Context, sequencesByBackingTable map[string]*sequenceMetadata) error {
	initSequenceTable := func(ictx context.Context, sequenceTableName string, sequenceMetadata *sequenceMetadata) error {
		// Now we need to run this query on the target shards in order
		// to get the max value and set the next id for the sequence to
		// a higher value.
		shardResults := make([]int64, 0, len(ts.TargetShards()))
		srMu := sync.Mutex{}
		ierr := ts.ForAllTargets(func(target *workflow.MigrationTarget) error {
			primary := target.GetPrimary()
			if primary == nil || primary.GetAlias() == nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no primary tablet found for target shard %s/%s",
					ts.targetKeyspace, target.GetShard().ShardName())
			}
			query := sqlparser.BuildParsedQuery(sqlGetMaxSequenceVal,
				sqlescape.EscapeID(sequenceMetadata.usingTableDefinition.AutoIncrement.Column),
				sqlescape.EscapeID(sequenceMetadata.usingTableDBName),
				sqlescape.EscapeID(sequenceMetadata.usingTableName),
			)
			qr, terr := ts.wr.ExecuteFetchAsApp(ictx, primary.GetAlias(), true, query.Query, 1)
			if terr != nil || len(qr.Rows) != 1 {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the max used sequence value for target table %s.%s on tablet %s in order to initialize the backing sequence table: %v",
					ts.targetKeyspace, sequenceMetadata.usingTableName, topoproto.TabletAliasString(primary.Alias), terr)
			}
			rawVal := sqltypes.Proto3ToResult(qr).Rows[0][0]
			maxID := int64(0)
			if !rawVal.IsNull() { // If it's NULL then there are no rows and 0 remains the max
				maxID, terr = rawVal.ToInt64()
				if terr != nil {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the max used sequence value for target table %s.%s on tablet %s in order to initialize the backing sequence table: %v",
						ts.targetKeyspace, sequenceMetadata.usingTableName, topoproto.TabletAliasString(primary.Alias), terr)
				}
			}
			srMu.Lock()
			defer srMu.Unlock()
			shardResults = append(shardResults, maxID)
			return nil
		})
		if ierr != nil {
			return ierr
		}
		select {
		case <-ictx.Done():
			return ictx.Err()
		default:
		}
		if len(shardResults) == 0 { // This should never happen
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "did not get any results for the max used sequence value for target table %s.%s in order to initialize the backing sequence table",
				ts.targetKeyspace, sequenceMetadata.usingTableName)
		}
		// Sort the values to find the max value across all shards.
		sort.Slice(shardResults, func(i, j int) bool {
			return shardResults[i] < shardResults[j]
		})
		nextVal := shardResults[len(shardResults)-1] + 1
		// Now we need to update the sequence table, if needed, in order to
		// ensure that that the next value it provides is > the current max.
		sequenceShard, ierr := ts.wr.TopoServer().GetOnlyShard(ictx, sequenceMetadata.backingTableKeyspace)
		if ierr != nil || sequenceShard == nil || sequenceShard.PrimaryAlias == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the primary tablet for keyspace %s: %v",
				sequenceMetadata.backingTableKeyspace, ierr)
		}
		sequenceTablet, ierr := ts.wr.TopoServer().GetTablet(ictx, sequenceShard.PrimaryAlias)
		if ierr != nil || sequenceTablet == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the primary tablet for keyspace %s: %v",
				sequenceMetadata.backingTableKeyspace, ierr)
		}
		select {
		case <-ictx.Done():
			return ictx.Err()
		default:
		}
		if sequenceTablet.DbNameOverride != "" {
			sequenceMetadata.backingTableDBName = sequenceTablet.DbNameOverride
		}
		query := sqlparser.BuildParsedQuery(sqlInitSequenceTable,
			sqlescape.EscapeID(sequenceMetadata.backingTableDBName),
			sqlescape.EscapeID(sequenceMetadata.backingTableName),
			nextVal,
			nextVal,
			nextVal,
		)
		// Now execute this on the primary tablet of the unsharded keyspace
		// housing the backing table.
		qr, ierr := ts.wr.ExecuteFetchAsApp(ictx, sequenceShard.PrimaryAlias, true, query.Query, 1)
		if ierr != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to initialize the backing sequence table %s.%s: %v",
				sequenceMetadata.backingTableDBName, sequenceMetadata.backingTableName, ierr)
		}
		// If we actually updated the backing sequence table, then we need
		// to tell the primary tablet managing the sequence to refresh/reset
		// its cache for the table.
		if qr.RowsAffected == 0 {
			return nil
		}
		select {
		case <-ictx.Done():
			return ictx.Err()
		default:
		}
		ts.Logger().Infof("Resetting sequence cache for backing table %s on shard %s/%s using tablet %s",
			sequenceMetadata.backingTableName, sequenceShard.Keyspace(), sequenceShard.ShardName(), sequenceShard.PrimaryAlias)
		ti, ierr := ts.TopoServer().GetTablet(ictx, sequenceShard.PrimaryAlias)
		if ierr != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get primary tablet for keyspace %s: %v",
				sequenceMetadata.backingTableKeyspace, ierr)
		}
		ierr = ts.TabletManagerClient().ResetSequences(ictx, ti.Tablet, []string{sequenceMetadata.backingTableName})
		if ierr != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to reset the sequence cache for backing table %s on shard %s/%s using tablet %s: %v",
				sequenceMetadata.backingTableName, sequenceShard.Keyspace(), sequenceShard.ShardName(), sequenceShard.PrimaryAlias, ierr)
		}
		return nil
	}

	initGroup, gctx := errgroup.WithContext(ctx)
	for sequenceTableName, sequenceMetadata := range sequencesByBackingTable {
		sequenceTableName, sequenceMetadata := sequenceTableName, sequenceMetadata // https://golang.org/doc/faq#closures_and_goroutines
		initGroup.Go(func() error {
			return initSequenceTable(gctx, sequenceTableName, sequenceMetadata)
		})
	}
	return initGroup.Wait()
}

func (ts *trafficSwitcher) mustResetSequences(ctx context.Context) (bool, error) {
	switch ts.workflowType {
	case binlogdatapb.VReplicationWorkflowType_Migrate,
		binlogdatapb.VReplicationWorkflowType_MoveTables:
		return ts.isSequenceParticipating(ctx)
	default:
		return false, nil
	}
}

func (ts *trafficSwitcher) resetSequences(ctx context.Context) error {
	var err error
	mustReset := false
	if mustReset, err = ts.mustResetSequences(ctx); err != nil {
		return err
	}
	if !mustReset {
		return nil
	}
	return ts.ForAllSources(func(source *workflow.MigrationSource) error {
		ts.Logger().Infof("Resetting sequences for source shard %s.%s on tablet %s",
			source.GetShard().Keyspace(), source.GetShard().ShardName(), source.GetPrimary().String())
		return ts.TabletManagerClient().ResetSequences(ctx, source.GetPrimary().Tablet, ts.Tables())
	})
}
