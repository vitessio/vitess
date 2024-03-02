/*
Copyright 2021 The Vitess Authors.

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

package workflow

import (
	"context"
	"errors"
	"fmt"
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
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
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
	// Frozen is the message value of frozen vreplication streams.
	Frozen = "FROZEN"
	// Running is the state value of a vreplication stream in the
	// replicating state.
	Running = "RUNNING"

	// How long to wait when refreshing the state of each tablet in a shard. Note that these
	// are refreshed in parallel, non-topo errors are ignored (in the error handling) and we
	// may only do a partial refresh. Because in some cases it's unsafe to switch the traffic
	// if some tablets do not refresh, we may need to look for partial results and produce
	// an error (with the provided details of WHY) if we see them.
	// Side note: the default lock/lease TTL in etcd is 60s so the default tablet refresh
	// timeout of 60s can cause us to lose our keyspace lock before completing the
	// operation too.
	shardTabletRefreshTimeout = time.Duration(30 * time.Second)

	// Use pt-osc's naming convention, this format also ensures vstreamer ignores such tables.
	renameTableTemplate = "_%.59s_old" // limit table name to 64 characters

	sqlDeleteWorkflow    = "delete from _vt.vreplication where db_name = %s and workflow = %s"
	sqlGetMaxSequenceVal = "select max(%a) as maxval from %a.%a"
	sqlInitSequenceTable = "insert into %a.%a (id, next_id, cache) values (0, %d, 1000) on duplicate key update next_id = if(next_id < %d, %d, next_id)"
)

// accessType specifies the type of access for a shard (allow/disallow writes).
type accessType int

const (
	allowWrites = accessType(iota)
	disallowWrites
)

// The following constants define the switching direction.
const (
	DirectionForward = TrafficSwitchDirection(iota)
	DirectionBackward
)

// The following consts define if DropSource will drop or rename the table.
const (
	DropTable = TableRemovalType(iota)
	RenameTable
)

// TrafficSwitchDirection specifies the switching direction.
type TrafficSwitchDirection int

// TableRemovalType specifies the way the a table will be removed during a
// DropSource for a MoveTables workflow.
type TableRemovalType int

var (
	// ErrNoStreams occurs when no target streams are found for a workflow in a
	// target keyspace.
	ErrNoStreams = errors.New("no streams found")

	tableRemovalTypeStrs = []string{
		"DROP TABLE",
		"RENAME TABLE",
	}
)

// String returns a string representation of a TableRemovalType
func (trt TableRemovalType) String() string {
	if trt < DropTable || trt > RenameTable {
		return "Unknown"
	}

	return tableRemovalTypeStrs[trt]
}

// ITrafficSwitcher is a hack to allow us to maintain the legacy wrangler
// package for vtctl/vtctlclient while migrating most of the TrafficSwitcher
// related code to the workflow package for vtctldclient usage.
//
// After moving TrafficSwitcher to this package and removing the implementation
// in wrangler, this type should be removed, and StreamMigrator should be updated
// to contain a field of type *TrafficSwitcher instead of ITrafficSwitcher.
type ITrafficSwitcher interface {
	/* Functions that expose types and behavior contained in *wrangler.Wrangler */

	TopoServer() *topo.Server
	TabletManagerClient() tmclient.TabletManagerClient
	Logger() logutil.Logger
	// VReplicationExec here is used when we want the (*wrangler.Wrangler)
	// implementation, which does a topo lookup on the tablet alias before
	// calling the underlying TabletManagerClient RPC.
	VReplicationExec(ctx context.Context, alias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error)

	/* Functions that expose fields on the *wrangler.trafficSwitcher */

	ExternalTopo() *topo.Server
	MigrationType() binlogdatapb.MigrationType
	ReverseWorkflowName() string
	SourceKeyspaceName() string
	SourceKeyspaceSchema() *vindexes.KeyspaceSchema
	Sources() map[string]*MigrationSource
	Tables() []string
	TargetKeyspaceName() string
	Targets() map[string]*MigrationTarget
	WorkflowName() string
	SourceTimeZone() string

	/* Functions that *wrangler.trafficSwitcher implements */

	ForAllSources(f func(source *MigrationSource) error) error
	ForAllTargets(f func(target *MigrationTarget) error) error
	ForAllUIDs(f func(target *MigrationTarget, uid int32) error) error
	SourceShards() []*topo.ShardInfo
	TargetShards() []*topo.ShardInfo
}

// TargetInfo contains the metadata for a set of targets involved in a workflow.
type TargetInfo struct {
	Targets         map[string]*MigrationTarget
	Frozen          bool
	OptCells        string
	OptTabletTypes  string
	WorkflowType    binlogdatapb.VReplicationWorkflowType
	WorkflowSubType binlogdatapb.VReplicationWorkflowSubType
}

// MigrationSource contains the metadata for each migration source.
type MigrationSource struct {
	si        *topo.ShardInfo
	primary   *topo.TabletInfo
	Position  string
	Journaled bool
}

// NewMigrationSource returns a MigrationSource for the given shard and primary.
//
// (TODO|@ajm188): do we always want to start with (position:"", journaled:false)?
func NewMigrationSource(si *topo.ShardInfo, primary *topo.TabletInfo) *MigrationSource {
	return &MigrationSource{
		si:      si,
		primary: primary,
	}
}

// GetShard returns the *topo.ShardInfo for the migration source.
func (source *MigrationSource) GetShard() *topo.ShardInfo {
	return source.si
}

// GetPrimary returns the *topo.TabletInfo for the primary tablet of the
// migration source.
func (source *MigrationSource) GetPrimary() *topo.TabletInfo {
	return source.primary
}

// trafficSwitcher contains the metadata for switching read and write traffic
// for vreplication streams.
type trafficSwitcher struct {
	ws     *Server
	logger logutil.Logger

	migrationType      binlogdatapb.MigrationType
	isPartialMigration bool
	workflow           string

	// if frozen is true, the rest of the fields are not set.
	frozen           bool
	reverseWorkflow  string
	id               int64
	sources          map[string]*MigrationSource
	targets          map[string]*MigrationTarget
	sourceKeyspace   string
	targetKeyspace   string
	tables           []string
	keepRoutingRules bool
	sourceKSSchema   *vindexes.KeyspaceSchema
	optCells         string // cells option passed to MoveTables/Reshard Create
	optTabletTypes   string // tabletTypes option passed to MoveTables/Reshard Create
	externalCluster  string
	externalTopo     *topo.Server
	sourceTimeZone   string
	targetTimeZone   string
	workflowType     binlogdatapb.VReplicationWorkflowType
	workflowSubType  binlogdatapb.VReplicationWorkflowSubType
}

func (ts *trafficSwitcher) TopoServer() *topo.Server                          { return ts.ws.ts }
func (ts *trafficSwitcher) TabletManagerClient() tmclient.TabletManagerClient { return ts.ws.tmc }
func (ts *trafficSwitcher) Logger() logutil.Logger {
	if ts.logger == nil {
		ts.logger = logutil.NewConsoleLogger()
	}
	return ts.logger
}
func (ts *trafficSwitcher) VReplicationExec(ctx context.Context, alias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error) {
	return ts.ws.VReplicationExec(ctx, alias, query)
}
func (ts *trafficSwitcher) ExternalTopo() *topo.Server                     { return ts.externalTopo }
func (ts *trafficSwitcher) MigrationType() binlogdatapb.MigrationType      { return ts.migrationType }
func (ts *trafficSwitcher) IsPartialMigration() bool                       { return ts.isPartialMigration }
func (ts *trafficSwitcher) ReverseWorkflowName() string                    { return ts.reverseWorkflow }
func (ts *trafficSwitcher) SourceKeyspaceName() string                     { return ts.sourceKSSchema.Keyspace.Name }
func (ts *trafficSwitcher) SourceKeyspaceSchema() *vindexes.KeyspaceSchema { return ts.sourceKSSchema }
func (ts *trafficSwitcher) Sources() map[string]*MigrationSource           { return ts.sources }
func (ts *trafficSwitcher) Tables() []string                               { return ts.tables }
func (ts *trafficSwitcher) TargetKeyspaceName() string                     { return ts.targetKeyspace }
func (ts *trafficSwitcher) Targets() map[string]*MigrationTarget           { return ts.targets }
func (ts *trafficSwitcher) WorkflowName() string                           { return ts.workflow }
func (ts *trafficSwitcher) SourceTimeZone() string                         { return ts.sourceTimeZone }
func (ts *trafficSwitcher) TargetTimeZone() string                         { return ts.targetTimeZone }

func (ts *trafficSwitcher) ForAllSources(f func(source *MigrationSource) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, source := range ts.sources {
		wg.Add(1)
		go func(source *MigrationSource) {
			defer wg.Done()

			if err := f(source); err != nil {
				allErrors.RecordError(err)
			}
		}(source)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (ts *trafficSwitcher) ForAllTargets(f func(source *MigrationTarget) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range ts.targets {
		wg.Add(1)
		go func(target *MigrationTarget) {
			defer wg.Done()

			if err := f(target); err != nil {
				allErrors.RecordError(err)
			}
		}(target)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

// MigrationTarget contains the metadata for each migration target.
type MigrationTarget struct {
	si       *topo.ShardInfo
	primary  *topo.TabletInfo
	Sources  map[int32]*binlogdatapb.BinlogSource
	Position string
}

// GetShard returns the *topo.ShardInfo for the migration target.
func (target *MigrationTarget) GetShard() *topo.ShardInfo {
	return target.si
}

// GetPrimary returns the *topo.TabletInfo for the primary tablet of the
// migration target.
func (target *MigrationTarget) GetPrimary() *topo.TabletInfo {
	return target.primary
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

// isPartialMoveTables returns true if whe workflow is MoveTables, has the same
// number of shards, is not covering the entire shard range, and has one-to-one
// shards in source and target.
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

// addParticipatingTablesToKeyspace updates the vschema with the new tables that
// were created as part of the Migrate flow. It is called when the Migrate flow
// is Completed.
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

func (ts *trafficSwitcher) dropSourceDeniedTables(ctx context.Context) error {
	return ts.ForAllSources(func(source *MigrationSource) error {
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
	return ts.ForAllTargets(func(target *MigrationTarget) error {
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

func (ts *trafficSwitcher) removeSourceTables(ctx context.Context, removalType TableRemovalType) error {
	err := ts.ForAllSources(func(source *MigrationSource) error {
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
			if removalType == DropTable {
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
			_, err = ts.ws.tmc.ExecuteFetchAsDba(ctx, source.GetPrimary().Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
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

// FIXME: even after dropSourceShards there are still entries in the topo, need to research and fix
func (ts *trafficSwitcher) dropSourceShards(ctx context.Context) error {
	return ts.ForAllSources(func(source *MigrationSource) error {
		ts.Logger().Infof("Deleting shard %s.%s\n", source.GetShard().Keyspace(), source.GetShard().ShardName())
		err := ts.ws.DeleteShard(ctx, source.GetShard().Keyspace(), source.GetShard().ShardName(), true, false)
		if err != nil {
			ts.Logger().Errorf("Error deleting shard %s: %v", source.GetShard().ShardName(), err)
			return err
		}
		ts.Logger().Infof("Deleted shard %s.%s\n", source.GetShard().Keyspace(), source.GetShard().ShardName())
		return nil
	})
}

func (ts *trafficSwitcher) switchShardReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction TrafficSwitchDirection) error {
	var fromShards, toShards []*topo.ShardInfo
	if direction == DirectionForward {
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
		if err := ts.ws.updateShardRecords(ctx, ts.SourceKeyspaceName(), fromShards, cells, servedType, true /* isFrom */, false /* clearSourceShards */, ts.logger); err != nil {
			return err
		}
		if err := ts.ws.updateShardRecords(ctx, ts.SourceKeyspaceName(), toShards, cells, servedType, false, false, ts.logger); err != nil {
			return err
		}
		err := ts.TopoServer().MigrateServedType(ctx, ts.SourceKeyspaceName(), toShards, fromShards, servedType, cells)
		if err != nil {
			return err
		}
	}
	if err := ts.TopoServer().ValidateSrvKeyspace(ctx, ts.TargetKeyspaceName(), strings.Join(cells, ",")); err != nil {
		err2 := vterrors.Wrapf(err, "after switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			ts.TargetKeyspaceName(), strings.Join(cells, ","))
		log.Errorf("%w", err2)
		return err2
	}
	return nil
}

func (ts *trafficSwitcher) switchTableReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction TrafficSwitchDirection) error {
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
		if servedType != topodatapb.TabletType_REPLICA && servedType != topodatapb.TabletType_RDONLY {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid tablet type specified when switching reads: %v", servedType)
		}

		tt := strings.ToLower(servedType.String())
		for _, table := range ts.Tables() {
			if direction == DirectionForward {
				log.Infof("Route direction forward")
			} else {
				log.Infof("Route direction backwards")
			}
			toTarget := []string{ts.TargetKeyspaceName() + "." + table}
			rules[table+"@"+tt] = toTarget
			rules[ts.TargetKeyspaceName()+"."+table+"@"+tt] = toTarget
			rules[ts.SourceKeyspaceName()+"."+table+"@"+tt] = toTarget
		}
	}
	if err := topotools.SaveRoutingRules(ctx, ts.TopoServer(), rules); err != nil {
		return err
	}
	return ts.TopoServer().RebuildSrvVSchema(ctx, cells)
}

func (ts *trafficSwitcher) startReverseVReplication(ctx context.Context) error {
	return ts.ForAllSources(func(source *MigrationSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s",
			encodeString(source.GetPrimary().DbName()), encodeString(ts.ReverseWorkflowName()))
		_, err := ts.VReplicationExec(ctx, source.GetPrimary().Alias, query)
		return err
	})
}

func (ts *trafficSwitcher) createJournals(ctx context.Context, sourceWorkflows []string) error {
	log.Infof("In createJournals for source workflows %+v", sourceWorkflows)
	return ts.ForAllSources(func(source *MigrationSource) error {
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

func (ts *trafficSwitcher) changeShardsAccess(ctx context.Context, keyspace string, shards []*topo.ShardInfo, access accessType) error {
	if err := ts.TopoServer().UpdateDisableQueryService(ctx, keyspace, shards, topodatapb.TabletType_PRIMARY, nil, access == disallowWrites /* disable */); err != nil {
		return err
	}
	return ts.ws.refreshPrimaryTablets(ctx, shards)
}

func (ts *trafficSwitcher) allowTargetWrites(ctx context.Context) error {
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		return ts.allowTableTargetWrites(ctx)
	}
	return ts.changeShardsAccess(ctx, ts.TargetKeyspaceName(), ts.TargetShards(), allowWrites)
}

func (ts *trafficSwitcher) allowTableTargetWrites(ctx context.Context) error {
	return ts.ForAllTargets(func(target *MigrationTarget) error {
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
	err := ts.ForAllSources(func(source *MigrationSource) error {
		_, err := ts.TopoServer().UpdateShardFields(ctx, ts.SourceKeyspaceName(), source.GetShard().ShardName(), func(si *topo.ShardInfo) error {
			si.IsPrimaryServing = false
			return nil
		})
		return err
	})
	if err != nil {
		return err
	}
	err = ts.ForAllTargets(func(target *MigrationTarget) error {
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
		err2 := vterrors.Wrapf(err, "after changing shard routes, found SrvKeyspace for %s is corrupt", ts.TargetKeyspaceName())
		log.Errorf("%w", err2)
		return err2
	}
	return nil
}

func (ts *trafficSwitcher) getReverseVReplicationUpdateQuery(targetCell string, sourceCell string, dbname string) string {
	// we try to be clever to understand what user intends:
	// if target's cell is present in cells but not source's cell we replace it
	// with the source's cell.
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
	return ts.ForAllSources(func(source *MigrationSource) error {
		query := fmt.Sprintf(sqlDeleteWorkflow, encodeString(source.GetPrimary().DbName()), encodeString(ts.reverseWorkflow))
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, source.GetPrimary().Tablet, query); err != nil {
			return err
		}
		ts.ws.deleteWorkflowVDiffData(ctx, source.GetPrimary().Tablet, ts.reverseWorkflow)
		ts.ws.optimizeCopyStateTable(source.GetPrimary().Tablet)
		return nil
	})
}

func (ts *trafficSwitcher) ForAllUIDs(f func(target *MigrationTarget, uid int32) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range ts.Targets() {
		for uid := range target.Sources {
			wg.Add(1)
			go func(target *MigrationTarget, uid int32) {
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

func (ts *trafficSwitcher) createReverseVReplication(ctx context.Context) error {
	if err := ts.deleteReverseVReplication(ctx); err != nil {
		return err
	}
	err := ts.ForAllUIDs(func(target *MigrationTarget, uid int32) error {
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
		log.Infof("Creating reverse workflow vreplication stream on tablet %s: workflow %s, startPos %s",
			source.GetPrimary().Alias, ts.ReverseWorkflowName(), target.Position)
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

func (ts *trafficSwitcher) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()
	// Source writes have been stopped, wait for all streams on targets to catch up.
	if err := ts.ForAllUIDs(func(target *MigrationTarget, uid int32) error {
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
			log.Infof("Error marking stopped for cutover on %s, uid %d", target.GetPrimary().AliasString(), uid)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	// all targets have caught up, record their positions for setting up reverse workflows
	return ts.ForAllTargets(func(target *MigrationTarget) error {
		var err error
		target.Position, err = ts.TabletManagerClient().PrimaryPosition(ctx, target.GetPrimary().Tablet)
		ts.Logger().Infof("After catchup, position for target primary %s, %v", target.GetPrimary().AliasString(), target.Position)
		return err
	})
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
	return ts.ForAllSources(func(source *MigrationSource) error {
		var err error
		source.Position, err = ts.TabletManagerClient().PrimaryPosition(ctx, source.GetPrimary().Tablet)
		log.Infof("Stopped Source Writes. Position for source %v:%v: %v",
			ts.SourceKeyspaceName(), source.GetShard().ShardName(), source.Position)
		if err != nil {
			log.Warningf("Error: %s", err)
		}
		return err
	})
}

func (ts *trafficSwitcher) changeTableSourceWrites(ctx context.Context, access accessType) error {
	err := ts.ForAllSources(func(source *MigrationSource) error {
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

func (ts *trafficSwitcher) cancelMigration(ctx context.Context, sm *StreamMigrator) {
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

	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s",
			encodeString(target.GetPrimary().DbName()), encodeString(ts.WorkflowName()))
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

func (ts *trafficSwitcher) freezeTargetVReplication(ctx context.Context) error {
	// Mark target streams as frozen before deleting. If SwitchWrites gets
	// re-invoked after a freeze, it will skip all the previous steps
	err := ts.ForAllTargets(func(target *MigrationTarget) error {
		ts.Logger().Infof("Marking target streams frozen for workflow %s db_name %s", ts.WorkflowName(), target.GetPrimary().DbName())
		query := fmt.Sprintf("update _vt.vreplication set message = '%s' where db_name=%s and workflow=%s", Frozen,
			encodeString(target.GetPrimary().DbName()), encodeString(ts.WorkflowName()))
		_, err := ts.TabletManagerClient().VReplicationExec(ctx, target.GetPrimary().Tablet, query)
		return err
	})
	if err != nil {
		return err
	}
	return nil
}

func (ts *trafficSwitcher) dropTargetVReplicationStreams(ctx context.Context) error {
	return ts.ForAllTargets(func(target *MigrationTarget) error {
		ts.Logger().Infof("Deleting target streams and related data for workflow %s db_name %s", ts.WorkflowName(), target.GetPrimary().DbName())
		query := fmt.Sprintf(sqlDeleteWorkflow, encodeString(target.GetPrimary().DbName()), encodeString(ts.WorkflowName()))
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, target.GetPrimary().Tablet, query); err != nil {
			return err
		}
		ts.ws.deleteWorkflowVDiffData(ctx, target.GetPrimary().Tablet, ts.WorkflowName())
		ts.ws.optimizeCopyStateTable(target.GetPrimary().Tablet)
		return nil
	})
}

func (ts *trafficSwitcher) dropSourceReverseVReplicationStreams(ctx context.Context) error {
	return ts.ForAllSources(func(source *MigrationSource) error {
		ts.Logger().Infof("Deleting reverse streams and related data for workflow %s db_name %s", ts.WorkflowName(), source.GetPrimary().DbName())
		query := fmt.Sprintf(sqlDeleteWorkflow, encodeString(source.GetPrimary().DbName()), encodeString(ReverseWorkflowName(ts.WorkflowName())))
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, source.GetPrimary().Tablet, query); err != nil {
			return err
		}
		ts.ws.deleteWorkflowVDiffData(ctx, source.GetPrimary().Tablet, ReverseWorkflowName(ts.WorkflowName()))
		ts.ws.optimizeCopyStateTable(source.GetPrimary().Tablet)
		return nil
	})
}

func (ts *trafficSwitcher) removeTargetTables(ctx context.Context) error {
	err := ts.ForAllTargets(func(target *MigrationTarget) error {
		log.Infof("ForAllTargets: %+v", target)
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
			res, err := ts.ws.tmc.ExecuteFetchAsDba(ctx, target.GetPrimary().Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
				Query:                   []byte(query),
				MaxRows:                 1,
				ReloadSchema:            true,
				DisableForeignKeyChecks: true,
			})
			log.Infof("Removed target table with result: %+v", res)
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
	return ts.ForAllTargets(func(target *MigrationTarget) error {
		ts.Logger().Infof("Deleting shard %s.%s\n", target.GetShard().Keyspace(), target.GetShard().ShardName())
		err := ts.ws.DeleteShard(ctx, target.GetShard().Keyspace(), target.GetShard().ShardName(), true, false)
		if err != nil {
			ts.Logger().Errorf("Error deleting shard %s: %v", target.GetShard().ShardName(), err)
			return err
		}
		ts.Logger().Infof("Deleted shard %s.%s\n", target.GetShard().Keyspace(), target.GetShard().ShardName())
		return nil
	})
}

func (ts *trafficSwitcher) validate(ctx context.Context) error {
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		if ts.isPartialMigration {
			return nil
		}
		sourceTopo := ts.ws.ts
		if ts.externalTopo != nil {
			sourceTopo = ts.externalTopo
		}

		// All shards must be present.
		if err := CompareShards(ctx, ts.SourceKeyspaceName(), ts.SourceShards(), sourceTopo); err != nil {
			return err
		}
		if err := CompareShards(ctx, ts.TargetKeyspaceName(), ts.TargetShards(), ts.ws.ts); err != nil {
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

// checkJournals returns true if at least one journal has been created.
// If so, it also returns the list of sourceWorkflows that need to be switched.
func (ts *trafficSwitcher) checkJournals(ctx context.Context) (journalsExist bool, sourceWorkflows []string, err error) {
	var mu sync.Mutex

	err = ts.ForAllSources(func(source *MigrationSource) error {
		mu.Lock()
		defer mu.Unlock()
		journal, exists, err := ts.ws.CheckReshardingJournalExistsOnTablet(ctx, source.GetPrimary().Tablet, ts.id)
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
	// trim extra trailing comma
	lockStmt := sb.String()[:sb.Len()-1]

	return ts.ForAllSources(func(source *MigrationSource) error {
		primary := source.GetPrimary()
		if primary == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no primary found for source shard %s", source.GetShard())
		}
		tablet := primary.Tablet
		_, err := ts.ws.tmc.ExecuteFetchAsDba(ctx, tablet, true, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
			Query:          []byte(lockStmt),
			MaxRows:        uint64(1),
			DisableBinlogs: false,
			ReloadSchema:   true,
		})
		if err != nil {
			ts.Logger().Errorf("Error executing %s on source tablet %v: %v", lockStmt, tablet, err)
			return err
		}
		return err
	})
}

func (ts *trafficSwitcher) gatherPositions(ctx context.Context) error {
	err := ts.ForAllSources(func(source *MigrationSource) error {
		var err error
		source.Position, err = ts.ws.tmc.PrimaryPosition(ctx, source.GetPrimary().Tablet)
		ts.Logger().Infof("Position for source %v:%v: %v", ts.SourceKeyspaceName(), source.GetShard().ShardName(), source.Position)
		return err
	})
	if err != nil {
		return err
	}
	return ts.ForAllTargets(func(target *MigrationTarget) error {
		var err error
		target.Position, err = ts.ws.tmc.PrimaryPosition(ctx, target.GetPrimary().Tablet)
		ts.Logger().Infof("Position for target %v:%v: %v", ts.TargetKeyspaceName(), target.GetShard().ShardName(), target.Position)
		return err
	})
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
		ierr := ts.ForAllTargets(func(target *MigrationTarget) error {
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
			qr, terr := ts.ws.tmc.ExecuteFetchAsApp(ictx, primary.Tablet, true, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
				Query:   []byte(query.Query),
				MaxRows: 1,
			})
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
		// Sort the values to find the max value across all shards.
		sort.Slice(shardResults, func(i, j int) bool {
			return shardResults[i] < shardResults[j]
		})
		nextVal := shardResults[len(shardResults)-1] + 1
		// Now we need to update the sequence table, if needed, in order to
		// ensure that that the next value it provides is > the current max.
		sequenceShard, ierr := ts.TopoServer().GetOnlyShard(ictx, sequenceMetadata.backingTableKeyspace)
		if ierr != nil || sequenceShard == nil || sequenceShard.PrimaryAlias == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the primary tablet for keyspace %s: %v",
				sequenceMetadata.backingTableKeyspace, ierr)
		}
		sequenceTablet, ierr := ts.TopoServer().GetTablet(ictx, sequenceShard.PrimaryAlias)
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
		primaryTablet, ierr := ts.TopoServer().GetTablet(ictx, sequenceShard.PrimaryAlias)
		if ierr != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the primary tablet for %s.%s using alias %s: %v",
				sequenceShard.Keyspace(), sequenceShard.ShardName(), sequenceShard.PrimaryAlias, ierr)
		}
		qr, ierr := ts.ws.tmc.ExecuteFetchAsApp(ictx, primaryTablet.Tablet, true, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
			Query:   []byte(query.Query),
			MaxRows: 1,
		})
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
	return ts.ForAllSources(func(source *MigrationSource) error {
		ts.Logger().Infof("Resetting sequences for source shard %s.%s on tablet %s",
			source.GetShard().Keyspace(), source.GetShard().ShardName(), source.GetPrimary().String())
		return ts.TabletManagerClient().ResetSequences(ctx, source.GetPrimary().Tablet, ts.Tables())
	})
}
