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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
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

	sqlDeleteWorkflow      = "delete from _vt.vreplication where db_name = %s and workflow = %s"
	sqlGetMaxSequenceVal   = "select max(%a) as maxval from %a.%a"
	sqlInitSequenceTable   = "insert into %a.%a (id, next_id, cache) values (0, %d, 1000) on duplicate key update next_id = if(next_id < %d, %d, next_id)"
	sqlCreateSequenceTable = "create table if not exists %a (id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence'"
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

func (tsd TrafficSwitchDirection) String() string {
	if tsd == DirectionForward {
		return "forward"
	}
	return "backward"
}

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
	Options         *vtctldatapb.WorkflowOptions
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

	// Should we continue if we encounter some potentially non-fatal errors such
	// as partial tablet refreshes?
	force bool
	// If frozen is true, the rest of the fields are not set.
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
	options          *vtctldatapb.WorkflowOptions
}

func (ts *trafficSwitcher) TopoServer() *topo.Server                          { return ts.ws.ts }
func (ts *trafficSwitcher) TabletManagerClient() tmclient.TabletManagerClient { return ts.ws.tmc }
func (ts *trafficSwitcher) Logger() logutil.Logger {
	if ts.logger == nil {
		ts.logger = logutil.NewConsoleLogger() // Use the default system logger
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

// isPartialMoveTables returns true if the workflow is MoveTables, has the same
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
		if err := json2.UnmarshalPB([]byte(wrap), ks); err != nil {
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
		if topo.IsErrType(err, topo.NoNode) {
			ts.Logger().Warningf("No shard routing rules found when attempting to delete the ones for the %s keyspace", ts.targetKeyspace)
			return nil
		}
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

func (ts *trafficSwitcher) deleteKeyspaceRoutingRules(ctx context.Context) error {
	if !ts.IsMultiTenantMigration() {
		return nil
	}
	ts.Logger().Infof("deleteKeyspaceRoutingRules: workflow %s.%s", ts.targetKeyspace, ts.workflow)
	reason := fmt.Sprintf("Deleting rules for %s", ts.SourceKeyspaceName())
	return topotools.UpdateKeyspaceRoutingRules(ctx, ts.TopoServer(), reason,
		func(ctx context.Context, rules *map[string]string) error {
			for _, suffix := range tabletTypeSuffixes {
				delete(*rules, ts.SourceKeyspaceName()+suffix)
			}
			return nil
		})
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
		isPartial, partialDetails, err := topotools.RefreshTabletsByShard(rtbsCtx, ts.TopoServer(), ts.TabletManagerClient(), source.GetShard(), nil, ts.Logger())
		if isPartial {
			msg := fmt.Sprintf("failed to successfully refresh all tablets in the %s/%s source shard (%v):\n  %v",
				source.GetShard().Keyspace(), source.GetShard().ShardName(), err, partialDetails)
			if ts.force {
				log.Warning(msg)
				return nil
			} else {
				return errors.New(msg)
			}
		}
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
		isPartial, partialDetails, err := topotools.RefreshTabletsByShard(rtbsCtx, ts.TopoServer(), ts.TabletManagerClient(), target.GetShard(), nil, ts.Logger())
		if isPartial {
			msg := fmt.Sprintf("failed to successfully refresh all tablets in the %s/%s target shard (%v):\n  %v",
				target.GetShard().Keyspace(), target.GetShard().ShardName(), err, partialDetails)
			if ts.force {
				log.Warning(msg)
				return nil
			} else {
				return errors.New(msg)
			}
		}
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
					topoproto.TabletAliasString(source.GetPrimary().GetAlias()), source.GetPrimary().DbName(), tableName)
			} else {
				renameName, err := sqlescape.EnsureEscaped(getRenameFileName(tableName))
				if err != nil {
					return err
				}
				ts.Logger().Infof("%s: Renaming table %s.%s to %s.%s\n",
					topoproto.TabletAliasString(source.GetPrimary().GetAlias()), source.GetPrimary().DbName(), tableName, source.GetPrimary().DbName(), renameName)
				query = fmt.Sprintf("rename table %s.%s TO %s.%s", primaryDbName, tableNameEscaped, primaryDbName, renameName)
			}
			_, err = ts.ws.tmc.ExecuteFetchAsDba(ctx, source.GetPrimary().Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
				Query:                   []byte(query),
				MaxRows:                 1,
				ReloadSchema:            true,
				DisableForeignKeyChecks: true,
			})
			if err != nil {
				if IsTableDidNotExistError(err) {
					ts.Logger().Warningf("%s: Table %s did not exist when attempting to remove it", topoproto.TabletAliasString(source.GetPrimary().GetAlias()), tableName)
				} else {
					ts.Logger().Errorf("%s: Error removing table %s: %v", topoproto.TabletAliasString(source.GetPrimary().GetAlias()), tableName, err)
					return err
				}
			}
			ts.Logger().Infof("%s: Removed table %s.%s\n", topoproto.TabletAliasString(source.GetPrimary().GetAlias()), source.GetPrimary().DbName(), tableName)

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
	ts.Logger().Infof("switchShardReads: workflow: %s, direction: %s, cells: %v, tablet types: %v",
		ts.workflow, direction.String(), cells, servedTypes)

	var fromShards, toShards []*topo.ShardInfo
	if direction == DirectionForward {
		fromShards, toShards = ts.SourceShards(), ts.TargetShards()
	} else {
		fromShards, toShards = ts.TargetShards(), ts.SourceShards()
	}

	cellsStr := strings.Join(cells, ",")
	if err := ts.TopoServer().ValidateSrvKeyspace(ctx, ts.TargetKeyspaceName(), cellsStr); err != nil {
		err2 := vterrors.Wrapf(err, "Before switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			ts.TargetKeyspaceName(), cellsStr)
		ts.Logger().Errorf("%w", err2)
		return err2
	}
	for _, servedType := range servedTypes {
		if err := ts.ws.updateShardRecords(ctx, ts.SourceKeyspaceName(), fromShards, cells, servedType, true /* isFrom */, false /* clearSourceShards */, ts.Logger()); err != nil {
			return err
		}
		if err := ts.ws.updateShardRecords(ctx, ts.SourceKeyspaceName(), toShards, cells, servedType, false, false, ts.Logger()); err != nil {
			return err
		}
		err := ts.TopoServer().MigrateServedType(ctx, ts.SourceKeyspaceName(), toShards, fromShards, servedType, cells)
		if err != nil {
			return err
		}
	}
	if err := ts.TopoServer().ValidateSrvKeyspace(ctx, ts.TargetKeyspaceName(), cellsStr); err != nil {
		err2 := vterrors.Wrapf(err, "after switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			ts.TargetKeyspaceName(), cellsStr)
		ts.Logger().Errorf("%w", err2)
		return err2
	}
	return nil
}

func (ts *trafficSwitcher) switchTableReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, rebuildSrvVSchema bool, direction TrafficSwitchDirection) error {
	ts.Logger().Infof("switchTableReads: workflow: %s, direction: %s, cells: %v, tablet types: %v",
		ts.workflow, direction.String(), cells, servedTypes)

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
				toTarget := []string{ts.TargetKeyspaceName() + "." + table}
				rules[table+"@"+tt] = toTarget
				rules[ts.TargetKeyspaceName()+"."+table+"@"+tt] = toTarget
				rules[ts.SourceKeyspaceName()+"."+table+"@"+tt] = toTarget
			} else {
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
	if rebuildSrvVSchema {
		return ts.TopoServer().RebuildSrvVSchema(ctx, cells)
	}
	return nil
}

func (ts *trafficSwitcher) startReverseVReplication(ctx context.Context) error {
	return ts.ForAllSources(func(source *MigrationSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s",
			encodeString(source.GetPrimary().DbName()), encodeString(ts.ReverseWorkflowName()))
		_, err := ts.VReplicationExec(ctx, source.GetPrimary().GetAlias(), query)
		return err
	})
}

func (ts *trafficSwitcher) createJournals(ctx context.Context, sourceWorkflows []string) error {
	ts.Logger().Infof("In createJournals for source workflows %+v", sourceWorkflows)
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
	return ts.ws.refreshPrimaryTablets(ctx, shards, ts.force)
}

func (ts *trafficSwitcher) allowTargetWrites(ctx context.Context) error {
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		return ts.switchDeniedTables(ctx, false)
	}
	return ts.changeShardsAccess(ctx, ts.TargetKeyspaceName(), ts.TargetShards(), allowWrites)
}

func (ts *trafficSwitcher) changeRouting(ctx context.Context) error {
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		return ts.changeWriteRoute(ctx)
	}
	return ts.changeShardRouting(ctx)
}

func (ts *trafficSwitcher) changeWriteRoute(ctx context.Context) error {
	if ts.IsMultiTenantMigration() {
		// For multi-tenant migrations, we can only move forward and not backwards.
		ts.Logger().Infof("Pointing keyspace routing rules for primary to %s for workflow %s", ts.TargetKeyspaceName(), ts.workflow)
		if err := changeKeyspaceRouting(ctx, ts.TopoServer(), []topodatapb.TabletType{topodatapb.TabletType_PRIMARY},
			ts.SourceKeyspaceName() /* from */, ts.TargetKeyspaceName() /* to */, "SwitchWrites"); err != nil {
			return err
		}
	} else if ts.isPartialMigration {
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
		ts.Logger().Errorf("%w", err2)
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
		ts.Logger().Errorf("%w", err2)
		return err2
	}
	return nil
}

func (ts *trafficSwitcher) getReverseVReplicationUpdateQuery(targetCell string, sourceCell string, dbname string, options string) string {
	// we try to be clever to understand what user intends:
	// if target's cell is present in cells but not source's cell we replace it
	// with the source's cell.
	if ts.optCells != "" && targetCell != sourceCell && strings.Contains(ts.optCells+",", targetCell+",") &&
		!strings.Contains(ts.optCells+",", sourceCell+",") {
		ts.optCells = strings.Replace(ts.optCells, targetCell, sourceCell, 1)
	}

	if ts.optCells != "" || ts.optTabletTypes != "" {
		query := fmt.Sprintf("update _vt.vreplication set cell = %s, tablet_types = %s, options = %s where workflow = %s and db_name = %s",
			sqltypes.EncodeStringSQL(ts.optCells), sqltypes.EncodeStringSQL(ts.optTabletTypes), sqltypes.EncodeStringSQL(options), sqltypes.EncodeStringSQL(ts.ReverseWorkflowName()), sqltypes.EncodeStringSQL(dbname))
		return query
	}
	return ""
}

func (ts *trafficSwitcher) deleteReverseVReplication(ctx context.Context) error {
	return ts.ForAllSources(func(source *MigrationSource) error {
		query := fmt.Sprintf(sqlDeleteWorkflow, encodeString(source.GetPrimary().DbName()), encodeString(ts.reverseWorkflow))
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, source.GetPrimary().Tablet, query); err != nil {
			// vreplication.exec returns no error on delete if the rows do not exist.
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
		var err error
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
							inKeyrange = fmt.Sprintf(" where in_keyrange(%s, '%s.%s', %s)", sqlparser.String(vtable.ColumnVindexes[0].Columns[0]),
								ts.SourceKeyspaceName(), vtable.ColumnVindexes[0].Name, encodeString(key.KeyRangeString(source.GetShard().KeyRange)))
						} else {
							return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no primary vindex found for the %s table in the %s keyspace",
								vtable.Name.String(), ts.SourceKeyspaceName())
						}
					}
				}
				filter = fmt.Sprintf("select * from %s%s", sqlescape.EscapeID(rule.Match), inKeyrange)
				if ts.IsMultiTenantMigration() {
					filter, err = ts.addTenantFilter(ctx, filter)
					if err != nil {
						return err
					}
				}
			}
			reverseBls.Filter.Rules = append(reverseBls.Filter.Rules, &binlogdatapb.Rule{
				Match:  rule.Match,
				Filter: filter,
			})
		}
		ts.Logger().Infof("Creating reverse workflow vreplication stream on tablet %s: workflow %s, startPos %s",
			source.GetPrimary().GetAlias(), ts.ReverseWorkflowName(), target.Position)
		_, err = ts.VReplicationExec(ctx, source.GetPrimary().GetAlias(),
			binlogplayer.CreateVReplicationState(ts.ReverseWorkflowName(), reverseBls, target.Position,
				binlogdatapb.VReplicationWorkflowState_Stopped, source.GetPrimary().DbName(), ts.workflowType, ts.workflowSubType))
		if err != nil {
			return err
		}

		// if user has defined the cell/tablet_types parameters in the forward workflow, update the reverse workflow as well
		optionsJSON, err := json.Marshal(ts.options)
		if err != nil {
			return err
		}
		updateQuery := ts.getReverseVReplicationUpdateQuery(target.GetPrimary().GetAlias().GetCell(),
			source.GetPrimary().GetAlias().GetCell(), source.GetPrimary().DbName(), string(optionsJSON))
		if updateQuery != "" {
			ts.Logger().Infof("Updating vreplication stream entry on %s with: %s", source.GetPrimary().GetAlias(), updateQuery)
			_, err = ts.VReplicationExec(ctx, source.GetPrimary().GetAlias(), updateQuery)
			return err
		}
		return nil
	})
	return err
}

func (ts *trafficSwitcher) addTenantFilter(ctx context.Context, filter string) (string, error) {
	parser := ts.ws.env.Parser()
	vschema, err := ts.TopoServer().GetVSchema(ctx, ts.targetKeyspace)
	if err != nil {
		return "", err
	}
	targetVSchema, err := vindexes.BuildKeyspaceSchema(vschema, ts.targetKeyspace, parser)
	if err != nil {
		return "", err
	}
	tenantClause, err := getTenantClause(ts.options, targetVSchema, parser)
	if err != nil {
		return "", err
	}
	stmt, err := parser.Parse(filter)
	if err != nil {
		return "", err
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return "", fmt.Errorf("unrecognized statement: %s", filter)
	}
	addFilter(sel, *tenantClause)
	filter = sqlparser.String(sel)
	return filter, nil
}

func (ts *trafficSwitcher) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()
	// Source writes have been stopped, wait for all streams on targets to catch up.
	if err := ts.ForAllUIDs(func(target *MigrationTarget, uid int32) error {
		ts.Logger().Infof("Before Catchup: uid: %d, target primary %s, target position %s, shard %s", uid,
			topoproto.TabletAliasString(target.GetPrimary().GetAlias()), target.Position, target.GetShard().String())
		bls := target.Sources[uid]
		source := ts.Sources()[bls.Shard]
		ts.Logger().Infof("Before Catchup: waiting for keyspace:shard: %v:%v to reach source position %v, uid %d",
			ts.TargetKeyspaceName(), target.GetShard().ShardName(), source.Position, uid)
		if err := ts.TabletManagerClient().VReplicationWaitForPos(ctx, target.GetPrimary().Tablet, uid, source.Position); err != nil {
			return err
		}
		ts.Logger().Infof("After catchup: target keyspace:shard: %v:%v, source position %v, uid %d",
			ts.TargetKeyspaceName(), target.GetShard().ShardName(), source.Position, uid)
		ts.Logger().Infof("After catchup: position for keyspace:shard: %v:%v reached, uid %d",
			ts.TargetKeyspaceName(), target.GetShard().ShardName(), uid)
		if _, err := ts.TabletManagerClient().VReplicationExec(ctx, target.GetPrimary().Tablet, binlogplayer.StopVReplication(uid, "stopped for cutover")); err != nil {
			ts.Logger().Infof("Error marking stopped for cutover on %s, uid %d", topoproto.TabletAliasString(target.GetPrimary().GetAlias()), uid)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	// All targets have caught up, record their positions for setting up reverse workflows.
	return ts.ForAllTargets(func(target *MigrationTarget) error {
		var err error
		target.Position, err = ts.TabletManagerClient().PrimaryPosition(ctx, target.GetPrimary().Tablet)
		ts.Logger().Infof("After catchup, position for target primary %s, %v", topoproto.TabletAliasString(target.GetPrimary().GetAlias()), target.Position)
		return err
	})
}

func (ts *trafficSwitcher) stopSourceWrites(ctx context.Context) error {
	var err error
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		err = ts.switchDeniedTables(ctx, false)
	} else {
		err = ts.changeShardsAccess(ctx, ts.SourceKeyspaceName(), ts.SourceShards(), disallowWrites)
	}
	if err != nil {
		ts.Logger().Warningf("Error stopping writes on migration sources: %v", err)
		return err
	}
	return nil
}

// switchDeniedTables switches the denied tables rules for the traffic switch.
// They are added on the source side and removed on the target side.
// If backward is true, then we swap this logic, removing on the source side
// and adding on the target side. You would want to do that e.g. when canceling
// a failed (and currently partial) traffic switch as we may have already
// switched the denied tables entries and in any event we need to go back to
// the original state.
func (ts *trafficSwitcher) switchDeniedTables(ctx context.Context, backward bool) error {
	if ts.MigrationType() != binlogdatapb.MigrationType_TABLES {
		return nil
	}

	rmsource, rmtarget := false, true
	if backward {
		rmsource, rmtarget = true, false
	}

	egrp, ectx := errgroup.WithContext(ctx)
	egrp.Go(func() error {
		return ts.ForAllSources(func(source *MigrationSource) error {
			if _, err := ts.TopoServer().UpdateShardFields(ctx, ts.SourceKeyspaceName(), source.GetShard().ShardName(), func(si *topo.ShardInfo) error {
				return si.UpdateDeniedTables(ectx, topodatapb.TabletType_PRIMARY, nil, rmsource, ts.Tables())
			}); err != nil {
				return err
			}
			rtbsCtx, cancel := context.WithTimeout(ectx, shardTabletRefreshTimeout)
			defer cancel()
			isPartial, partialDetails, err := topotools.RefreshTabletsByShard(rtbsCtx, ts.TopoServer(), ts.TabletManagerClient(), source.GetShard(), nil, ts.Logger())
			if isPartial {
				msg := fmt.Sprintf("failed to successfully refresh all tablets in the %s/%s source shard (%v):\n  %v",
					source.GetShard().Keyspace(), source.GetShard().ShardName(), err, partialDetails)
				if ts.force {
					log.Warning(msg)
					return nil
				} else {
					return errors.New(msg)
				}
			}
			return err
		})
	})
	egrp.Go(func() error {
		return ts.ForAllTargets(func(target *MigrationTarget) error {
			if _, err := ts.TopoServer().UpdateShardFields(ectx, ts.TargetKeyspaceName(), target.GetShard().ShardName(), func(si *topo.ShardInfo) error {
				return si.UpdateDeniedTables(ctx, topodatapb.TabletType_PRIMARY, nil, rmtarget, ts.Tables())
			}); err != nil {
				return err
			}
			rtbsCtx, cancel := context.WithTimeout(ectx, shardTabletRefreshTimeout)
			defer cancel()
			isPartial, partialDetails, err := topotools.RefreshTabletsByShard(rtbsCtx, ts.TopoServer(), ts.TabletManagerClient(), target.GetShard(), nil, ts.Logger())
			if isPartial {
				msg := fmt.Sprintf("failed to successfully refresh all tablets in the %s/%s target shard (%v):\n  %v",
					target.GetShard().Keyspace(), target.GetShard().ShardName(), err, partialDetails)
				if ts.force {
					log.Warning(msg)
					return nil
				} else {
					return errors.New(msg)
				}
			}
			return err
		})
	})
	if err := egrp.Wait(); err != nil {
		ts.Logger().Warningf("Error in switchDeniedTables: %s", err)
		return err
	}

	return nil
}

// cancelMigration attempts to revert all changes made during the migration so that we can get back to the
// state when traffic switching (or reversing) was initiated.
func (ts *trafficSwitcher) cancelMigration(ctx context.Context, sm *StreamMigrator) error {
	var err error
	cancelErrs := &concurrency.AllErrorRecorder{}

	if ctx.Err() != nil {
		// Even though we create a new context later on we still record any context error:
		// for forensics in case of failures.
		ts.Logger().Infof("cancelMigration (%v): original context invalid: %s", ts.WorkflowName(), ctx.Err())
	}
	ts.Logger().Infof("cancelMigration (%v): starting", ts.WorkflowName())
	// We create a new context while canceling the migration, so that we are independent of the original
	// context being canceled prior to or during the cancel operation itself.
	// First we create a copy of the parent context, so that we maintain the locks, but which cannot be
	// canceled by the parent context.
	wcCtx := context.WithoutCancel(ctx)
	// Now we create a child context from that which has a timeout.
	cmTimeout := 2 * time.Minute
	cmCtx, cmCancel := context.WithTimeout(wcCtx, cmTimeout)
	defer cmCancel()

	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		if !ts.IsMultiTenantMigration() {
			ts.Logger().Infof("cancelMigration (%v): adding denied tables to target", ts.WorkflowName())
			err = ts.switchDeniedTables(cmCtx, true /* revert */)
		} else {
			ts.Logger().Infof("cancelMigration (%v): multi-tenant, not adding denied tables to target", ts.WorkflowName())
		}
	} else {
		ts.Logger().Infof("cancelMigration (%v): allowing writes on source shards", ts.WorkflowName())
		err = ts.changeShardsAccess(cmCtx, ts.SourceKeyspaceName(), ts.SourceShards(), allowWrites)
	}
	if err != nil {
		cancelErrs.RecordError(fmt.Errorf("could not revert denied tables / shard access: %v", err))
		ts.Logger().Errorf("Cancel migration failed (%v): could not revert denied tables / shard access: %v", ts.WorkflowName(), err)
	}

	if err := sm.CancelStreamMigrations(cmCtx); err != nil {
		cancelErrs.RecordError(fmt.Errorf("could not cancel stream migrations: %v", err))
		ts.Logger().Errorf("Cancel migration failed (%v): could not cancel stream migrations: %v", ts.WorkflowName(), err)
	}

	ts.Logger().Infof("cancelMigration (%v): restarting vreplication workflows", ts.WorkflowName())
	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s",
			encodeString(target.GetPrimary().DbName()), encodeString(ts.WorkflowName()))
		_, err := ts.TabletManagerClient().VReplicationExec(cmCtx, target.GetPrimary().Tablet, query)
		return err
	})
	if err != nil {
		cancelErrs.RecordError(fmt.Errorf("could not restart vreplication: %v", err))
		ts.Logger().Errorf("Cancel migration failed (%v): could not restart vreplication: %v", ts.WorkflowName(), err)
	}

	ts.Logger().Infof("cancelMigration (%v): deleting reverse vreplication workflows", ts.WorkflowName())
	if err := ts.deleteReverseVReplication(cmCtx); err != nil {
		cancelErrs.RecordError(fmt.Errorf("could not delete reverse vreplication streams: %v", err))
		ts.Logger().Errorf("Cancel migration failed (%v): could not delete reverse vreplication streams: %v", ts.WorkflowName(), err)
	}

	if cancelErrs.HasErrors() {
		ts.Logger().Errorf("Cancel migration failed for %v, manual cleanup work may be necessary: %v", ts.WorkflowName(), cancelErrs.AggrError(vterrors.Aggregate))
		return vterrors.Wrap(cancelErrs.AggrError(vterrors.Aggregate), "cancel migration failed, manual cleanup work may be necessary")
	}

	ts.Logger().Infof("cancelMigration (%v): completed", ts.WorkflowName())
	return nil
}

func (ts *trafficSwitcher) freezeTargetVReplication(ctx context.Context) error {
	// Mark target streams as frozen before deleting. If SwitchWrites gets
	// re-invoked after a freeze, it will skip all the previous steps
	err := ts.ForAllTargets(func(target *MigrationTarget) error {
		ts.Logger().Infof("Marking target streams frozen for workflow %s db_name %s", ts.WorkflowName(), target.GetPrimary().DbName())
		query := fmt.Sprintf("update _vt.vreplication set message = %s where db_name=%s and workflow=%s", encodeString(Frozen),
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
			// vreplication.exec returns no error on delete if the rows do not exist.
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
			// vreplication.exec returns no error on delete if the rows do not exist.
			return err
		}
		ts.ws.deleteWorkflowVDiffData(ctx, source.GetPrimary().Tablet, ReverseWorkflowName(ts.WorkflowName()))
		ts.ws.optimizeCopyStateTable(source.GetPrimary().Tablet)
		return nil
	})
}

func (ts *trafficSwitcher) removeTargetTables(ctx context.Context) error {
	switch ts.MigrationType() {
	case binlogdatapb.MigrationType_TABLES:
		err := ts.ForAllTargets(func(target *MigrationTarget) error {
			ts.Logger().Infof("ForAllTargets: %+v", target)
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
					topoproto.TabletAliasString(target.GetPrimary().GetAlias()), target.GetPrimary().DbName(), tableName)
				res, err := ts.ws.tmc.ExecuteFetchAsDba(ctx, target.GetPrimary().Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
					Query:                   []byte(query),
					MaxRows:                 1,
					ReloadSchema:            true,
					DisableForeignKeyChecks: true,
				})
				ts.Logger().Infof("Removed target table with result: %+v", res)
				if err != nil {
					if IsTableDidNotExistError(err) {
						// The table was already gone, so we can ignore the error.
						ts.Logger().Warningf("%s: Table %s did not exist when attempting to remove it", topoproto.TabletAliasString(target.GetPrimary().GetAlias()), tableName)
					} else {
						ts.Logger().Errorf("%s: Error removing table %s: %v", topoproto.TabletAliasString(target.GetPrimary().GetAlias()), tableName, err)
						return err
					}
				}
				ts.Logger().Infof("%s: Removed table %s.%s\n",
					topoproto.TabletAliasString(target.GetPrimary().GetAlias()), target.GetPrimary().DbName(), tableName)

			}
			return nil
		})
		if err != nil {
			return err
		}

		// Remove the tables from the vschema.
		return ts.dropParticipatingTablesFromKeyspace(ctx, ts.TargetKeyspaceName())

	case binlogdatapb.MigrationType_SHARDS:
		// For reshard streams, do the following:
		// * get the schema definition from one of the source primaries to
		//   determine which tables to drop.
		// * drop the tables on each of the target shard's primaries
		// * do not remove the tables from the vschema
		oneSource := ts.SourceShards()[0].PrimaryAlias

		// Get the schema definition from the target primary. We only want to drop tables
		// that match the vreplication filters.
		req := &tabletmanagerdatapb.GetSchemaRequest{Tables: ts.Tables(), ExcludeTables: nil, IncludeViews: false}
		sd, err := schematools.GetSchema(ctx, ts.TopoServer(), ts.ws.tmc, oneSource, req)
		if err != nil {
			return err
		}

		err = ts.ForAllTargets(func(target *MigrationTarget) error {
			primaryDbName, err := sqlescape.EnsureEscaped(target.GetPrimary().DbName())
			if err != nil {
				return err
			}

			for _, td := range sd.TableDefinitions {
				if schema.IsInternalOperationTableName(td.Name) {
					continue
				}

				tableName, err := sqlescape.EnsureEscaped(td.Name)
				if err != nil {
					return err
				}

				var query string

				if td.Type == tmutils.TableView {
					query = fmt.Sprintf("drop view %s.%s", primaryDbName, tableName)
					ts.Logger().Infof("%s: Dropping view %s.%s\n",
						topoproto.TabletAliasString(target.GetPrimary().GetAlias()), target.GetPrimary().DbName(), tableName)

					res, err := ts.ws.tmc.ExecuteFetchAsDba(ctx, target.GetPrimary().Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
						Query:                   []byte(query),
						MaxRows:                 1,
						ReloadSchema:            true,
						DisableForeignKeyChecks: true,
					})

					ts.Logger().Infof("Removed target view with result: %+v", res)
					if err != nil {
						if IsTableDidNotExistError(err) {
							// The view was already gone, so we can ignore the error.
							ts.Logger().Warningf("%s: view %s did not exist when attempting to remove it", topoproto.TabletAliasString(target.GetPrimary().GetAlias()), tableName)
						} else {
							ts.Logger().Errorf("%s: Error removing view %s: %v", topoproto.TabletAliasString(target.GetPrimary().GetAlias()), tableName, err)
							return err
						}
					}
					ts.Logger().Infof("%s: Removed view %s.%s\n",
						topoproto.TabletAliasString(target.GetPrimary().GetAlias()), target.GetPrimary().DbName(), tableName)

				} else {
					query = fmt.Sprintf("drop table %s.%s", primaryDbName, tableName)
					ts.Logger().Infof("%s: Dropping table %s.%s\n",
						topoproto.TabletAliasString(target.GetPrimary().GetAlias()), target.GetPrimary().DbName(), tableName)

					res, err := ts.ws.tmc.ExecuteFetchAsDba(ctx, target.GetPrimary().Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
						Query:                   []byte(query),
						MaxRows:                 1,
						ReloadSchema:            true,
						DisableForeignKeyChecks: true,
					})

					ts.Logger().Infof("Removed target table with result: %+v", res)
					if err != nil {
						if IsTableDidNotExistError(err) {
							// The table was already gone, so we can ignore the error.
							ts.Logger().Warningf("%s: Table %s did not exist when attempting to remove it", topoproto.TabletAliasString(target.GetPrimary().GetAlias()), tableName)
						} else {
							ts.Logger().Errorf("%s: Error removing table %s: %v", topoproto.TabletAliasString(target.GetPrimary().GetAlias()), tableName, err)
							return err
						}
					}
					ts.Logger().Infof("%s: Removed table %s.%s\n",
						topoproto.TabletAliasString(target.GetPrimary().GetAlias()), target.GetPrimary().DbName(), tableName)
				}
			}

			return nil
		})

		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown migration type: %v", ts.MigrationType())
	}

	return nil
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
		if ts.isPartialMigration ||
			(ts.IsMultiTenantMigration() && ts.options != nil && len(ts.options.GetShards()) > 0) {
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

// gatherSourcePositions will get the current replication position for all
// migration sources.
func (ts *trafficSwitcher) gatherSourcePositions(ctx context.Context) error {
	return ts.ForAllSources(func(source *MigrationSource) error {
		var err error
		tablet := source.GetPrimary().Tablet
		tabletAlias := topoproto.TabletAliasString(tablet.Alias)
		source.Position, err = ts.TabletManagerClient().PrimaryPosition(ctx, tablet)
		if err != nil {
			ts.Logger().Errorf("Error getting migration source position on %s: %s", tabletAlias, err)
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get position on migration source %s: %v",
				tabletAlias, err)
		}
		ts.Logger().Infof("Position on migration source %s after having stopped writes: %s", tabletAlias, source.Position)
		return nil
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
	tablesFound := make(map[string]struct{}) // Used to short circuit the search
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
			// The table name can be escaped in the vschema definition.
			unescapedTableName, err := sqlescape.UnescapeID(tableName)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid table name %q in keyspace %s: %v",
					tableName, keyspace, err)
			}
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
				sm := sequencesByBackingTable[unescapedTableName]
				if tableDef != nil && tableDef.Type == vindexes.TypeSequence &&
					sm != nil && unescapedTableName == sm.backingTableName {
					tablesFound[tableName] = struct{}{} // This is also protected by the mutex
					sm.backingTableKeyspace = keyspace
					// Set the default keyspace name. We will later check to
					// see if the tablet we send requests to is using a dbname
					// override and use that if it is.
					sm.backingTableDBName = "vt_" + keyspace
					if len(tablesFound) == tableCount { // Short circuit the search
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
		// The keyspace name could be escaped so we need to unescape it.
		ks, err := sqlescape.UnescapeID(keyspace)
		if err != nil { // Should never happen
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid keyspace name %q: %v", keyspace, err)
		}
		searchGroup.Go(func() error {
			return searchKeyspace(gctx, searchCompleted, ks)
		})
	}
	if err := searchGroup.Wait(); err != nil {
		return nil, err
	}

	if len(tablesFound) != tableCount {
		// Try and create the missing backing sequence tables if we can.
		if err := ts.createMissingSequenceTables(ctx, sequencesByBackingTable, tablesFound); err != nil {
			return nil, err
		}
	}

	return sequencesByBackingTable, nil
}

// createMissingSequenceTables will create the backing sequence tables for those that
// could not be found in any current keyspace.
func (ts trafficSwitcher) createMissingSequenceTables(ctx context.Context, sequencesByBackingTable map[string]*sequenceMetadata, tablesFound map[string]struct{}) error {
	globalKeyspace := ts.options.GetGlobalKeyspace()
	if globalKeyspace == "" {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to locate all of the backing sequence tables being used and no global-keyspace was provided to auto create them in: %s",
			strings.Join(maps.Keys(sequencesByBackingTable), ","))
	}
	shards, err := ts.ws.ts.GetShardNames(ctx, globalKeyspace)
	if err != nil {
		return err
	}
	if len(shards) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "global-keyspace %s is not unsharded", globalKeyspace)
	}
	globalVSchema, err := ts.ws.ts.GetVSchema(ctx, globalKeyspace)
	if err != nil {
		return err
	}
	updatedGlobalVSchema := false
	for tableName, sequenceMetadata := range sequencesByBackingTable {
		if _, ok := tablesFound[tableName]; !ok {
			// Create the backing table.
			shard, err := ts.ws.ts.GetShard(ctx, globalKeyspace, shards[0])
			if err != nil {
				return err
			}
			if shard.PrimaryAlias == nil {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "global-keyspace %s does not currently have a primary tablet",
					globalKeyspace)
			}
			primary, err := ts.ws.ts.GetTablet(ctx, shard.PrimaryAlias)
			if err != nil {
				return err
			}
			escapedTableName, err := sqlescape.EnsureEscaped(tableName)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid table name %s: %v",
					tableName, err)
			}
			stmt := sqlparser.BuildParsedQuery(sqlCreateSequenceTable, escapedTableName)
			_, err = ts.ws.tmc.ApplySchema(ctx, primary.Tablet, &tmutils.SchemaChange{
				SQL:                     stmt.Query,
				Force:                   false,
				AllowReplication:        true,
				SQLMode:                 vreplication.SQLMode,
				DisableForeignKeyChecks: true,
			})
			if err != nil {
				return vterrors.Wrapf(err, "failed to create sequence backing table %s in global-keyspace %s",
					tableName, globalKeyspace)
			}
			if bt := globalVSchema.Tables[sequenceMetadata.backingTableName]; bt == nil {
				if globalVSchema.Tables == nil {
					globalVSchema.Tables = make(map[string]*vschemapb.Table)
				}
				globalVSchema.Tables[tableName] = &vschemapb.Table{
					Type: vindexes.TypeSequence,
				}
				updatedGlobalVSchema = true
				sequenceMetadata.backingTableDBName = "vt_" + globalKeyspace // This will be overridden later if needed
				sequenceMetadata.backingTableKeyspace = globalKeyspace
			}
		}
	}
	if updatedGlobalVSchema {
		err = ts.ws.ts.SaveVSchema(ctx, globalKeyspace, globalVSchema)
		if err != nil {
			return vterrors.Wrapf(err, "failed to update vschema in the global-keyspace %s", globalKeyspace)
		}
	}
	return nil
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

	for _, table := range ts.tables {
		seqTable, ok := vschema.Tables[table]
		if !ok || seqTable.GetAutoIncrement().GetSequence() == "" {
			continue
		}
		// Be sure that the table name is unescaped as it can be escaped
		// in the vschema.
		unescapedTable, err := sqlescape.UnescapeID(table)
		if err != nil {
			return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid table name %q defined in the sequence table %+v: %v",
				table, seqTable, err)
		}
		sm := &sequenceMetadata{
			usingTableName:   unescapedTable,
			usingTableDBName: targetDBName,
		}
		// If the sequence table is fully qualified in the vschema then
		// we don't need to find it later.
		if strings.Contains(seqTable.AutoIncrement.Sequence, ".") {
			keyspace, tableName, found := strings.Cut(seqTable.AutoIncrement.Sequence, ".")
			if !found {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence table name %q defined in the %s keyspace",
					seqTable.AutoIncrement.Sequence, ts.targetKeyspace)
			}
			// Unescape the table name and keyspace name as they may be escaped in the
			// vschema definition if they e.g. contain dashes.
			if keyspace, err = sqlescape.UnescapeID(keyspace); err != nil {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid keyspace in qualified sequence table name %q defined in sequence table %+v: %v",
					seqTable.AutoIncrement.Sequence, seqTable, err)
			}
			if tableName, err = sqlescape.UnescapeID(tableName); err != nil {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid qualified sequence table name %q defined in sequence table %+v: %v",
					seqTable.AutoIncrement.Sequence, seqTable, err)
			}
			sm.backingTableKeyspace = keyspace
			sm.backingTableName = tableName
			// Update the definition with the unescaped values.
			seqTable.AutoIncrement.Sequence = fmt.Sprintf("%s.%s", keyspace, tableName)
			// Set the default keyspace name. We will later check to
			// see if the tablet we send requests to is using a dbname
			// override and use that if it is.
			sm.backingTableDBName = "vt_" + keyspace
		} else {
			sm.backingTableName, err = sqlescape.UnescapeID(seqTable.AutoIncrement.Sequence)
			if err != nil {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence table name %q defined in sequence table %+v: %v",
					seqTable.AutoIncrement.Sequence, seqTable, err)
			}
			seqTable.AutoIncrement.Sequence = sm.backingTableName
			allFullyQualified = false
		}
		// The column names can be escaped in the vschema definition.
		for i := range seqTable.ColumnVindexes {
			var (
				unescapedColumn string
				err             error
			)
			if len(seqTable.ColumnVindexes[i].Columns) > 0 {
				for n := range seqTable.ColumnVindexes[i].Columns {
					unescapedColumn, err = sqlescape.UnescapeID(seqTable.ColumnVindexes[i].Columns[n])
					seqTable.ColumnVindexes[i].Columns[n] = unescapedColumn
				}
			} else {
				// This is the legacy vschema definition.
				unescapedColumn, err = sqlescape.UnescapeID(seqTable.ColumnVindexes[i].Column)
				seqTable.ColumnVindexes[i].Column = unescapedColumn
			}
			if err != nil {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence column vindex name %q defined in sequence table %+v: %v",
					seqTable.ColumnVindexes[i].Column, seqTable, err)
			}
		}
		unescapedAutoIncCol, err := sqlescape.UnescapeID(seqTable.AutoIncrement.Column)
		if err != nil {
			return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid auto-increment column name %q defined in sequence table %+v: %v",
				seqTable.AutoIncrement.Column, seqTable, err)
		}
		seqTable.AutoIncrement.Column = unescapedAutoIncCol
		sm.usingTableDefinition = seqTable
		sequencesByBackingTable[sm.backingTableName] = sm
	}

	return sequencesByBackingTable, allFullyQualified, nil
}

// initializeTargetSequences initializes the backing sequence tables
// using a map keyed by the backing sequence table name.
//
// The backing tables must have already been created, unless a default
// global keyspace exists for the trafficSwitcher -- in which case we
// will create the backing table there if needed.

// This function will then ensure that the next value is set to a value
// greater than any currently stored in the using table on the target
// keyspace. If the backing table is updated to a new higher value then
// it will also tell the primary tablet serving the sequence to
// refresh/reset its cache to be sure that it does not provide a value
// that is less than the current max.
func (ts *trafficSwitcher) initializeTargetSequences(ctx context.Context, sequencesByBackingTable map[string]*sequenceMetadata) error {
	initSequenceTable := func(ictx context.Context, sequenceMetadata *sequenceMetadata) error {
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
			usingCol, err := sqlescape.EnsureEscaped(sequenceMetadata.usingTableDefinition.AutoIncrement.Column)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid column name %s specified for sequence in table %s: %v",
					sequenceMetadata.usingTableDefinition.AutoIncrement.Column, sequenceMetadata.usingTableName, err)
			}
			usingDB, err := sqlescape.EnsureEscaped(sequenceMetadata.usingTableDBName)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid database name %s specified for sequence in table %s: %v",
					sequenceMetadata.usingTableDBName, sequenceMetadata.usingTableName, err)
			}
			usingTable, err := sqlescape.EnsureEscaped(sequenceMetadata.usingTableName)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence table name specified for sequence in table %s: %v",
					sequenceMetadata.usingTableName, err)
			}
			query := sqlparser.BuildParsedQuery(sqlGetMaxSequenceVal,
				usingCol,
				usingDB,
				usingTable,
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
		backingDB, err := sqlescape.EnsureEscaped(sequenceMetadata.backingTableDBName)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid database name %s in sequence backing table %s: %v",
				sequenceMetadata.backingTableDBName, sequenceMetadata.backingTableName, err)
		}
		backingTable, err := sqlescape.EnsureEscaped(sequenceMetadata.backingTableName)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence backing table name %s: %v",
				sequenceMetadata.backingTableName, err)
		}
		query := sqlparser.BuildParsedQuery(sqlInitSequenceTable,
			backingDB,
			backingTable,
			nextVal,
			nextVal,
			nextVal,
		)
		// Now execute this on the primary tablet of the unsharded keyspace
		// housing the backing table.
	initialize:
		qr, ierr := ts.ws.tmc.ExecuteFetchAsApp(ictx, sequenceTablet.Tablet, true, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
			Query:   []byte(query.Query),
			MaxRows: 1,
		})
		if ierr != nil {
			vterr := vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to initialize the backing sequence table %s.%s: %v",
				sequenceMetadata.backingTableDBName, sequenceMetadata.backingTableName, ierr)
			// If the sequence table doesn't exist, let's try and create it, otherwise
			// return the error.
			if sqlErr, ok := sqlerror.NewSQLErrorFromError(ierr).(*sqlerror.SQLError); !ok ||
				(sqlErr.Num != sqlerror.ERNoSuchTable && sqlErr.Num != sqlerror.ERBadTable) {
				return vterr
			}
			stmt := sqlparser.BuildParsedQuery(sqlCreateSequenceTable, backingTable)
			_, ierr = ts.ws.tmc.ApplySchema(ctx, sequenceTablet.Tablet, &tmutils.SchemaChange{
				SQL:                     stmt.Query,
				Force:                   false,
				AllowReplication:        true,
				SQLMode:                 vreplication.SQLMode,
				DisableForeignKeyChecks: true,
			})
			if ierr != nil {
				return vterrors.Wrapf(vterr, "could not create missing sequence table: %v", err)
			}
			select {
			case <-ctx.Done():
				return vterrors.Wrapf(vterr, "could not create missing sequence table: %v", ctx.Err())
			default:
				goto initialize
			}
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
		// ResetSequences interfaces with the schema engine and the actual
		// table identifiers DO NOT contain the backticks. So we have to
		// ensure that the table name is unescaped.
		unescapedBackingTable, err := sqlescape.UnescapeID(backingTable)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence backing table name %s: %v", backingTable, err)
		}
		ierr = ts.TabletManagerClient().ResetSequences(ictx, ti.Tablet, []string{unescapedBackingTable})
		if ierr != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to reset the sequence cache for backing table %s on shard %s/%s using tablet %s: %v",
				sequenceMetadata.backingTableName, sequenceShard.Keyspace(), sequenceShard.ShardName(), sequenceShard.PrimaryAlias, ierr)
		}
		return nil
	}

	initGroup, gctx := errgroup.WithContext(ctx)
	for _, sequenceMetadata := range sequencesByBackingTable {
		initGroup.Go(func() error {
			return initSequenceTable(gctx, sequenceMetadata)
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
			source.GetShard().Keyspace(), source.GetShard().ShardName(), topoproto.TabletAliasString(source.GetPrimary().GetAlias()))
		return ts.TabletManagerClient().ResetSequences(ctx, source.GetPrimary().Tablet, ts.Tables())
	})
}

func (ts *trafficSwitcher) IsMultiTenantMigration() bool {
	if ts.options != nil && ts.options.TenantId != "" {
		return true
	}
	return false
}

func (ts *trafficSwitcher) mirrorTableTraffic(ctx context.Context, types []topodatapb.TabletType, percent float32) error {
	mrs, err := topotools.GetMirrorRules(ctx, ts.TopoServer())
	if err != nil {
		return err
	}

	var numExisting int
	for _, table := range ts.tables {
		for _, tabletType := range types {
			fromTable := fmt.Sprintf("%s.%s", ts.SourceKeyspaceName(), table)
			if tabletType != topodatapb.TabletType_PRIMARY {
				fromTable = fmt.Sprintf("%s@%s", fromTable, topoproto.TabletTypeLString(tabletType))
			}
			toTable := fmt.Sprintf("%s.%s", ts.TargetKeyspaceName(), table)

			if _, ok := mrs[fromTable]; !ok {
				mrs[fromTable] = make(map[string]float32)
			}

			if _, ok := mrs[fromTable][toTable]; ok {
				numExisting++
			}

			if percent == 0 {
				// When percent is 0, remove mirror rule if it exists.
				if _, ok := mrs[fromTable][toTable]; ok {
					delete(mrs, fromTable)
				}
			} else {
				mrs[fromTable][toTable] = percent
			}
		}
	}

	if numExisting > 0 && numExisting != (len(types)*len(ts.tables)) {
		return vterrors.Errorf(vtrpcpb.Code_ALREADY_EXISTS, "wrong number of pre-existing mirror rules")
	}

	if err := topotools.SaveMirrorRules(ctx, ts.TopoServer(), mrs); err != nil {
		return err
	}

	return ts.TopoServer().RebuildSrvVSchema(ctx, nil)
}
