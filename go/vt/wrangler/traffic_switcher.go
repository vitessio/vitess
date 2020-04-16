/*
Copyright 2020 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/log"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
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
	frozenStr = "FROZEN"
)

// TrafficSwitchDirection specifies the switching direction.
type TrafficSwitchDirection int

// The following constants define the switching direction.
const (
	DirectionForward = TrafficSwitchDirection(iota)
	DirectionBackward
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
	sources         map[string]*tsSource
	targets         map[string]*tsTarget
	sourceKeyspace  string
	targetKeyspace  string
	tables          []string
	sourceKSSchema  *vindexes.KeyspaceSchema
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

// SwitchReads is a generic way of switching read traffic for a resharding workflow.
func (wr *Wrangler) SwitchReads(ctx context.Context, targetKeyspace, workflow string, servedType topodatapb.TabletType, cells []string, direction TrafficSwitchDirection, dryRun bool) (*[]string, error) {
	if servedType != topodatapb.TabletType_REPLICA && servedType != topodatapb.TabletType_RDONLY {
		return nil, fmt.Errorf("tablet type must be REPLICA or RDONLY: %v", servedType)
	}
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, err
	}

	var sw switcher
	if dryRun {
		sw = &switchDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switchForReal{ts: ts, wr: wr}
	}

	if ts.frozen {
		return nil, fmt.Errorf("cannot switch reads while SwitchWrites is in progress")
	}
	if err := ts.validate(ctx, false /* isWrite */); err != nil {
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
		if err := sw.switchTableReads(ctx, cells, servedType, direction); err != nil {
			ts.wr.Logger().Errorf("switchTableReads failed: %v", err)
			return nil, err
		}
		return sw.logs(), nil
	}
	if err := ts.switchShardReads(ctx, cells, servedType, direction); err != nil {
		ts.wr.Logger().Errorf("switchShardReads failed: %v", err)
		return nil, err
	}
	return sw.logs(), nil
}

// SwitchWrites is a generic way of migrating write traffic for a resharding workflow.
func (wr *Wrangler) SwitchWrites(ctx context.Context, targetKeyspace, workflow string, filteredReplicationWaitTime time.Duration, cancelMigrate, reverseReplication bool, dryRun bool) (journalID int64, dryRunResults *[]string, err error) {
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return 0, nil, err
	}

	var sw switcher
	if dryRun {
		sw = &switchDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switchForReal{ts: ts, wr: wr}
	}

	if ts.frozen {
		ts.wr.Logger().Warningf("Writes have already been switched for workflow %s, nothing to do here", ts.workflow)
		return 0, sw.logs(), nil
	}

	ts.wr.Logger().Infof("Built switching metadata: %+v", ts)
	if err := ts.validate(ctx, true /* isWrite */); err != nil {
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
		sm, err := buildStreamMigrater(ctx, ts, cancelMigrate)
		if err != nil {
			ts.wr.Logger().Errorf("buildStreamMigrater failed: %v", err)
			return 0, nil, err
		}
		if cancelMigrate {
			sw.cancelMigration(ctx, sm)
			return 0, sw.logs(), nil
		}
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
		if err := sw.stopSourceWrites(ctx); err != nil {
			ts.wr.Logger().Errorf("stopSourceWrites failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		if err := sw.waitForCatchup(ctx, filteredReplicationWaitTime); err != nil {
			ts.wr.Logger().Errorf("waitForCatchup failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		if err := sw.migrateStreams(ctx, sm); err != nil {
			ts.wr.Logger().Errorf("migrateStreams failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		if err := sw.createReverseVReplication(ctx); err != nil {
			ts.wr.Logger().Errorf("createReverseVReplication failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}
	} else {
		if cancelMigrate {
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

// DropSources cleans up source tables, shards and blacklisted tables after a MoveTables/Reshard is completed
func (wr *Wrangler) DropSources(ctx context.Context, targetKeyspace, workflow string, dryRun bool) (*[]string, error) {
	fmt.Printf("DropSources: %s.%s\n", targetKeyspace, workflow)
	ts, err := wr.buildTrafficSwitcher(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, err
	}
	var sw switcher
	if dryRun {
		sw = &switchDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switchForReal{ts: ts, wr: wr}
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
	if err := sw.validateWorkflowHasCompleted(ctx); err != nil {
		wr.Logger().Errorf("Workflow has not completed, cannot DropSources\n%v\n", err)
		return nil, err
	}
	switch ts.migrationType {
	case binlogdatapb.MigrationType_TABLES:
		sw.dropSourceTables(ctx)
	case binlogdatapb.MigrationType_SHARDS:
		sw.dropSourceShards(ctx)
	}

	sw.dropSourceBlacklistedTables(ctx)
	sw.dropTargetVReplicationStreams(ctx)
	return sw.logs(), nil
}

func (wr *Wrangler) buildTrafficSwitcher(ctx context.Context, targetKeyspace, workflow string) (*trafficSwitcher, error) {
	targets, frozen, err := wr.buildTargets(ctx, targetKeyspace, workflow)
	if err != nil {
		return nil, err
	}

	ts := &trafficSwitcher{
		wr:              wr,
		workflow:        workflow,
		reverseWorkflow: reverseName(workflow),
		id:              hashStreams(targetKeyspace, targets),
		targets:         targets,
		sources:         make(map[string]*tsSource),
		targetKeyspace:  targetKeyspace,
		frozen:          frozen,
	}
	ts.wr.Logger().Infof("Migration ID for workflow %s: %d", workflow, ts.id)

	// Build the sources
	for _, target := range targets {
		for _, bls := range target.sources {
			if ts.sourceKeyspace == "" {
				ts.sourceKeyspace = bls.Keyspace
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
			sourcesi, err := ts.wr.ts.GetShard(ctx, bls.Keyspace, bls.Shard)
			if err != nil {
				return nil, err
			}
			sourceMaster, err := ts.wr.ts.GetTablet(ctx, sourcesi.MasterAlias)
			if err != nil {
				return nil, err
			}
			ts.sources[bls.Shard] = &tsSource{
				si:     sourcesi,
				master: sourceMaster,
			}
		}
	}
	if ts.sourceKeyspace != ts.targetKeyspace {
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
	vs, err := ts.wr.ts.GetVSchema(ctx, ts.sourceKeyspace)
	if err != nil {
		return nil, err
	}
	ts.sourceKSSchema, err = vindexes.BuildKeyspaceSchema(vs, ts.sourceKeyspace)
	if err != nil {
		return nil, err
	}
	return ts, nil
}

func (wr *Wrangler) buildTargets(ctx context.Context, targetKeyspace, workflow string) (targets map[string]*tsTarget, frozen bool, err error) {
	targets = make(map[string]*tsTarget)
	targetShards, err := wr.ts.GetShardNames(ctx, targetKeyspace)
	if err != nil {
		return nil, false, err
	}
	// We check all target shards. All of them may not have a stream.
	// For example, if we're splitting -80 to -40,40-80, only those
	// two target shards will have vreplication streams.
	for _, targetShard := range targetShards {
		targetsi, err := wr.ts.GetShard(ctx, targetKeyspace, targetShard)
		if err != nil {
			return nil, false, err
		}
		if targetsi.MasterAlias == nil {
			// This can happen if bad inputs are given.
			return nil, false, fmt.Errorf("shard %v:%v doesn't have a master set", targetKeyspace, targetShard)
		}
		targetMaster, err := wr.ts.GetTablet(ctx, targetsi.MasterAlias)
		if err != nil {
			return nil, false, err
		}
		p3qr, err := wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, fmt.Sprintf("select id, source, message from _vt.vreplication where workflow=%s and db_name=%s", encodeString(workflow), encodeString(targetMaster.DbName())))
		if err != nil {
			return nil, false, err
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
				return nil, false, err
			}

			var bls binlogdatapb.BinlogSource
			if err := proto.UnmarshalText(row[1].ToString(), &bls); err != nil {
				return nil, false, err
			}
			targets[targetShard].sources[uint32(id)] = &bls

			if row[2].ToString() == frozenStr {
				frozen = true
			}
		}
	}
	if len(targets) == 0 {
		return nil, false, fmt.Errorf("no streams found in keyspace %s for: %s", targetKeyspace, workflow)
	}
	return targets, frozen, nil
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

func (ts *trafficSwitcher) validate(ctx context.Context, isWrite bool) error {
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		// All shards must be present.
		if err := ts.compareShards(ctx, ts.sourceKeyspace, ts.sourceShards()); err != nil {
			return err
		}
		if err := ts.compareShards(ctx, ts.targetKeyspace, ts.targetShards()); err != nil {
			return err
		}
		// Wildcard table names not allowed.
		for _, table := range ts.tables {
			if strings.HasPrefix(table, "/") {
				return fmt.Errorf("cannot migrate streams with wild card table names: %v", table)
			}
		}
		if isWrite {
			return ts.validateTableForWrite(ctx)
		}
	} else { // binlogdatapb.MigrationType_SHARDS
		if isWrite {
			return ts.validateShardForWrite(ctx)
		}
	}
	return nil
}

func (ts *trafficSwitcher) validateTableForWrite(ctx context.Context) error {
	rules, err := ts.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	for _, table := range ts.tables {
		for _, tabletType := range []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY} {
			tt := strings.ToLower(tabletType.String())
			if rules[table+"@"+tt] == nil || rules[ts.targetKeyspace+"."+table+"@"+tt] == nil {
				return fmt.Errorf("missing tablet type specific routing, read-only traffic must be switched before switching writes: %v", table)
			}
		}
	}
	return nil
}

func (ts *trafficSwitcher) validateShardForWrite(ctx context.Context) error {
	srvKeyspaces, err := ts.wr.ts.GetSrvKeyspaceAllCells(ctx, ts.sourceKeyspace)
	if err != nil {
		return err
	}

	// Checking one shard is enough.
	var si *topo.ShardInfo
	for _, source := range ts.sources {
		si = source.si
		break
	}

	for _, srvKeyspace := range srvKeyspaces {
		var shardServedTypes []string
		for _, partition := range srvKeyspace.GetPartitions() {
			if partition.GetServedType() == topodatapb.TabletType_MASTER {
				continue
			}
			for _, shardReference := range partition.GetShardReferences() {
				if key.KeyRangeEqual(shardReference.GetKeyRange(), si.GetKeyRange()) {
					shardServedTypes = append(shardServedTypes, partition.GetServedType().String())
				}
			}
		}
		if len(shardServedTypes) > 0 {
			return fmt.Errorf("cannot switch MASTER away from %v/%v until everything else is switched. Make sure that the following types are switched first: %v", si.Keyspace(), si.ShardName(), strings.Join(shardServedTypes, ", "))
		}
	}
	return nil
}

func (ts *trafficSwitcher) compareShards(ctx context.Context, keyspace string, sis []*topo.ShardInfo) error {
	var shards []string
	for _, si := range sis {
		shards = append(shards, si.ShardName())
	}
	topoShards, err := ts.wr.ts.GetShardNames(ctx, keyspace)
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

func (ts *trafficSwitcher) switchTableReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error {
	rules, err := ts.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	// We assume that the following rules were setup when the targets were created:
	// table -> sourceKeyspace.table
	// targetKeyspace.table -> sourceKeyspace.table
	// For forward migration, we add tablet type specific rules to redirect traffic to the target.
	// For backward, we delete them.
	tt := strings.ToLower(servedType.String())
	for _, table := range ts.tables {
		if direction == DirectionForward {
			rules[table+"@"+tt] = []string{ts.targetKeyspace + "." + table}
			rules[ts.targetKeyspace+"."+table+"@"+tt] = []string{ts.targetKeyspace + "." + table}
			rules[ts.sourceKeyspace+"."+table+"@"+tt] = []string{ts.targetKeyspace + "." + table}
		} else {
			delete(rules, table+"@"+tt)
			delete(rules, ts.targetKeyspace+"."+table+"@"+tt)
			delete(rules, ts.sourceKeyspace+"."+table+"@"+tt)
		}
	}
	if err := ts.wr.saveRoutingRules(ctx, rules); err != nil {
		return err
	}
	return ts.wr.ts.RebuildSrvVSchema(ctx, cells)
}

func (ts *trafficSwitcher) switchShardReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error {
	var fromShards, toShards []*topo.ShardInfo
	if direction == DirectionForward {
		fromShards, toShards = ts.sourceShards(), ts.targetShards()
	} else {
		fromShards, toShards = ts.targetShards(), ts.sourceShards()
	}

	if err := ts.wr.updateShardRecords(ctx, ts.sourceKeyspace, fromShards, cells, servedType, true /* isFrom */, false /* clearSourceShards */); err != nil {
		return err
	}
	if err := ts.wr.updateShardRecords(ctx, ts.sourceKeyspace, toShards, cells, servedType, false, false); err != nil {
		return err
	}
	return ts.wr.ts.MigrateServedType(ctx, ts.sourceKeyspace, toShards, fromShards, servedType, cells)
}

// checkJournals returns true if at least one journal has been created.
// If so, it also returns the list of sourceWorkflows that need to be switched.
func (ts *trafficSwitcher) checkJournals(ctx context.Context) (journalsExist bool, sourceWorkflows []string, err error) {
	var mu sync.Mutex
	journal := &binlogdatapb.Journal{}
	var exists bool
	err = ts.forAllSources(func(source *tsSource) error {
		statement := fmt.Sprintf("select val from _vt.resharding_journal where id=%v", ts.id)
		p3qr, err := ts.wr.tmc.VReplicationExec(ctx, source.master.Tablet, statement)
		if err != nil {
			return err
		}
		if len(p3qr.Rows) != 0 {
			qr := sqltypes.Proto3ToResult(p3qr)
			mu.Lock()
			defer mu.Unlock()

			if !exists {
				if err := proto.UnmarshalText(qr.Rows[0][0].ToString(), journal); err != nil {
					return err
				}
				exists = true
			}
			source.journaled = true
		}
		return nil
	})
	return exists, journal.SourceWorkflows, err
}

func (ts *trafficSwitcher) stopSourceWrites(ctx context.Context) error {
	var err error
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		err = ts.changeTableSourceWrites(ctx, disallowWrites)
	} else {
		err = ts.changeShardsAccess(ctx, ts.sourceKeyspace, ts.sourceShards(), disallowWrites)
	}
	if err != nil {
		return err
	}
	return ts.forAllSources(func(source *tsSource) error {
		var err error
		source.position, err = ts.wr.tmc.MasterPosition(ctx, source.master.Tablet)
		ts.wr.Logger().Infof("Position for source %v:%v: %v", ts.sourceKeyspace, source.si.ShardName(), source.position)
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
		return ts.wr.tmc.RefreshState(ctx, source.master.Tablet)
	})
}

func (ts *trafficSwitcher) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()

	var mu sync.Mutex
	return ts.forAllUids(func(target *tsTarget, uid uint32) error {
		bls := target.sources[uid]
		source := ts.sources[bls.Shard]
		ts.wr.Logger().Infof("waiting for keyspace:shard: %v:%v, position %v", ts.targetKeyspace, target.si.ShardName(), source.position)
		if err := ts.wr.tmc.VReplicationWaitForPos(ctx, target.master.Tablet, int(uid), source.position); err != nil {
			return err
		}
		ts.wr.Logger().Infof("position for keyspace:shard: %v:%v reached", ts.targetKeyspace, target.si.ShardName())
		if _, err := ts.wr.tmc.VReplicationExec(ctx, target.master.Tablet, binlogplayer.StopVReplication(uid, "stopped for cutover")); err != nil {
			return err
		}

		// Need lock because a target can have multiple uids.
		mu.Lock()
		defer mu.Unlock()
		if target.position != "" {
			return nil
		}
		var err error
		target.position, err = ts.wr.tmc.MasterPosition(ctx, target.master.Tablet)
		ts.wr.Logger().Infof("Position for uid %v: %v", uid, target.position)
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
						return fmt.Errorf("table %s not found in vschema", rule.Match)
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

		_, err := ts.wr.VReplicationExec(ctx, source.master.Alias, binlogplayer.CreateVReplicationState(ts.reverseWorkflow, reverseBls, target.position, binlogplayer.BlpStopped, source.master.DbName()))
		return err
	})
	return err
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
		return ts.wr.tmc.RefreshState(ctx, target.master.Tablet)
	})
}

func (ts *trafficSwitcher) changeRouting(ctx context.Context) error {
	if ts.migrationType == binlogdatapb.MigrationType_TABLES {
		return ts.changeTableRouting(ctx)
	}
	return ts.changeShardRouting(ctx)
}

func (ts *trafficSwitcher) changeTableRouting(ctx context.Context) error {
	rules, err := ts.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	// We assume that the following rules were setup when the targets were created:
	// table -> sourceKeyspace.table
	// targetKeyspace.table -> sourceKeyspace.table
	// Additionally, SwitchReads would have added rules like this:
	// table@replica -> targetKeyspace.table
	// targetKeyspace.table@replica -> targetKeyspace.table
	// After this step, only the following rules will be left:
	// table -> targetKeyspace.table
	// sourceKeyspace.table -> targetKeyspace.table
	for _, table := range ts.tables {
		for _, tabletType := range []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY} {
			tt := strings.ToLower(tabletType.String())
			delete(rules, table+"@"+tt)
			delete(rules, ts.targetKeyspace+"."+table+"@"+tt)
			delete(rules, ts.sourceKeyspace+"."+table+"@"+tt)
			ts.wr.Logger().Infof("Delete routing: %v %v %v", table+"@"+tt, ts.targetKeyspace+"."+table+"@"+tt, ts.sourceKeyspace+"."+table+"@"+tt)
		}
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
	return ts.wr.ts.MigrateServedType(ctx, ts.targetKeyspace, ts.targetShards(), ts.sourceShards(), topodatapb.TabletType_MASTER, nil)
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
		return ts.wr.tmc.RefreshState(ctx, source.master.Tablet)
	})
}

func (ts *trafficSwitcher) validateWorkflowHasCompleted(ctx context.Context) error {
	return doValidateWorkflowHasCompleted(ctx, ts)
}

func doValidateWorkflowHasCompleted(ctx context.Context, ts *trafficSwitcher) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	_ = ts.forAllSources(func(source *tsSource) error {
		wg.Add(1)
		if ts.migrationType == binlogdatapb.MigrationType_SHARDS {
			if source.si.IsMasterServing {
				rec.RecordError(fmt.Errorf(fmt.Sprintf("Shard %s is still serving", source.si.ShardName())))
			}
		}
		query := fmt.Sprintf("select 1 from _vt.vreplication where db_name='%s' and workflow='%s'", source.master.DbName(), ts.workflow)
		rs, _ := ts.wr.VReplicationExec(ctx, source.master.Alias, query)
		if len(rs.Rows) > 0 {
			rec.RecordError(fmt.Errorf("vreplication streams are not deleted from %d", source.master.Alias.Uid))
		}
		wg.Done()
		return nil
	})
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

func (ts *trafficSwitcher) dropSourceTables(ctx context.Context) error {
	return ts.forAllSources(func(source *tsSource) error {
		for _, tableName := range ts.tables {
			query := fmt.Sprintf("drop table %s.%s", encodeString(source.master.DbName()), tableName)
			_, err := ts.wr.ExecuteFetchAsDba(ctx, source.master.Alias, query, 1, false, true)
			if err != nil {
				ts.wr.Logger().Errorf("Error dropping table %s: %v", tableName, err)
				return err
			}
			ts.wr.Logger().Infof("Dropped table %s.%s\n", source.master.DbName(), tableName)

		}
		return nil
	})
}

func (ts *trafficSwitcher) dropSourceShards(ctx context.Context) error {
	return ts.forAllSources(func(source *tsSource) error {
		err := ts.wr.DeleteShard(ctx, source.si.Keyspace(), source.si.ShardName(), true, false)
		if err != nil {
			ts.wr.Logger().Errorf("Error deleting shard %s: %v", source.si.ShardName(), err)
			return err
		}
		ts.wr.Logger().Printf("Deleted shard %s.%s\n", source.si.Keyspace(), source.si.ShardName())
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
		ts.wr.Logger().Infof("Delete target streams for workflow %s db_name %s", ts.workflow, target.master.DbName())
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow=%s", encodeString(target.master.DbName()), encodeString(ts.workflow))
		_, err := ts.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query)
		return err
	})
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
