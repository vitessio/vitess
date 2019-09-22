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
)

// MigrateDirection specifies the migration direction.
type MigrateDirection int

// The following constants define the migration direction.
const (
	DirectionForward = MigrateDirection(iota)
	DirectionBackward
)

// accessType specifies the type of access for a shard (allow/disallow writes).
type accessType int

const (
	allowWrites = accessType(iota)
	disallowWrites
)

// migrater contains the metadata for migrating read and write traffic
// for vreplication streams.
type migrater struct {
	migrationType   binlogdatapb.MigrationType
	wr              *Wrangler
	workflow        string
	id              int64
	sources         map[string]*miSource
	targets         map[string]*miTarget
	sourceKeyspace  string
	targetKeyspace  string
	tables          []string
	sourceKSSchema  *vindexes.KeyspaceSchema
	sourceWorkflows []string
}

// miTarget contains the metadata for each migration target.
type miTarget struct {
	si       *topo.ShardInfo
	master   *topo.TabletInfo
	sources  map[uint32]*binlogdatapb.BinlogSource
	position string
}

// miSource contains the metadata for each migration source.
type miSource struct {
	si        *topo.ShardInfo
	master    *topo.TabletInfo
	position  string
	journaled bool
}

// MigrateReads is a generic way of migrating read traffic for a resharding workflow.
func (wr *Wrangler) MigrateReads(ctx context.Context, targetKeyspace, workflow string, servedType topodatapb.TabletType, cells []string, direction MigrateDirection) error {
	if servedType != topodatapb.TabletType_REPLICA && servedType != topodatapb.TabletType_RDONLY {
		return fmt.Errorf("tablet type must be REPLICA or RDONLY: %v", servedType)
	}
	mi, err := wr.buildMigrater(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildMigrater failed: %v", err)
		return err
	}
	if err := mi.validate(ctx, false /* isWrite */); err != nil {
		mi.wr.Logger().Errorf("validate failed: %v", err)
		return err
	}

	// For reads, locking the source keyspace is sufficient.
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, mi.sourceKeyspace, "MigrateReads")
	if lockErr != nil {
		mi.wr.Logger().Errorf("LockKeyspace failed: %v", lockErr)
		return lockErr
	}
	defer unlock(&err)

	if mi.migrationType == binlogdatapb.MigrationType_TABLES {
		if err := mi.migrateTableReads(ctx, cells, servedType, direction); err != nil {
			mi.wr.Logger().Errorf("migrateTableReads failed: %v", err)
			return err
		}
		return nil
	}
	if err := mi.migrateShardReads(ctx, cells, servedType, direction); err != nil {
		mi.wr.Logger().Errorf("migrateShardReads failed: %v", err)
		return err
	}
	return nil
}

// MigrateWrites is a generic way of migrating write traffic for a resharding workflow.
func (wr *Wrangler) MigrateWrites(ctx context.Context, targetKeyspace, workflow string, filteredReplicationWaitTime time.Duration) (journalID int64, err error) {
	mi, err := wr.buildMigrater(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildMigrater failed: %v", err)
		return 0, err
	}
	mi.wr.Logger().Infof("Built migration metadata: %+v", mi)
	if err := mi.validate(ctx, true /* isWrite */); err != nil {
		mi.wr.Logger().Errorf("validate failed: %v", err)
		return 0, err
	}

	// Need to lock both source and target keyspaces.
	ctx, sourceUnlock, lockErr := wr.ts.LockKeyspace(ctx, mi.sourceKeyspace, "MigrateWrites")
	if lockErr != nil {
		mi.wr.Logger().Errorf("LockKeyspace failed: %v", lockErr)
		return 0, lockErr
	}
	defer sourceUnlock(&err)
	if mi.targetKeyspace != mi.sourceKeyspace {
		tctx, targetUnlock, lockErr := wr.ts.LockKeyspace(ctx, mi.targetKeyspace, "MigrateWrites")
		if lockErr != nil {
			mi.wr.Logger().Errorf("LockKeyspace failed: %v", lockErr)
			return 0, lockErr
		}
		ctx = tctx
		defer targetUnlock(&err)
	}

	journalsExist, err := mi.checkJournals(ctx)
	if err != nil {
		mi.wr.Logger().Errorf("checkJournals failed: %v", err)
		return 0, err
	}
	if !journalsExist {
		mi.wr.Logger().Infof("No previous journals were found. Proceeding normally.")
		sm := &streamMigrater{mi: mi}
		tabletStreams, err := sm.stopStreams(ctx)
		if err != nil {
			mi.wr.Logger().Errorf("stopStreams failed: %v", err)
			mi.cancelMigration(ctx)
			return 0, err
		}
		if err := mi.stopSourceWrites(ctx); err != nil {
			mi.wr.Logger().Errorf("stopSourceWrites failed: %v", err)
			mi.cancelMigration(ctx)
			return 0, err
		}
		if err := mi.waitForCatchup(ctx, filteredReplicationWaitTime); err != nil {
			mi.wr.Logger().Errorf("waitForCatchup failed: %v", err)
			mi.cancelMigration(ctx)
			return 0, err
		}
		mi.sourceWorkflows, err = sm.migrateStreams(ctx, tabletStreams)
		if err != nil {
			mi.wr.Logger().Errorf("migrateStreams failed: %v", err)
			mi.cancelMigration(ctx)
			return 0, err
		}
	} else {
		mi.wr.Logger().Infof("Journals were found. Completing the left over steps.")
		// Need to gather positions in case all journals were not created.
		if err := mi.gatherPositions(ctx); err != nil {
			mi.wr.Logger().Errorf("gatherPositions failed: %v", err)
			return 0, err
		}
	}
	// This is the point of no return. Once a journal is created,
	// traffic can be redirected to target shards.
	if err := mi.createJournals(ctx); err != nil {
		mi.wr.Logger().Errorf("createJournals failed: %v", err)
		return 0, err
	}
	if err := mi.createReverseReplication(ctx); err != nil {
		mi.wr.Logger().Errorf("createReverseReplication failed: %v", err)
		return 0, err
	}
	if err := mi.allowTargetWrites(ctx); err != nil {
		mi.wr.Logger().Errorf("allowTargetWrites failed: %v", err)
		return 0, err
	}
	if err := mi.changeRouting(ctx); err != nil {
		mi.wr.Logger().Errorf("changeRouting failed: %v", err)
		return 0, err
	}
	sm := &streamMigrater{mi: mi}
	if err := sm.finalize(ctx, mi.sourceWorkflows); err != nil {
		mi.wr.Logger().Errorf("finalize failed: %v", err)
		return 0, err
	}
	mi.deleteTargetVReplication(ctx)
	return mi.id, nil
}

func (wr *Wrangler) buildMigrater(ctx context.Context, targetKeyspace, workflow string) (*migrater, error) {
	targets, err := wr.buildMigrationTargets(ctx, targetKeyspace, workflow)
	if err != nil {
		return nil, err
	}

	mi := &migrater{
		wr:             wr,
		workflow:       workflow,
		id:             hashStreams(targetKeyspace, targets),
		targets:        targets,
		sources:        make(map[string]*miSource),
		targetKeyspace: targetKeyspace,
	}
	mi.wr.Logger().Infof("Migration ID for workflow %s: %d", workflow, mi.id)

	// Build the sources
	for _, target := range targets {
		for _, bls := range target.sources {
			if mi.sourceKeyspace == "" {
				mi.sourceKeyspace = bls.Keyspace
			} else if mi.sourceKeyspace != bls.Keyspace {
				return nil, fmt.Errorf("source keyspaces are mismatched across streams: %v vs %v", mi.sourceKeyspace, bls.Keyspace)
			}

			if mi.tables == nil {
				for _, rule := range bls.Filter.Rules {
					mi.tables = append(mi.tables, rule.Match)
				}
				sort.Strings(mi.tables)
			} else {
				var tables []string
				for _, rule := range bls.Filter.Rules {
					tables = append(tables, rule.Match)
				}
				sort.Strings(tables)
				if !reflect.DeepEqual(mi.tables, tables) {
					return nil, fmt.Errorf("table lists are mismatched across streams: %v vs %v", mi.tables, tables)
				}
			}

			if _, ok := mi.sources[bls.Shard]; ok {
				continue
			}
			sourcesi, err := mi.wr.ts.GetShard(ctx, bls.Keyspace, bls.Shard)
			if err != nil {
				return nil, err
			}
			sourceMaster, err := mi.wr.ts.GetTablet(ctx, sourcesi.MasterAlias)
			if err != nil {
				return nil, err
			}
			mi.sources[bls.Shard] = &miSource{
				si:     sourcesi,
				master: sourceMaster,
			}
		}
	}
	if mi.sourceKeyspace != mi.targetKeyspace {
		mi.migrationType = binlogdatapb.MigrationType_TABLES
	} else {
		// TODO(sougou): for shard migration, validate that source and target combined
		// keyranges match.
		mi.migrationType = binlogdatapb.MigrationType_SHARDS
		for sourceShard := range mi.sources {
			if _, ok := mi.targets[sourceShard]; ok {
				// If shards are overlapping, then this is a table migration.
				mi.migrationType = binlogdatapb.MigrationType_TABLES
				break
			}
		}
	}
	vs, err := mi.wr.ts.GetVSchema(ctx, mi.sourceKeyspace)
	if err != nil {
		return nil, err
	}
	mi.sourceKSSchema, err = vindexes.BuildKeyspaceSchema(vs, mi.sourceKeyspace)
	if err != nil {
		return nil, err
	}
	return mi, nil
}

func (wr *Wrangler) buildMigrationTargets(ctx context.Context, targetKeyspace, workflow string) (targets map[string]*miTarget, err error) {
	targets = make(map[string]*miTarget)
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
		targetMaster, err := wr.ts.GetTablet(ctx, targetsi.MasterAlias)
		if err != nil {
			return nil, err
		}
		p3qr, err := wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, fmt.Sprintf("select id, source from _vt.vreplication where workflow=%s and db_name=%s", encodeString(workflow), encodeString(targetMaster.DbName())))
		if err != nil {
			return nil, err
		}
		// If there's no vreplication stream, check the next target.
		if len(p3qr.Rows) < 1 {
			continue
		}

		targets[targetShard] = &miTarget{
			si:      targetsi,
			master:  targetMaster,
			sources: make(map[uint32]*binlogdatapb.BinlogSource),
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		for _, row := range qr.Rows {
			id, err := sqltypes.ToInt64(row[0])
			if err != nil {
				return nil, err
			}
			var bls binlogdatapb.BinlogSource
			if err := proto.UnmarshalText(row[1].ToString(), &bls); err != nil {
				return nil, err
			}
			targets[targetShard].sources[uint32(id)] = &bls
		}
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no streams found in keyspace %s for: %s", targetKeyspace, workflow)
	}
	return targets, nil
}

// hashStreams produces a reproducible hash based on the input parameters.
func hashStreams(targetKeyspace string, targets map[string]*miTarget) int64 {
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

func (mi *migrater) validate(ctx context.Context, isWrite bool) error {
	if mi.migrationType == binlogdatapb.MigrationType_TABLES {
		// All shards must be present.
		if err := mi.compareShards(ctx, mi.sourceKeyspace, mi.sourceShards()); err != nil {
			return err
		}
		if err := mi.compareShards(ctx, mi.targetKeyspace, mi.targetShards()); err != nil {
			return err
		}
		// Wildcard table names not allowed.
		for _, table := range mi.tables {
			if strings.HasPrefix(table, "/") {
				return fmt.Errorf("cannot migrate streams with wild card table names: %v", table)
			}
		}
		if isWrite {
			return mi.validateTableForWrite(ctx)
		}
	} else { // binlogdatapb.MigrationType_SHARDS
		if isWrite {
			return mi.validateShardForWrite(ctx)
		}
	}
	return nil
}

func (mi *migrater) validateTableForWrite(ctx context.Context) error {
	rules, err := mi.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	for _, table := range mi.tables {
		for _, tabletType := range []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY} {
			tt := strings.ToLower(tabletType.String())
			if rules[table+"@"+tt] == nil || rules[mi.targetKeyspace+"."+table+"@"+tt] == nil {
				return fmt.Errorf("missing tablet type specific routing, read-only traffic must be migrated before migrating writes: %v", table)
			}
		}
	}
	return nil
}

func (mi *migrater) validateShardForWrite(ctx context.Context) error {
	srvKeyspaces, err := mi.wr.ts.GetSrvKeyspaceAllCells(ctx, mi.sourceKeyspace)
	if err != nil {
		return err
	}

	// Checking one shard is enough.
	var si *topo.ShardInfo
	for _, source := range mi.sources {
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
			return fmt.Errorf("cannot migrate MASTER away from %v/%v until everything else is migrated. Make sure that the following types are migrated first: %v", si.Keyspace(), si.ShardName(), strings.Join(shardServedTypes, ", "))
		}
	}
	return nil
}

func (mi *migrater) compareShards(ctx context.Context, keyspace string, sis []*topo.ShardInfo) error {
	var shards []string
	for _, si := range sis {
		shards = append(shards, si.ShardName())
	}
	topoShards, err := mi.wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}
	sort.Strings(topoShards)
	sort.Strings(shards)
	if !reflect.DeepEqual(topoShards, shards) {
		return fmt.Errorf("mismatched shards for keyspace %s: topo: %v vs migrate command: %v", keyspace, topoShards, shards)
	}
	return nil
}

func (mi *migrater) migrateTableReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction MigrateDirection) error {
	rules, err := mi.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	// We assume that the following rules were setup when the targets were created:
	// table -> sourceKeyspace.table
	// targetKeyspace.table -> sourceKeyspace.table
	// For forward migration, we add tablet type specific rules to redirect traffic to the target.
	// For backward, we delete them.
	tt := strings.ToLower(servedType.String())
	for _, table := range mi.tables {
		if direction == DirectionForward {
			rules[table+"@"+tt] = []string{mi.targetKeyspace + "." + table}
			rules[mi.targetKeyspace+"."+table+"@"+tt] = []string{mi.targetKeyspace + "." + table}
			rules[mi.sourceKeyspace+"."+table+"@"+tt] = []string{mi.targetKeyspace + "." + table}
		} else {
			delete(rules, table+"@"+tt)
			delete(rules, mi.targetKeyspace+"."+table+"@"+tt)
			delete(rules, mi.sourceKeyspace+"."+table+"@"+tt)
		}
	}
	if err := mi.wr.saveRoutingRules(ctx, rules); err != nil {
		return err
	}
	return mi.wr.ts.RebuildSrvVSchema(ctx, cells)
}

func (mi *migrater) migrateShardReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction MigrateDirection) error {
	var fromShards, toShards []*topo.ShardInfo
	if direction == DirectionForward {
		fromShards, toShards = mi.sourceShards(), mi.targetShards()
	} else {
		fromShards, toShards = mi.targetShards(), mi.sourceShards()
	}

	if err := mi.wr.updateShardRecords(ctx, mi.sourceKeyspace, fromShards, cells, servedType, true /* isFrom */, false /* clearSourceShards */); err != nil {
		return err
	}
	if err := mi.wr.updateShardRecords(ctx, mi.sourceKeyspace, toShards, cells, servedType, false, false); err != nil {
		return err
	}
	return mi.wr.ts.MigrateServedType(ctx, mi.sourceKeyspace, toShards, fromShards, servedType, cells)
}

func (mi *migrater) checkJournals(ctx context.Context) (journalsExist bool, err error) {
	var mu sync.Mutex
	journal := &binlogdatapb.Journal{}
	var exists bool
	err = mi.forAllSources(func(source *miSource) error {
		statement := fmt.Sprintf("select val from _vt.resharding_journal where id=%v", mi.id)
		p3qr, err := mi.wr.tmc.VReplicationExec(ctx, source.master.Tablet, statement)
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
	mi.sourceWorkflows = journal.SourceWorkflows
	return exists, err
}

func (mi *migrater) stopSourceWrites(ctx context.Context) error {
	var err error
	if mi.migrationType == binlogdatapb.MigrationType_TABLES {
		err = mi.changeTableSourceWrites(ctx, disallowWrites)
	} else {
		err = mi.changeShardsAccess(ctx, mi.sourceKeyspace, mi.sourceShards(), disallowWrites)
	}
	if err != nil {
		return err
	}
	return mi.forAllSources(func(source *miSource) error {
		var err error
		source.position, err = mi.wr.tmc.MasterPosition(ctx, source.master.Tablet)
		mi.wr.Logger().Infof("Position for source %v:%v: %v", mi.sourceKeyspace, source.si.ShardName(), source.position)
		return err
	})
}

func (mi *migrater) changeTableSourceWrites(ctx context.Context, access accessType) error {
	return mi.forAllSources(func(source *miSource) error {
		if _, err := mi.wr.ts.UpdateShardFields(ctx, mi.sourceKeyspace, source.si.ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, access == allowWrites /* remove */, mi.tables)
		}); err != nil {
			return err
		}
		return mi.wr.tmc.RefreshState(ctx, source.master.Tablet)
	})
}

func (mi *migrater) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()

	var mu sync.Mutex
	return mi.forAllUids(func(target *miTarget, uid uint32) error {
		bls := target.sources[uid]
		source := mi.sources[bls.Shard]
		if err := mi.wr.tmc.VReplicationWaitForPos(ctx, target.master.Tablet, int(uid), source.position); err != nil {
			return err
		}
		if _, err := mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, binlogplayer.StopVReplication(uid, "stopped for cutover")); err != nil {
			return err
		}

		// Need lock because a target can have multiple uids.
		mu.Lock()
		defer mu.Unlock()
		if target.position != "" {
			return nil
		}
		var err error
		target.position, err = mi.wr.tmc.MasterPosition(ctx, target.master.Tablet)
		mi.wr.Logger().Infof("Position for uid %v: %v", uid, target.position)
		return err
	})
}

func (mi *migrater) cancelMigration(ctx context.Context) {
	var err error
	if mi.migrationType == binlogdatapb.MigrationType_TABLES {
		err = mi.changeTableSourceWrites(ctx, allowWrites)
	} else {
		err = mi.changeShardsAccess(ctx, mi.sourceKeyspace, mi.sourceShards(), allowWrites)
	}
	if err != nil {
		mi.wr.Logger().Errorf("Cancel migration failed:", err)
	}

	sm := &streamMigrater{mi: mi}
	sm.cancelMigration(ctx)

	err = mi.forAllTargets(func(target *miTarget) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s", encodeString(target.master.DbName()), encodeString(mi.workflow))
		_, err := mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query)
		return err
	})
	if err != nil {
		mi.wr.Logger().Errorf("Cancel migration failed: could not restart vreplication: %v", err)
	}
}

func (mi *migrater) gatherPositions(ctx context.Context) error {
	err := mi.forAllSources(func(source *miSource) error {
		var err error
		source.position, err = mi.wr.tmc.MasterPosition(ctx, source.master.Tablet)
		mi.wr.Logger().Infof("Position for source %v:%v: %v", mi.sourceKeyspace, source.si.ShardName(), source.position)
		return err
	})
	if err != nil {
		return err
	}
	return mi.forAllTargets(func(target *miTarget) error {
		var err error
		target.position, err = mi.wr.tmc.MasterPosition(ctx, target.master.Tablet)
		mi.wr.Logger().Infof("Position for target %v:%v: %v", mi.targetKeyspace, target.si.ShardName(), target.position)
		return err
	})
}

func (mi *migrater) createJournals(ctx context.Context) error {
	var participants []*binlogdatapb.KeyspaceShard
	for sourceShard := range mi.sources {
		participants = append(participants, &binlogdatapb.KeyspaceShard{
			Keyspace: mi.sourceKeyspace,
			Shard:    sourceShard,
		})
	}
	return mi.forAllSources(func(source *miSource) error {
		if source.journaled {
			return nil
		}
		journal := &binlogdatapb.Journal{
			Id:              mi.id,
			MigrationType:   mi.migrationType,
			Tables:          mi.tables,
			LocalPosition:   source.position,
			Participants:    participants,
			SourceWorkflows: mi.sourceWorkflows,
		}
		for targetShard, target := range mi.targets {
			found := false
			for _, tsource := range target.sources {
				if source.si.ShardName() == tsource.Shard {
					found = true
					break
				}
			}
			if !found {
				continue
			}
			journal.ShardGtids = append(journal.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: mi.targetKeyspace,
				Shard:    targetShard,
				Gtid:     target.position,
			})
		}
		mi.wr.Logger().Infof("Creating journal: %v", journal)
		statement := fmt.Sprintf("insert into _vt.resharding_journal "+
			"(id, db_name, val) "+
			"values (%v, %v, %v)",
			mi.id, encodeString(source.master.DbName()), encodeString(journal.String()))
		if _, err := mi.wr.tmc.VReplicationExec(ctx, source.master.Tablet, statement); err != nil {
			return err
		}
		return nil
	})
}

func (mi *migrater) createReverseReplication(ctx context.Context) error {
	return mi.forAllUids(func(target *miTarget, uid uint32) error {
		bls := target.sources[uid]
		source := mi.sources[bls.Shard]
		reverseBls := &binlogdatapb.BinlogSource{
			Keyspace:   mi.targetKeyspace,
			Shard:      target.si.ShardName(),
			TabletType: bls.TabletType,
			Filter:     &binlogdatapb.Filter{},
		}
		for _, rule := range bls.Filter.Rules {
			var filter string
			if strings.HasPrefix(rule.Match, "/") {
				if mi.sourceKSSchema.Keyspace.Sharded {
					filter = bls.Shard
				}
			} else {
				var inKeyrange string
				if mi.sourceKSSchema.Keyspace.Sharded {
					vtable, ok := mi.sourceKSSchema.Tables[rule.Match]
					if !ok {
						return fmt.Errorf("table %s not found in vschema", rule.Match)
					}
					// TODO(sougou): handle degenerate cases like sequence, etc.
					// We currently assume the primary vindex is the best way to filter, which may not be true.
					inKeyrange = fmt.Sprintf(" where in_keyrange(%s, '%s', '%s')", sqlparser.String(vtable.ColumnVindexes[0].Columns[0]), vtable.ColumnVindexes[0].Type, bls.Shard)
				}
				filter = fmt.Sprintf("select * from %s%s", rule.Match, inKeyrange)
			}
			reverseBls.Filter.Rules = append(reverseBls.Filter.Rules, &binlogdatapb.Rule{
				Match:  rule.Match,
				Filter: filter,
			})
		}

		_, err := mi.wr.VReplicationExec(ctx, source.master.Alias, binlogplayer.CreateVReplicationState("ReversedResharding", reverseBls, target.position, binlogplayer.BlpStopped, source.master.DbName()))
		return err
	})
}

func (mi *migrater) allowTargetWrites(ctx context.Context) error {
	if mi.migrationType == binlogdatapb.MigrationType_TABLES {
		return mi.allowTableTargetWrites(ctx)
	}
	return mi.changeShardsAccess(ctx, mi.targetKeyspace, mi.targetShards(), allowWrites)
}

func (mi *migrater) allowTableTargetWrites(ctx context.Context) error {
	return mi.forAllTargets(func(target *miTarget) error {
		if _, err := mi.wr.ts.UpdateShardFields(ctx, mi.targetKeyspace, target.si.ShardName(), func(si *topo.ShardInfo) error {
			return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, true, mi.tables)
		}); err != nil {
			return err
		}
		return mi.wr.tmc.RefreshState(ctx, target.master.Tablet)
	})
}

func (mi *migrater) changeRouting(ctx context.Context) error {
	if mi.migrationType == binlogdatapb.MigrationType_TABLES {
		return mi.changeTableRouting(ctx)
	}
	return mi.changeShardRouting(ctx)
}

func (mi *migrater) changeTableRouting(ctx context.Context) error {
	rules, err := mi.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	// We assume that the following rules were setup when the targets were created:
	// table -> sourceKeyspace.table
	// targetKeyspace.table -> sourceKeyspace.table
	// Additionally, MigrateReads would have added rules like this:
	// table@replica -> targetKeyspace.table
	// targetKeyspace.table@replica -> targetKeyspace.table
	// After this step, only the following rules will be left:
	// table -> targetKeyspace.table
	// sourceKeyspace.table -> targetKeyspace.table
	for _, table := range mi.tables {
		for _, tabletType := range []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY} {
			tt := strings.ToLower(tabletType.String())
			delete(rules, table+"@"+tt)
			delete(rules, mi.targetKeyspace+"."+table+"@"+tt)
			delete(rules, mi.sourceKeyspace+"."+table+"@"+tt)
			mi.wr.Logger().Infof("Delete routing: %v %v %v", table+"@"+tt, mi.targetKeyspace+"."+table+"@"+tt, mi.sourceKeyspace+"."+table+"@"+tt)
		}
		delete(rules, mi.targetKeyspace+"."+table)
		mi.wr.Logger().Infof("Delete routing: %v", mi.targetKeyspace+"."+table)
		rules[table] = []string{mi.targetKeyspace + "." + table}
		rules[mi.sourceKeyspace+"."+table] = []string{mi.targetKeyspace + "." + table}
		mi.wr.Logger().Infof("Add routing: %v %v", table, mi.sourceKeyspace+"."+table)
	}
	if err := mi.wr.saveRoutingRules(ctx, rules); err != nil {
		return err
	}
	return mi.wr.ts.RebuildSrvVSchema(ctx, nil)
}

func (mi *migrater) changeShardRouting(ctx context.Context) error {
	err := mi.forAllSources(func(source *miSource) error {
		_, err := mi.wr.ts.UpdateShardFields(ctx, mi.sourceKeyspace, source.si.ShardName(), func(si *topo.ShardInfo) error {
			si.IsMasterServing = false
			return nil
		})
		return err
	})
	if err != nil {
		return err
	}
	err = mi.forAllTargets(func(target *miTarget) error {
		_, err := mi.wr.ts.UpdateShardFields(ctx, mi.targetKeyspace, target.si.ShardName(), func(si *topo.ShardInfo) error {
			si.IsMasterServing = true
			return nil
		})
		return err
	})
	if err != nil {
		return err
	}
	return mi.wr.ts.MigrateServedType(ctx, mi.targetKeyspace, mi.targetShards(), mi.sourceShards(), topodatapb.TabletType_MASTER, nil)
}

func (mi *migrater) deleteTargetVReplication(ctx context.Context) {
	_ = mi.forAllTargets(func(target *miTarget) error {
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow=%s", encodeString(target.master.DbName()), encodeString(mi.workflow))
		if _, err := mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query); err != nil {
			mi.wr.Logger().Errorf("Final cleanup: could not delete vreplication, please delete stopped streams manually: %v", err)
		}
		return nil
	})
}

func (mi *migrater) changeShardsAccess(ctx context.Context, keyspace string, shards []*topo.ShardInfo, access accessType) error {
	if err := mi.wr.ts.UpdateDisableQueryService(ctx, mi.sourceKeyspace, shards, topodatapb.TabletType_MASTER, nil, access == disallowWrites /* disable */); err != nil {
		return err
	}
	return mi.wr.refreshMasters(ctx, shards)
}

func (mi *migrater) forAllSources(f func(*miSource) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, source := range mi.sources {
		wg.Add(1)
		go func(source *miSource) {
			defer wg.Done()

			if err := f(source); err != nil {
				allErrors.RecordError(err)
			}
		}(source)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (mi *migrater) forAllTargets(f func(*miTarget) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range mi.targets {
		wg.Add(1)
		go func(target *miTarget) {
			defer wg.Done()

			if err := f(target); err != nil {
				allErrors.RecordError(err)
			}
		}(target)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (mi *migrater) forAllUids(f func(target *miTarget, uid uint32) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range mi.targets {
		for uid := range target.sources {
			wg.Add(1)
			go func(target *miTarget, uid uint32) {
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

func (mi *migrater) sourceShards() []*topo.ShardInfo {
	shards := make([]*topo.ShardInfo, 0, len(mi.sources))
	for _, source := range mi.sources {
		shards = append(shards, source.si)
	}
	return shards
}

func (mi *migrater) targetShards() []*topo.ShardInfo {
	shards := make([]*topo.ShardInfo, 0, len(mi.targets))
	for _, target := range mi.targets {
		shards = append(shards, target.si)
	}
	return shards
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
