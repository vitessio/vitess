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
	"vitess.io/vitess/go/sync2"
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

// MigrationType specifies the type of migration.
type MigrationType int

// The following constants define the migration type.
const (
	MigrateTables = MigrationType(iota)
	MigrateShards
)

type migrateDirection int

const (
	directionForward = migrateDirection(iota)
	directionBackward
)

type accessType int

const (
	allowWrites = accessType(iota)
	disallowWrites
)

type migrater struct {
	migrationType  MigrationType
	wr             *Wrangler
	id             int64
	sources        map[topo.KeyspaceShard]*miSource
	targets        map[topo.KeyspaceShard]*miTarget
	sourceKeyspace string
	targetKeyspace string
	tables         []string
}

type miTarget struct {
	shard    *topo.ShardInfo
	master   *topo.TabletInfo
	sources  map[uint32]*binlogdatapb.BinlogSource
	position string
}

type miSource struct {
	shard     *topo.ShardInfo
	master    *topo.TabletInfo
	position  string
	journaled bool
}

// MigrateReads is a generic way of migrating read traffic for a resharding workflow.
func (wr *Wrangler) MigrateReads(ctx context.Context, migrationType MigrationType, streams map[topo.KeyspaceShard][]uint32, cells []string, servedType topodatapb.TabletType, direction migrateDirection) error {
	if servedType != topodatapb.TabletType_REPLICA && servedType != topodatapb.TabletType_RDONLY {
		return fmt.Errorf("tablet type must be REPLICA or RDONLY: %v", servedType)
	}
	mi, err := wr.buildMigrater(ctx, migrationType, streams)
	if err != nil {
		return err
	}
	if err := mi.validate(ctx); err != nil {
		return err
	}

	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, mi.sourceKeyspace, "MigrateReads")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	if mi.migrationType == MigrateTables {
		return mi.migrateTableReads(ctx, cells, servedType, direction)
	}
	return mi.migrateShardReads(ctx, cells, servedType, direction)
}

// MigrateWrites is a generic way of migrating write traffic for a resharding workflow.
func (wr *Wrangler) MigrateWrites(ctx context.Context, migrationType MigrationType, streams map[topo.KeyspaceShard][]uint32, filteredReplicationWaitTime time.Duration) error {
	mi, err := wr.buildMigrater(ctx, migrationType, streams)
	if err != nil {
		return err
	}
	if err := mi.validate(ctx); err != nil {
		return err
	}
	if err := mi.validateForWrite(ctx); err != nil {
		return err
	}

	ctx, sourceUnlock, lockErr := wr.ts.LockKeyspace(ctx, mi.sourceKeyspace, "MigrateWrites")
	if lockErr != nil {
		return lockErr
	}
	defer sourceUnlock(&err)
	if mi.targetKeyspace != mi.sourceKeyspace {
		tctx, targetUnlock, lockErr := wr.ts.LockKeyspace(ctx, mi.targetKeyspace, "MigrateWrites")
		if lockErr != nil {
			return lockErr
		}
		ctx = tctx
		defer targetUnlock(&err)
	}

	journalsExist, err := mi.checkJournals(ctx)
	if err != nil {
		return err
	}
	if !journalsExist {
		if err := mi.stopSourceWrites(ctx); err != nil {
			mi.cancelMigration(ctx)
			return err
		}
		if err := mi.waitForCatchup(ctx, filteredReplicationWaitTime); err != nil {
			mi.cancelMigration(ctx)
			return err
		}
	}
	if err := mi.createJournals(ctx); err != nil {
		return err
	}
	if err := mi.createReverseReplication(ctx); err != nil {
		return err
	}
	if err := mi.allowTargetWrites(ctx); err != nil {
		return err
	}
	if err := mi.changeRouting(ctx); err != nil {
		return err
	}
	mi.deleteTargetVReplication(ctx)
	return nil
}

func (wr *Wrangler) buildMigrater(ctx context.Context, migrationType MigrationType, streams map[topo.KeyspaceShard][]uint32) (*migrater, error) {
	mi := &migrater{
		migrationType: migrationType,
		wr:            wr,
		id:            hashStreams(streams),
		targets:       make(map[topo.KeyspaceShard]*miTarget),
		sources:       make(map[topo.KeyspaceShard]*miSource),
	}
	for targetks, uids := range streams {
		targetShard, err := mi.wr.ts.GetShard(ctx, targetks.Keyspace, targetks.Shard)
		if err != nil {
			return nil, err
		}
		targetMaster, err := mi.wr.ts.GetTablet(ctx, targetShard.MasterAlias)
		if err != nil {
			return nil, err
		}
		if _, ok := mi.targets[targetks]; ok {
			return nil, fmt.Errorf("duplicate targets: %v", targetks)
		}
		mi.targets[targetks] = &miTarget{
			shard:   targetShard,
			master:  targetMaster,
			sources: make(map[uint32]*binlogdatapb.BinlogSource),
		}
		if mi.targetKeyspace == "" {
			mi.targetKeyspace = targetks.Keyspace
		} else if mi.targetKeyspace != targetks.Keyspace {
			return nil, fmt.Errorf("target keyspaces are mismatched across streams: %v vs %v", mi.targetKeyspace, targetks.Keyspace)
		}
		for _, uid := range uids {
			p3qr, err := mi.wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, fmt.Sprintf("select source from _vt.vreplication where id=%d", uid))
			if err != nil {
				return nil, err
			}
			qr := sqltypes.Proto3ToResult(p3qr)
			if len(qr.Rows) < 1 || len(qr.Rows[0]) < 1 {
				return nil, fmt.Errorf("VReplication stream %d not found for %s:%s", int(uid), targetks.Keyspace, targetks.Shard)
			}
			for _, row := range qr.Rows {
				str := row[0].ToString()
				var binlogSource binlogdatapb.BinlogSource
				if err := proto.UnmarshalText(str, &binlogSource); err != nil {
					return nil, err
				}
				mi.targets[targetks].sources[uid] = &binlogSource

				sourceks := topo.KeyspaceShard{Keyspace: binlogSource.Keyspace, Shard: binlogSource.Shard}
				if _, ok := mi.sources[sourceks]; !ok {
					sourceShard, err := mi.wr.ts.GetShard(ctx, binlogSource.Keyspace, binlogSource.Shard)
					if err != nil {
						return nil, err
					}
					sourceMaster, err := mi.wr.ts.GetTablet(ctx, sourceShard.MasterAlias)
					if err != nil {
						return nil, err
					}
					mi.sources[sourceks] = &miSource{
						shard:  sourceShard,
						master: sourceMaster,
					}

					if mi.tables == nil {
						for _, rule := range binlogSource.Filter.Rules {
							mi.tables = append(mi.tables, rule.Match)
						}
						sort.Strings(mi.tables)
					} else {
						var tables []string
						for _, rule := range binlogSource.Filter.Rules {
							tables = append(tables, rule.Match)
						}
						sort.Strings(tables)
						if !reflect.DeepEqual(mi.tables, tables) {
							return nil, fmt.Errorf("table lists are mismatched across streams: %v vs %v", mi.tables, tables)
						}
					}

					if mi.sourceKeyspace == "" {
						mi.sourceKeyspace = sourceks.Keyspace
					} else if mi.sourceKeyspace != sourceks.Keyspace {
						return nil, fmt.Errorf("source keyspaces are mismatched across streams: %v vs %v", mi.sourceKeyspace, sourceks.Keyspace)
					}
				}
			}
		}
	}
	return mi, nil
}

// hashStreams produces a reproduceable hash based on the input parameters.
func hashStreams(streams map[topo.KeyspaceShard][]uint32) int64 {
	var expanded []string
	for ks, uids := range streams {
		for _, uid := range uids {
			expanded = append(expanded, fmt.Sprintf("%s:%s:%d", ks.Keyspace, ks.Shard, uid))
		}
	}
	sort.Strings(expanded)
	hasher := fnv.New64()
	for _, str := range expanded {
		hasher.Write([]byte(str))
	}
	// Convert to int64 after dropping the highest bit.
	return int64(hasher.Sum64() & math.MaxInt64)
}

func (mi *migrater) validate(ctx context.Context) error {
	// Ensure no duplicate sources in each target.
	for _, target := range mi.targets {
		uniqueSources := make(map[topo.KeyspaceShard]uint32)
		for uid, binlogSource := range target.sources {
			sourceks := topo.KeyspaceShard{Keyspace: binlogSource.Keyspace, Shard: binlogSource.Shard}
			if suid, ok := uniqueSources[sourceks]; ok {
				return fmt.Errorf("duplicate sources for uids: %v and %v", suid, uid)
			}
			uniqueSources[sourceks] = uid
		}
	}
	if mi.migrationType == MigrateTables {
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
	} else { // MigrateShards
		// Source and target keyspace must match
		if mi.sourceKeyspace != mi.targetKeyspace {
			return fmt.Errorf("source and target keyspace must match: %v vs %v", mi.sourceKeyspace, mi.targetKeyspace)
		}
		// Source and target shards must not match.
		for sourceks := range mi.sources {
			if _, ok := mi.targets[sourceks]; ok {
				return fmt.Errorf("target shard matches a source shard: %v", sourceks)
			}
		}
	}
	return nil
}

func (mi *migrater) validateForWrite(ctx context.Context) error {
	if mi.migrationType == MigrateTables {
		return mi.validateTableForWrite(ctx)
	}
	return mi.validateShardForWrite(ctx)
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
		si = source.shard
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

func (mi *migrater) migrateTableReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction migrateDirection) error {
	rules, err := mi.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	// We assume that the following rules were setup when the targets were created:
	// table -> sourceKeyspace.table
	// targetKeyspace.table -> sourceKeyspace.table
	tt := strings.ToLower(servedType.String())
	for _, table := range mi.tables {
		if direction == directionForward {
			rules[table+"@"+tt] = []string{mi.targetKeyspace + "." + table}
			rules[mi.targetKeyspace+"."+table+"@"+tt] = []string{mi.targetKeyspace + "." + table}
		} else {
			delete(rules, table+"@"+tt)
			delete(rules, mi.targetKeyspace+"."+table+"@"+tt)
		}
	}
	if err := mi.wr.saveRoutingRules(ctx, rules); err != nil {
		return err
	}
	return mi.wr.ts.RebuildSrvVSchema(ctx, cells)
}

func (mi *migrater) migrateShardReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction migrateDirection) error {
	var fromShards, toShards []*topo.ShardInfo
	if direction == directionForward {
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
	var exist sync2.AtomicBool
	err = mi.forAllSources(func(sourceks topo.KeyspaceShard, source *miSource) error {
		statement := fmt.Sprintf("select 1 from _vt.resharding_journal where id=%v", mi.id)
		p3qr, err := mi.wr.tmc.VReplicationExec(ctx, source.master.Tablet, statement)
		if err != nil {
			return err
		}
		if len(p3qr.Rows) >= 1 {
			exist.Set(true)
			source.journaled = true
		}
		return nil
	})
	return exist.Get(), err
}

func (mi *migrater) stopSourceWrites(ctx context.Context) error {
	var err error
	if mi.migrationType == MigrateTables {
		err = mi.changeTableSourceWrites(ctx, disallowWrites)
	} else {
		err = mi.changeShardsAccess(ctx, mi.sourceKeyspace, mi.sourceShards(), disallowWrites)
	}
	if err != nil {
		return err
	}
	return mi.forAllSources(func(sourceks topo.KeyspaceShard, source *miSource) error {
		var err error
		source.position, err = mi.wr.tmc.MasterPosition(ctx, source.master.Tablet)
		return err
	})
}

func (mi *migrater) changeTableSourceWrites(ctx context.Context, access accessType) error {
	return mi.forAllSources(func(sourceks topo.KeyspaceShard, source *miSource) error {
		if _, err := mi.wr.ts.UpdateShardFields(ctx, sourceks.Keyspace, sourceks.Shard, func(si *topo.ShardInfo) error {
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

	return mi.forAllUids(func(target *miTarget, uid uint32) error {
		bls := target.sources[uid]
		source := mi.sources[topo.KeyspaceShard{Keyspace: bls.Keyspace, Shard: bls.Shard}]
		if err := mi.wr.tmc.VReplicationWaitForPos(ctx, target.master.Tablet, int(uid), source.position); err != nil {
			return err
		}
		if _, err := mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, binlogplayer.StopVReplication(uid, "stopped for cutover")); err != nil {
			return err
		}
		var err error
		target.position, err = mi.wr.tmc.MasterPosition(ctx, target.master.Tablet)
		return err
	})
}

func (mi *migrater) cancelMigration(ctx context.Context) {
	var err error
	if mi.migrationType == MigrateTables {
		err = mi.changeTableSourceWrites(ctx, allowWrites)
	} else {
		err = mi.changeShardsAccess(ctx, mi.sourceKeyspace, mi.sourceShards(), allowWrites)
	}
	if err != nil {
		mi.wr.Logger().Errorf("Cancel migration failed:", err)
	}

	err = mi.forAllUids(func(target *miTarget, uid uint32) error {
		if _, err := mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, binlogplayer.StartVReplication(uid)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		mi.wr.Logger().Errorf("Cancel migration failed: could not restart vreplication: %v", err)
	}
}

func (mi *migrater) createJournals(ctx context.Context) error {
	return mi.forAllSources(func(sourceks topo.KeyspaceShard, source *miSource) error {
		if source.journaled {
			return nil
		}
		journal := &binlogdatapb.Journal{
			Id:            mi.id,
			Tables:        mi.tables,
			LocalPosition: source.position,
		}
		participantMap := make(map[topo.KeyspaceShard]bool)
		for targetks, target := range mi.targets {
			found := false
			for _, tsource := range target.sources {
				if sourceks == (topo.KeyspaceShard{Keyspace: tsource.Keyspace, Shard: tsource.Shard}) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
			journal.ShardGtids = append(journal.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: targetks.Keyspace,
				Shard:    targetks.Shard,
				Gtid:     target.position,
			})
			for _, tsource := range target.sources {
				participantMap[topo.KeyspaceShard{Keyspace: tsource.Keyspace, Shard: tsource.Shard}] = true
			}
		}
		for ks := range participantMap {
			journal.Participants = append(journal.Participants, &binlogdatapb.KeyspaceShard{
				Keyspace: ks.Keyspace,
				Shard:    ks.Shard,
			})
		}
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
	vs, err := mi.wr.ts.GetVSchema(ctx, mi.sourceKeyspace)
	if err != nil {
		return err
	}
	ksschema, err := vindexes.BuildKeyspaceSchema(vs, mi.sourceKeyspace)
	if err != nil {
		return err
	}
	return mi.forAllUids(func(target *miTarget, uid uint32) error {
		bls := target.sources[uid]
		source := mi.sources[topo.KeyspaceShard{Keyspace: bls.Keyspace, Shard: bls.Shard}]
		reverseBls := &binlogdatapb.BinlogSource{
			Keyspace:   target.shard.Keyspace(),
			Shard:      target.shard.ShardName(),
			TabletType: bls.TabletType,
			Filter:     &binlogdatapb.Filter{},
		}
		for _, rule := range bls.Filter.Rules {
			var filter string
			if strings.HasPrefix(rule.Match, "/") {
				if ksschema.Keyspace.Sharded {
					filter = bls.Shard
				}
			} else {
				var inKeyrange string
				if ksschema.Keyspace.Sharded {
					vtable, ok := ksschema.Tables[rule.Match]
					if !ok {
						return fmt.Errorf("table %s not found in vschema", rule.Match)
					}
					// TODO(sougou): handle degenerate cases like sequence, etc.
					// We currently assume the primary vindex is the best way to filter, which may not be true.
					inKeyrange = fmt.Sprintf(" where in_keyrange(%s, '%s', '%s')", sqlparser.String(vtable.ColumnVindexes[0].Columns[0]), vs.Vindexes[vtable.ColumnVindexes[0].Name].Type, bls.Shard)
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
	if mi.migrationType == MigrateTables {
		return mi.allowTableTargetWrites(ctx)
	}
	return mi.changeShardsAccess(ctx, mi.targetKeyspace, mi.targetShards(), allowWrites)
}

func (mi *migrater) allowTableTargetWrites(ctx context.Context) error {
	return mi.forAllTargets(func(targetks topo.KeyspaceShard, target *miTarget) error {
		if _, err := mi.wr.ts.UpdateShardFields(ctx, targetks.Keyspace, targetks.Shard, func(si *topo.ShardInfo) error {
			return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, true, mi.tables)
		}); err != nil {
			return err
		}
		return mi.wr.tmc.RefreshState(ctx, target.master.Tablet)
	})
}

func (mi *migrater) changeRouting(ctx context.Context) error {
	if mi.migrationType == MigrateTables {
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
		}
		delete(rules, mi.targetKeyspace+"."+table)
		rules[table] = []string{mi.targetKeyspace + "." + table}
	}
	if err := mi.wr.saveRoutingRules(ctx, rules); err != nil {
		return err
	}
	return mi.wr.ts.RebuildSrvVSchema(ctx, nil)
}

func (mi *migrater) changeShardRouting(ctx context.Context) error {
	err := mi.forAllSources(func(sourceks topo.KeyspaceShard, source *miSource) error {
		_, err := mi.wr.ts.UpdateShardFields(ctx, source.shard.Keyspace(), source.shard.ShardName(), func(si *topo.ShardInfo) error {
			si.IsMasterServing = false
			return nil
		})
		return err
	})
	if err != nil {
		return err
	}
	err = mi.forAllTargets(func(targetks topo.KeyspaceShard, target *miTarget) error {
		_, err := mi.wr.ts.UpdateShardFields(ctx, target.shard.Keyspace(), target.shard.ShardName(), func(si *topo.ShardInfo) error {
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
	_ = mi.forAllUids(func(target *miTarget, uid uint32) error {
		if _, err := mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, binlogplayer.DeleteVReplication(uid)); err != nil {
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

func (mi *migrater) forAllSources(f func(topo.KeyspaceShard, *miSource) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for sourceks, source := range mi.sources {
		wg.Add(1)
		go func(sourceks topo.KeyspaceShard, source *miSource) {
			defer wg.Done()

			if err := f(sourceks, source); err != nil {
				allErrors.RecordError(err)
			}
		}(sourceks, source)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (mi *migrater) forAllTargets(f func(topo.KeyspaceShard, *miTarget) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for targetks, target := range mi.targets {
		wg.Add(1)
		go func(targetks topo.KeyspaceShard, target *miTarget) {
			defer wg.Done()

			if err := f(targetks, target); err != nil {
				allErrors.RecordError(err)
			}
		}(targetks, target)
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
		shards = append(shards, source.shard)
	}
	return shards
}

func (mi *migrater) targetShards() []*topo.ShardInfo {
	shards := make([]*topo.ShardInfo, 0, len(mi.targets))
	for _, target := range mi.targets {
		shards = append(shards, target.shard)
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
