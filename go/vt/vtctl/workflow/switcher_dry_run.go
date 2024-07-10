/*
Copyright 2023 The Vitess Authors.

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
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/topo"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var _ iswitcher = (*switcherDryRun)(nil)

type switcherDryRun struct {
	drLog *LogRecorder
	ts    *trafficSwitcher
}

func (dr *switcherDryRun) addParticipatingTablesToKeyspace(ctx context.Context, keyspace, tableSpecs string) error {
	dr.drLog.Log("All source tables will be added to the target keyspace vschema")
	return nil
}

func (dr *switcherDryRun) deleteRoutingRules(ctx context.Context) error {
	dr.drLog.Log("Routing rules for participating tables will be deleted")
	return nil
}

func (dr *switcherDryRun) deleteShardRoutingRules(ctx context.Context) error {
	if dr.ts.isPartialMigration {
		dr.drLog.Log("Shard routing rules for participating shards will be deleted")
	}
	return nil
}

func (dr *switcherDryRun) deleteKeyspaceRoutingRules(ctx context.Context) error {
	if dr.ts.IsMultiTenantMigration() {
		dr.drLog.Log("Keyspace routing rules will be deleted")
	}
	return nil
}

func (dr *switcherDryRun) mirrorTableTraffic(ctx context.Context, types []topodatapb.TabletType, percent float32) error {
	var tabletTypes []string
	for _, servedType := range types {
		tabletTypes = append(tabletTypes, servedType.String())
	}
	dr.drLog.Logf("Mirroring %.2f percent of traffic from keyspace %s to keyspace %s for tablet types [%s]",
		percent, dr.ts.SourceKeyspaceName(), dr.ts.TargetKeyspaceName(), strings.Join(tabletTypes, ","))

	return nil
}

func (dr *switcherDryRun) switchKeyspaceReads(ctx context.Context, types []topodatapb.TabletType) error {
	var tabletTypes []string
	for _, servedType := range types {
		tabletTypes = append(tabletTypes, servedType.String())
	}
	dr.drLog.Logf("Switch reads from keyspace %s to keyspace %s for tablet types [%s]",
		dr.ts.SourceKeyspaceName(), dr.ts.TargetKeyspaceName(), strings.Join(tabletTypes, ","))
	return nil
}

func (dr *switcherDryRun) switchShardReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction TrafficSwitchDirection) error {
	sourceShards := make([]string, 0)
	targetShards := make([]string, 0)
	for _, source := range dr.ts.Sources() {
		sourceShards = append(sourceShards, source.GetShard().ShardName())
	}
	for _, target := range dr.ts.Targets() {
		targetShards = append(targetShards, target.GetShard().ShardName())
	}
	// Sort the slices for deterministic output.
	sort.Strings(sourceShards)
	sort.Strings(targetShards)
	if direction == DirectionForward {
		dr.drLog.Logf("Switch reads from keyspace %s to keyspace %s for shards [%s] to shards [%s]",
			dr.ts.SourceKeyspaceName(), dr.ts.TargetKeyspaceName(), strings.Join(sourceShards, ","), strings.Join(targetShards, ","))
	} else {
		dr.drLog.Logf("Switch reads from keyspace %s to keyspace %s for shards [%s] to shards [%s]",
			dr.ts.TargetKeyspaceName(), dr.ts.SourceKeyspaceName(), strings.Join(targetShards, ","), strings.Join(sourceShards, ","))
	}
	return nil
}

func (dr *switcherDryRun) switchTableReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, rebuildSrvVSchema bool, direction TrafficSwitchDirection) error {
	ks := dr.ts.TargetKeyspaceName()
	if direction == DirectionBackward {
		ks = dr.ts.SourceKeyspaceName()
	}
	var tabletTypes []string
	for _, servedType := range servedTypes {
		tabletTypes = append(tabletTypes, servedType.String())
	}
	sort.Strings(dr.ts.Tables()) // For deterministic output
	tables := strings.Join(dr.ts.Tables(), ",")
	dr.drLog.Logf("Switch reads for tables [%s] to keyspace %s for tablet types [%s]", tables, ks, strings.Join(tabletTypes, ","))
	dr.drLog.Logf("Routing rules for tables [%s] will be updated", tables)
	if rebuildSrvVSchema {
		dr.drLog.Logf("Serving VSchema will be rebuilt for the %s keyspace", ks)
	}
	return nil
}

func (dr *switcherDryRun) createJournals(ctx context.Context, sourceWorkflows []string) error {
	dr.drLog.Log("Create journal entries on source databases")
	sort.Strings(sourceWorkflows) // For deterministic output
	if len(sourceWorkflows) > 0 {
		dr.drLog.Logf("Source workflows found: [%s]", strings.Join(sourceWorkflows, ","))
	}
	return nil
}

func (dr *switcherDryRun) allowTargetWrites(ctx context.Context) error {
	sort.Strings(dr.ts.Tables()) // For deterministic output
	dr.drLog.Logf("Enable writes on keyspace %s for tables [%s]", dr.ts.TargetKeyspaceName(), strings.Join(dr.ts.Tables(), ","))
	return nil
}

func (dr *switcherDryRun) changeRouting(ctx context.Context) error {
	dr.drLog.Logf("Switch routing from keyspace %s to keyspace %s", dr.ts.SourceKeyspaceName(), dr.ts.TargetKeyspaceName())
	var deleteLogs, addLogs []string
	if dr.ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		sort.Strings(dr.ts.Tables()) // For deterministic output
		tables := strings.Join(dr.ts.Tables(), ",")
		dr.drLog.Logf("Routing rules for tables [%s] will be updated", tables)
		return nil
	}
	deleteLogs = nil
	addLogs = nil
	sources := maps.Values(dr.ts.Sources())
	// Sort the slice for deterministic output.
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetPrimary().Alias.Uid < sources[j].GetPrimary().Alias.Uid
	})
	for _, source := range sources {
		deleteLogs = append(deleteLogs, fmt.Sprintf("shard:%s;tablet:%d", source.GetShard().ShardName(), source.GetShard().PrimaryAlias.Uid))
	}
	targets := maps.Values(dr.ts.Targets())
	// Sort the slice for deterministic output.
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetPrimary().Alias.Uid < targets[j].GetPrimary().Alias.Uid
	})
	for _, target := range targets {
		addLogs = append(addLogs, fmt.Sprintf("shard:%s;tablet:%d", target.GetShard().ShardName(), target.GetShard().PrimaryAlias.Uid))
	}
	if len(deleteLogs) > 0 {
		dr.drLog.Logf("IsPrimaryServing will be set to false for: [%s]", strings.Join(deleteLogs, ","))
		dr.drLog.Logf("IsPrimaryServing will be set to true for: [%s]", strings.Join(addLogs, ","))
	}
	return nil
}

func (dr *switcherDryRun) streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error {
	logs := make([]string, 0)
	targets := maps.Values(ts.Targets())
	// Sort the slice for deterministic output.
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetPrimary().Alias.Uid < targets[j].GetPrimary().Alias.Uid
	})
	for _, t := range targets {
		logs = append(logs, fmt.Sprintf("tablet:%d", t.GetPrimary().Alias.Uid))
	}
	dr.drLog.Logf("Switch writes completed, freeze and delete vreplication streams on: [%s]", strings.Join(logs, ","))
	return nil
}

func (dr *switcherDryRun) startReverseVReplication(ctx context.Context) error {
	logs := make([]string, 0)
	sources := maps.Values(dr.ts.Sources())
	// Sort the slice for deterministic output.
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetPrimary().Alias.Uid < sources[j].GetPrimary().Alias.Uid
	})
	for _, t := range sources {
		logs = append(logs, fmt.Sprintf("tablet:%d", t.GetPrimary().Alias.Uid))
	}
	dr.drLog.Logf("Start reverse vreplication streams on: [%s]", strings.Join(logs, ","))
	return nil
}

func (dr *switcherDryRun) createReverseVReplication(ctx context.Context) error {
	dr.drLog.Logf("Create reverse vreplication workflow %s", dr.ts.ReverseWorkflowName())
	return nil
}

func (dr *switcherDryRun) migrateStreams(ctx context.Context, sm *StreamMigrator) error {
	templates := sm.Templates()

	if len(templates) == 0 {
		return nil
	}
	logs := make([]string, 0)

	dr.drLog.Logf("Migrate streams to %s:", dr.ts.TargetKeyspaceName())
	allStreams := sm.Streams()
	// Sort the keys and slices for deterministic output.
	shards := maps.Keys(sm.Streams())
	sort.Strings(shards)
	for _, shard := range shards {
		shardStreams := allStreams[shard]
		sort.Slice(shardStreams, func(i, j int) bool {
			return shardStreams[i].ID < shardStreams[j].ID
		})
		for _, stream := range shardStreams {
			logs = append(logs, fmt.Sprintf("shard:%s;id:%d;workflow:%s;position:%s;binlogsource:%v", shard, stream.ID, stream.Workflow, replication.EncodePosition(stream.Position), stream.BinlogSource))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Migrate source streams: [%s]", strings.Join(logs, ","))
		logs = nil
	}
	// Sort the keys and slices for deterministic output.
	targets := maps.Values(dr.ts.Targets())
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetPrimary().Alias.Uid < targets[j].GetPrimary().Alias.Uid
	})
	for _, target := range targets {
		tabletStreams := templates
		sort.Slice(tabletStreams, func(i, j int) bool {
			return tabletStreams[i].ID < tabletStreams[j].ID
		})
		for _, vrs := range tabletStreams {
			logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;tablet:%d;workflow:%s;id:%d,position:%v;binlogsource:%s",
				vrs.BinlogSource.Keyspace, vrs.BinlogSource.Shard, target.GetPrimary().Alias.Uid, vrs.Workflow, vrs.ID, replication.EncodePosition(vrs.Position), vrs.BinlogSource))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Create target streams (as stopped): [%s]", strings.Join(logs, ","))
	}
	return nil
}

func (dr *switcherDryRun) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	dr.drLog.Logf("Wait for vreplication on stopped streams to catchup for up to %v", filteredReplicationWaitTime)
	return nil
}

func (dr *switcherDryRun) stopSourceWrites(ctx context.Context) error {
	logs := make([]string, 0)
	sources := maps.Values(dr.ts.Sources())
	// Sort the slice for deterministic output.
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetPrimary().Alias.Uid < sources[j].GetPrimary().Alias.Uid
	})
	for _, source := range sources {
		position, _ := dr.ts.TabletManagerClient().PrimaryPosition(ctx, source.GetPrimary().Tablet)
		logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;position:%s", dr.ts.SourceKeyspaceName(), source.GetShard().ShardName(), position))
	}
	sort.Strings(dr.ts.Tables()) // For deterministic output
	if len(logs) > 0 {
		dr.drLog.Logf("Stop writes on keyspace %s for tables [%s]: [%s]", dr.ts.SourceKeyspaceName(),
			strings.Join(dr.ts.Tables(), ","), strings.Join(logs, ","))
	}
	return nil
}

func (dr *switcherDryRun) stopStreams(ctx context.Context, sm *StreamMigrator) ([]string, error) {
	logs := make([]string, 0)
	allStreams := sm.Streams()
	// Sort the keys and slices for deterministic output.
	shards := maps.Keys(sm.Streams())
	sort.Strings(shards)
	for _, shard := range shards {
		shardStreams := allStreams[shard]
		sort.Slice(shardStreams, func(i, j int) bool {
			return shardStreams[i].ID < shardStreams[j].ID
		})
		for _, stream := range shardStreams {
			logs = append(logs, fmt.Sprintf("id:%d;keyspace:%s;shard:%s;rules:%s;position:%v",
				stream.ID, stream.BinlogSource.Keyspace, stream.BinlogSource.Shard, stream.BinlogSource.Filter, stream.Position))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Stop streams on keyspace %s: [%s]", dr.ts.SourceKeyspaceName(), strings.Join(logs, ","))
	}
	return nil, nil
}

func (dr *switcherDryRun) cancelMigration(ctx context.Context, sm *StreamMigrator) {
	dr.drLog.Log("Cancel migration as requested")
}

func (dr *switcherDryRun) lockKeyspace(ctx context.Context, keyspace, _ string, _ ...topo.LockOption) (context.Context, func(*error), error) {
	dr.drLog.Logf("Lock keyspace %s", keyspace)
	return ctx, func(e *error) {
		dr.drLog.Logf("Unlock keyspace %s", keyspace)
	}, nil
}

func (dr *switcherDryRun) removeSourceTables(ctx context.Context, removalType TableRemovalType) error {
	logs := make([]string, 0)
	sort.Strings(dr.ts.Tables()) // For deterministic output
	sources := maps.Values(dr.ts.Sources())
	// Sort the slice for deterministic output.
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetPrimary().Alias.Uid < sources[j].GetPrimary().Alias.Uid
	})
	for _, source := range sources {
		for _, tableName := range dr.ts.Tables() {
			logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;dbname:%s;tablet:%d;table:%s",
				source.GetPrimary().Keyspace, source.GetPrimary().Shard, source.GetPrimary().DbName(), source.GetPrimary().Alias.Uid, tableName))
		}
	}
	action := "Dropping"
	if removalType == RenameTable {
		action = "Renaming"
	}
	if len(logs) > 0 {
		dr.drLog.Logf("%s these tables from the database and removing them from the vschema for keyspace %s: [%s]",
			action, dr.ts.SourceKeyspaceName(), strings.Join(logs, ","))
	}
	return nil
}

func (dr *switcherDryRun) dropSourceShards(ctx context.Context) error {
	logs := make([]string, 0)
	tabletsList := make(map[string][]string)
	// Sort the slice for deterministic output.
	sourceShards := dr.ts.SourceShards()
	sort.Slice(sourceShards, func(i, j int) bool {
		return sourceShards[i].PrimaryAlias.Uid < sourceShards[j].PrimaryAlias.Uid
	})
	for _, si := range sourceShards {
		tabletAliases, err := dr.ts.TopoServer().FindAllTabletAliasesInShard(ctx, si.Keyspace(), si.ShardName())
		if err != nil {
			return err
		}
		tabletsList[si.ShardName()] = make([]string, 0)
		for _, t := range tabletAliases {
			tabletsList[si.ShardName()] = append(tabletsList[si.ShardName()], fmt.Sprintf("%d", t.Uid))
		}
		sort.Strings(tabletsList[si.ShardName()]) // For deterministic output
		logs = append(logs, fmt.Sprintf("cell:%s;keyspace:%s;shards:[%s]",
			si.Shard.PrimaryAlias.Cell, si.Keyspace(), si.ShardName()), strings.Join(tabletsList[si.ShardName()], ","))
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Delete shards (and all related tablets): [%s]", strings.Join(logs, ","))
	}

	return nil
}

func (dr *switcherDryRun) validateWorkflowHasCompleted(ctx context.Context) error {
	return doValidateWorkflowHasCompleted(ctx, dr.ts)
}

func (dr *switcherDryRun) dropTargetVReplicationStreams(ctx context.Context) error {
	logs := make([]string, 0)
	// Sort the keys and slices for deterministic output.
	targets := maps.Values(dr.ts.Targets())
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetPrimary().Alias.Uid < targets[j].GetPrimary().Alias.Uid
	})
	for _, t := range targets {
		logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;workflow:%s;dbname:%s;tablet:%d",
			t.GetShard().Keyspace(), t.GetShard().ShardName(), dr.ts.WorkflowName(), t.GetPrimary().DbName(), t.GetPrimary().Alias.Uid))
	}
	dr.drLog.Logf("Delete vreplication streams on targets: [%s]", strings.Join(logs, ","))
	return nil
}

func (dr *switcherDryRun) dropSourceReverseVReplicationStreams(ctx context.Context) error {
	logs := make([]string, 0)
	sources := maps.Values(dr.ts.Sources())
	// Sort the slice for deterministic output.
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetPrimary().Alias.Uid < sources[j].GetPrimary().Alias.Uid
	})
	for _, t := range sources {
		logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;workflow:%s;dbname:%s;tablet:%d",
			t.GetShard().Keyspace(), t.GetShard().ShardName(), ReverseWorkflowName(dr.ts.WorkflowName()), t.GetPrimary().DbName(), t.GetPrimary().Alias.Uid))
	}
	dr.drLog.Logf("Delete reverse vreplication streams on sources: [%s]", strings.Join(logs, ","))
	return nil
}

func (dr *switcherDryRun) freezeTargetVReplication(ctx context.Context) error {
	logs := make([]string, 0)
	// Sort the keys and slices for deterministic output.
	targets := maps.Values(dr.ts.Targets())
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetPrimary().Alias.Uid < targets[j].GetPrimary().Alias.Uid
	})
	for _, target := range targets {
		logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;tablet:%d;workflow:%s;dbname:%s",
			target.GetPrimary().Keyspace, target.GetPrimary().Shard, target.GetPrimary().Alias.Uid, dr.ts.WorkflowName(), target.GetPrimary().DbName()))
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Mark vreplication streams frozen on: [%s]", strings.Join(logs, ","))
	}
	return nil
}

func (dr *switcherDryRun) dropSourceDeniedTables(ctx context.Context) error {
	logs := make([]string, 0)
	// Sort the slice for deterministic output.
	sourceShards := dr.ts.SourceShards()
	sort.Slice(sourceShards, func(i, j int) bool {
		return sourceShards[i].PrimaryAlias.Uid < sourceShards[j].PrimaryAlias.Uid
	})
	for _, si := range sourceShards {
		logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;tablet:%d", si.Keyspace(), si.ShardName(), si.PrimaryAlias.Uid))
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Denied tables records on [%s] will be removed from: [%s]", strings.Join(dr.ts.Tables(), ","), strings.Join(logs, ","))
	}
	return nil
}

func (dr *switcherDryRun) dropTargetDeniedTables(ctx context.Context) error {
	logs := make([]string, 0)
	// Sort the slice for deterministic output.
	targetShards := dr.ts.TargetShards()
	sort.Slice(targetShards, func(i, j int) bool {
		return targetShards[i].PrimaryAlias.Uid < targetShards[j].PrimaryAlias.Uid
	})
	for _, si := range targetShards {
		logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;tablet:%d", si.Keyspace(), si.ShardName(), si.PrimaryAlias.Uid))
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Denied tables records on [%s] will be removed from: [%s]", strings.Join(dr.ts.Tables(), ","), strings.Join(logs, ","))
	}
	return nil
}

func (dr *switcherDryRun) logs() *[]string {
	return &dr.drLog.logs
}

func (dr *switcherDryRun) removeTargetTables(ctx context.Context) error {
	logs := make([]string, 0)
	sort.Strings(dr.ts.Tables()) // For deterministic output
	// Sort the keys and slices for deterministic output.
	targets := maps.Values(dr.ts.Targets())
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetPrimary().Alias.Uid < targets[j].GetPrimary().Alias.Uid
	})
	for _, target := range targets {
		for _, tableName := range dr.ts.Tables() {
			logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;dbname:%s;tablet:%d;table:%s",
				target.GetPrimary().Keyspace, target.GetPrimary().Shard, target.GetPrimary().DbName(), target.GetPrimary().Alias.Uid, tableName))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Dropping these tables from the database and removing from the vschema for keyspace %s: [%s]",
			dr.ts.TargetKeyspaceName(), strings.Join(logs, ","))
	}
	return nil
}

func (dr *switcherDryRun) dropTargetShards(ctx context.Context) error {
	logs := make([]string, 0)
	tabletsList := make(map[string][]string)
	sort.Strings(dr.ts.Tables()) // For deterministic output
	// Sort the slice for deterministic output.
	targetShards := dr.ts.TargetShards()
	sort.Slice(targetShards, func(i, j int) bool {
		return targetShards[i].PrimaryAlias.Uid < targetShards[j].PrimaryAlias.Uid
	})
	for _, si := range targetShards {
		tabletAliases, err := dr.ts.TopoServer().FindAllTabletAliasesInShard(ctx, si.Keyspace(), si.ShardName())
		if err != nil {
			return err
		}
		tabletsList[si.ShardName()] = make([]string, 0)
		for _, t := range tabletAliases {
			tabletsList[si.ShardName()] = append(tabletsList[si.ShardName()], fmt.Sprintf("%d", t.Uid))
		}
		sort.Strings(tabletsList[si.ShardName()]) // For deterministic output
		logs = append(logs, fmt.Sprintf("cell:%s;keyspace:%s;shards:[%s]",
			si.Shard.PrimaryAlias.Cell, si.Keyspace(), si.ShardName()), strings.Join(tabletsList[si.ShardName()], ","))
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Delete shards (and all related tablets): [%s]", strings.Join(logs, ","))
	}

	return nil
}

func (dr *switcherDryRun) resetSequences(ctx context.Context) error {
	var err error
	mustReset := false
	if mustReset, err = dr.ts.mustResetSequences(ctx); err != nil {
		return err
	}
	if !mustReset {
		return nil
	}
	dr.drLog.Log("The sequence caches will be reset on the source since sequence tables are being moved")
	return nil
}

func (dr *switcherDryRun) initializeTargetSequences(ctx context.Context, sequencesByBackingTable map[string]*sequenceMetadata) error {
	// Sort keys for deterministic output.
	sortedBackingTableNames := maps.Keys(sequencesByBackingTable)
	slices.Sort(sortedBackingTableNames)
	dr.drLog.Log(fmt.Sprintf("The following sequence backing tables used by tables being moved will be initialized: %s",
		strings.Join(sortedBackingTableNames, ",")))
	return nil
}
