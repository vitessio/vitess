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
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"

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

func (dr *switcherDryRun) switchShardReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction TrafficSwitchDirection) error {
	sourceShards := make([]string, 0)
	targetShards := make([]string, 0)
	for _, source := range dr.ts.Sources() {
		sourceShards = append(sourceShards, source.GetShard().ShardName())
	}
	for _, target := range dr.ts.Targets() {
		targetShards = append(targetShards, target.GetShard().ShardName())
	}
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

func (dr *switcherDryRun) switchTableReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction TrafficSwitchDirection) error {
	ks := dr.ts.TargetKeyspaceName()
	if direction == DirectionBackward {
		ks = dr.ts.SourceKeyspaceName()
	}
	var tabletTypes []string
	for _, servedType := range servedTypes {
		tabletTypes = append(tabletTypes, servedType.String())
	}
	tables := strings.Join(dr.ts.Tables(), ",")
	dr.drLog.Logf("Switch reads for tables [%s] to keyspace %s for tablet types [%s]", tables, ks, strings.Join(tabletTypes, ","))
	dr.drLog.Logf("Routing rules for tables [%s] will be updated", tables)
	return nil
}

func (dr *switcherDryRun) createJournals(ctx context.Context, sourceWorkflows []string) error {
	dr.drLog.Log("Create journal entries on source databases")
	if len(sourceWorkflows) > 0 {
		dr.drLog.Logf("Source workflows found: [%s]", strings.Join(sourceWorkflows, ","))
	}
	return nil
}

func (dr *switcherDryRun) allowTargetWrites(ctx context.Context) error {
	dr.drLog.Logf("Enable writes on keyspace %s for tables [%s]", dr.ts.TargetKeyspaceName(), strings.Join(dr.ts.Tables(), ","))
	return nil
}

func (dr *switcherDryRun) changeRouting(ctx context.Context) error {
	dr.drLog.Logf("Switch routing from keyspace %s to keyspace %s", dr.ts.SourceKeyspaceName(), dr.ts.TargetKeyspaceName())
	var deleteLogs, addLogs []string
	if dr.ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		tables := strings.Join(dr.ts.Tables(), ",")
		dr.drLog.Logf("Routing rules for tables [%s] will be updated", tables)
		return nil
	}
	deleteLogs = nil
	addLogs = nil
	for _, source := range dr.ts.Sources() {
		deleteLogs = append(deleteLogs, fmt.Sprintf("shard:%s;tablet:%d", source.GetShard().ShardName(), source.GetShard().PrimaryAlias.Uid))
	}
	for _, target := range dr.ts.Targets() {
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
	for _, t := range ts.Targets() {
		logs = append(logs, fmt.Sprintf("tablet:%d", t.GetPrimary().Alias.Uid))
	}
	dr.drLog.Logf("Switch writes completed, freeze and delete vreplication streams on: [%s]", strings.Join(logs, ","))
	return nil
}

func (dr *switcherDryRun) startReverseVReplication(ctx context.Context) error {
	logs := make([]string, 0)
	for _, t := range dr.ts.Sources() {
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
	for key, streams := range sm.Streams() {
		for _, stream := range streams {
			logs = append(logs, fmt.Sprintf("shard:%s;id:%d;workflow:%s;position:%s;binlogsource:%v", key, stream.ID, stream.Workflow, mysql.EncodePosition(stream.Position), stream.BinlogSource))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Migrate source streams: [%s]", strings.Join(logs, ","))
		logs = nil
	}
	for _, target := range dr.ts.Targets() {
		tabletStreams := templates
		for _, vrs := range tabletStreams {
			logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;tablet:%d;workflow:%s;id:%d,position:%v;binlogsource:%s",
				vrs.BinlogSource.Keyspace, vrs.BinlogSource.Shard, target.GetPrimary().Alias.Uid, vrs.Workflow, vrs.ID, mysql.EncodePosition(vrs.Position), vrs.BinlogSource))
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
	for _, source := range dr.ts.Sources() {
		position, _ := dr.ts.TabletManagerClient().PrimaryPosition(ctx, source.GetPrimary().Tablet)
		logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;position:%s", dr.ts.SourceKeyspaceName(), source.GetShard().ShardName(), position))
	}
	if len(logs) > 0 {
		dr.drLog.Logf("Stop writes on keyspace %s for tables [%s]: [%s]", dr.ts.SourceKeyspaceName(),
			strings.Join(dr.ts.Tables(), ","), strings.Join(logs, ","))
	}
	return nil
}

func (dr *switcherDryRun) stopStreams(ctx context.Context, sm *StreamMigrator) ([]string, error) {
	logs := make([]string, 0)
	for _, streams := range sm.Streams() {
		for _, stream := range streams {
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
	dr.drLog.Log("Cancel stream migrations as requested")
}

func (dr *switcherDryRun) lockKeyspace(ctx context.Context, keyspace, _ string) (context.Context, func(*error), error) {
	dr.drLog.Logf("Lock keyspace %s", keyspace)
	return ctx, func(e *error) {
		dr.drLog.Logf("Unlock keyspace %s", keyspace)
	}, nil
}

func (dr *switcherDryRun) removeSourceTables(ctx context.Context, removalType TableRemovalType) error {
	logs := make([]string, 0)
	for _, source := range dr.ts.Sources() {
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
	for _, si := range dr.ts.SourceShards() {
		tabletAliases, err := dr.ts.TopoServer().FindAllTabletAliasesInShard(ctx, si.Keyspace(), si.ShardName())
		if err != nil {
			return err
		}
		tabletsList[si.ShardName()] = make([]string, 0)
		for _, t := range tabletAliases {
			tabletsList[si.ShardName()] = append(tabletsList[si.ShardName()], fmt.Sprintf("%d", t.Uid))
		}
		sort.Strings(tabletsList[si.ShardName()])
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
	for _, t := range dr.ts.Targets() {
		logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;workflow:%s;dbname:%s;tablet:%d",
			t.GetShard().Keyspace(), t.GetShard().ShardName(), dr.ts.WorkflowName(), t.GetPrimary().DbName(), t.GetPrimary().Alias.Uid))
	}
	dr.drLog.Logf("Delete vreplication streams on targets: [%s]", strings.Join(logs, ","))
	return nil
}

func (dr *switcherDryRun) dropSourceReverseVReplicationStreams(ctx context.Context) error {
	logs := make([]string, 0)
	for _, t := range dr.ts.Sources() {
		logs = append(logs, fmt.Sprintf("keyspace:%s;shard:%s;workflow:%s;dbname:%s;tablet:%d",
			t.GetShard().Keyspace(), t.GetShard().ShardName(), ReverseWorkflowName(dr.ts.WorkflowName()), t.GetPrimary().DbName(), t.GetPrimary().Alias.Uid))
	}
	dr.drLog.Logf("Delete reverse vreplication streams on sources: [%s]", strings.Join(logs, ","))
	return nil
}

func (dr *switcherDryRun) freezeTargetVReplication(ctx context.Context) error {
	logs := make([]string, 0)
	for _, target := range dr.ts.Targets() {
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
	for _, si := range dr.ts.SourceShards() {
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
	for _, target := range dr.ts.Targets() {
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
	for _, si := range dr.ts.TargetShards() {
		tabletAliases, err := dr.ts.TopoServer().FindAllTabletAliasesInShard(ctx, si.Keyspace(), si.ShardName())
		if err != nil {
			return err
		}
		tabletsList[si.ShardName()] = make([]string, 0)
		for _, t := range tabletAliases {
			tabletsList[si.ShardName()] = append(tabletsList[si.ShardName()], fmt.Sprintf("%d", t.Uid))
		}
		sort.Strings(tabletsList[si.ShardName()])
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
