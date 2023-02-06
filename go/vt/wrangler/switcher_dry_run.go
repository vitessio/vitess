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
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtctl/workflow"

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

func (dr *switcherDryRun) switchShardReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction workflow.TrafficSwitchDirection) error {
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
	if direction == workflow.DirectionForward {
		dr.drLog.Log(fmt.Sprintf("Switch reads from keyspace %s to keyspace %s for shards %s to shards %s",
			dr.ts.SourceKeyspaceName(), dr.ts.TargetKeyspaceName(), strings.Join(sourceShards, ","), strings.Join(targetShards, ",")))
	} else {
		dr.drLog.Log(fmt.Sprintf("Switch reads from keyspace %s to keyspace %s for shards %s to shards %s",
			dr.ts.TargetKeyspaceName(), dr.ts.SourceKeyspaceName(), strings.Join(targetShards, ","), strings.Join(sourceShards, ",")))
	}
	return nil
}

func (dr *switcherDryRun) switchTableReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction workflow.TrafficSwitchDirection) error {
	ks := dr.ts.TargetKeyspaceName()
	if direction == workflow.DirectionBackward {
		ks = dr.ts.SourceKeyspaceName()
	}
	var tabletTypes []string
	for _, servedType := range servedTypes {
		tabletTypes = append(tabletTypes, servedType.String())
	}
	tables := strings.Join(dr.ts.Tables(), ",")
	dr.drLog.Log(fmt.Sprintf("Switch reads for tables [%s] to keyspace %s for tablet types [%s]",
		tables, ks, strings.Join(tabletTypes, ",")))
	dr.drLog.Log(fmt.Sprintf("Routing rules for tables [%s] will be updated", tables))
	return nil
}

func (dr *switcherDryRun) createJournals(ctx context.Context, sourceWorkflows []string) error {
	dr.drLog.Log("Create journal entries on source databases")
	if len(sourceWorkflows) > 0 {
		dr.drLog.Log("Source workflows found: ")
		dr.drLog.LogSlice(sourceWorkflows)
	}
	return nil
}

func (dr *switcherDryRun) allowTargetWrites(ctx context.Context) error {
	dr.drLog.Log(fmt.Sprintf("Enable writes on keyspace %s tables [%s]", dr.ts.TargetKeyspaceName(), strings.Join(dr.ts.Tables(), ",")))
	return nil
}

func (dr *switcherDryRun) changeRouting(ctx context.Context) error {
	dr.drLog.Log(fmt.Sprintf("Switch routing from keyspace %s to keyspace %s", dr.ts.SourceKeyspaceName(), dr.ts.TargetKeyspaceName()))
	var deleteLogs, addLogs []string
	if dr.ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		tables := strings.Join(dr.ts.Tables(), ",")
		dr.drLog.Log(fmt.Sprintf("Routing rules for tables [%s] will be updated", tables))
		return nil
	}
	deleteLogs = nil
	addLogs = nil
	for _, source := range dr.ts.Sources() {
		deleteLogs = append(deleteLogs, fmt.Sprintf("\tShard %s, Tablet %d", source.GetShard().ShardName(), source.GetShard().PrimaryAlias.Uid))
	}
	for _, target := range dr.ts.Targets() {
		addLogs = append(addLogs, fmt.Sprintf("\tShard %s, Tablet %d", target.GetShard().ShardName(), target.GetShard().PrimaryAlias.Uid))
	}
	if len(deleteLogs) > 0 {
		dr.drLog.Log("IsPrimaryServing will be set to false for:")
		dr.drLog.LogSlice(deleteLogs)
		dr.drLog.Log("IsPrimaryServing will be set to true for:")
		dr.drLog.LogSlice(addLogs)
	}
	return nil
}

func (dr *switcherDryRun) streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error {
	dr.drLog.Log("Switch writes completed, freeze and delete vreplication streams on:")
	logs := make([]string, 0)
	for _, t := range ts.Targets() {
		logs = append(logs, fmt.Sprintf("\ttablet %d", t.GetPrimary().Alias.Uid))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switcherDryRun) startReverseVReplication(ctx context.Context) error {
	dr.drLog.Log("Start reverse replication streams on:")
	logs := make([]string, 0)
	for _, t := range dr.ts.Sources() {
		logs = append(logs, fmt.Sprintf("\ttablet %d", t.GetPrimary().Alias.Uid))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switcherDryRun) createReverseVReplication(ctx context.Context) error {
	dr.drLog.Log(fmt.Sprintf("Create reverse replication workflow %s", dr.ts.ReverseWorkflowName()))
	return nil
}

func (dr *switcherDryRun) migrateStreams(ctx context.Context, sm *workflow.StreamMigrator) error {
	templates := sm.Templates()

	if len(templates) == 0 {
		return nil
	}
	logs := make([]string, 0)

	dr.drLog.Log(fmt.Sprintf("Migrate streams to %s:", dr.ts.TargetKeyspaceName()))
	for key, streams := range sm.Streams() {
		for _, stream := range streams {
			logs = append(logs, fmt.Sprintf("\tShard %s Id %d, Workflow %s, Pos %s, BinLogSource %v", key, stream.ID, stream.Workflow, mysql.EncodePosition(stream.Position), stream.BinlogSource))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log("Source streams will be migrated:")
		dr.drLog.LogSlice(logs)
		logs = nil
	}
	for _, target := range dr.ts.Targets() {
		tabletStreams := templates
		for _, vrs := range tabletStreams {
			logs = append(logs, fmt.Sprintf("\t Keyspace %s, Shard %s, Tablet %d, Workflow %s, Id %d, Pos %v, BinLogSource %s",
				vrs.BinlogSource.Keyspace, vrs.BinlogSource.Shard, target.GetPrimary().Alias.Uid, vrs.Workflow, vrs.ID, mysql.EncodePosition(vrs.Position), vrs.BinlogSource))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log("Target streams will be created (as stopped):")
		dr.drLog.LogSlice(logs)
	}
	return nil
}

func (dr *switcherDryRun) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	dr.drLog.Log(fmt.Sprintf("Wait for VReplication on stopped streams to catchup for up to %v", filteredReplicationWaitTime))
	return nil
}

func (dr *switcherDryRun) stopSourceWrites(ctx context.Context) error {
	logs := make([]string, 0)
	for _, source := range dr.ts.Sources() {
		position, _ := dr.ts.TabletManagerClient().PrimaryPosition(ctx, source.GetPrimary().Tablet)
		logs = append(logs, fmt.Sprintf("\tKeyspace %s, Shard %s at Position %s", dr.ts.SourceKeyspaceName(), source.GetShard().ShardName(), position))
	}
	if len(logs) > 0 {
		dr.drLog.Log(fmt.Sprintf("Stop writes on keyspace %s, tables [%s]:", dr.ts.SourceKeyspaceName(), strings.Join(dr.ts.Tables(), ",")))
		dr.drLog.LogSlice(logs)
	}
	return nil
}

func (dr *switcherDryRun) stopStreams(ctx context.Context, sm *workflow.StreamMigrator) ([]string, error) {
	logs := make([]string, 0)
	for _, streams := range sm.Streams() {
		for _, stream := range streams {
			logs = append(logs, fmt.Sprintf("\tId %d Keyspace %s Shard %s Rules %s at Position %v",
				stream.ID, stream.BinlogSource.Keyspace, stream.BinlogSource.Shard, stream.BinlogSource.Filter, stream.Position))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log(fmt.Sprintf("Stop streams on keyspace %s", dr.ts.SourceKeyspaceName()))
		dr.drLog.LogSlice(logs)
	}
	return nil, nil
}

func (dr *switcherDryRun) cancelMigration(ctx context.Context, sm *workflow.StreamMigrator) {
	dr.drLog.Log("Cancel stream migrations as requested")
}

func (dr *switcherDryRun) lockKeyspace(ctx context.Context, keyspace, _ string) (context.Context, func(*error), error) {
	dr.drLog.Log(fmt.Sprintf("Lock keyspace %s", keyspace))
	return ctx, func(e *error) {
		dr.drLog.Log(fmt.Sprintf("Unlock keyspace %s", keyspace))
	}, nil
}

func (dr *switcherDryRun) removeSourceTables(ctx context.Context, removalType workflow.TableRemovalType) error {
	logs := make([]string, 0)
	for _, source := range dr.ts.Sources() {
		for _, tableName := range dr.ts.Tables() {
			logs = append(logs, fmt.Sprintf("\tKeyspace %s Shard %s DbName %s Tablet %d Table %s",
				source.GetPrimary().Keyspace, source.GetPrimary().Shard, source.GetPrimary().DbName(), source.GetPrimary().Alias.Uid, tableName))
		}
	}
	action := "Dropping"
	if removalType == workflow.RenameTable {
		action = "Renaming"
	}
	if len(logs) > 0 {
		dr.drLog.Log(fmt.Sprintf("%s these tables from the database and removing them from the vschema for keyspace %s:",
			action, dr.ts.SourceKeyspaceName()))
		dr.drLog.LogSlice(logs)
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
			tabletsList[si.ShardName()] = append(tabletsList[si.ShardName()], fmt.Sprintf("\t\t%d", t.Uid))
		}
		sort.Strings(tabletsList[si.ShardName()])
		logs = append(logs, fmt.Sprintf("\tCell %s Keyspace %s Shard\n%s",
			si.Shard.PrimaryAlias.Cell, si.Keyspace(), si.ShardName()), strings.Join(tabletsList[si.ShardName()], "\n"))
	}
	if len(logs) > 0 {
		dr.drLog.Log("Deleting following shards (and all related tablets):")
		dr.drLog.LogSlice(logs)
	}

	return nil
}

func (dr *switcherDryRun) validateWorkflowHasCompleted(ctx context.Context) error {
	return doValidateWorkflowHasCompleted(ctx, dr.ts)
}

func (dr *switcherDryRun) dropTargetVReplicationStreams(ctx context.Context) error {
	dr.drLog.Log("Delete vreplication streams on target:")
	logs := make([]string, 0)
	for _, t := range dr.ts.Targets() {
		logs = append(logs, fmt.Sprintf("\tKeyspace %s Shard %s Workflow %s DbName %s Tablet %d",
			t.GetShard().Keyspace(), t.GetShard().ShardName(), dr.ts.WorkflowName(), t.GetPrimary().DbName(), t.GetPrimary().Alias.Uid))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switcherDryRun) dropSourceReverseVReplicationStreams(ctx context.Context) error {
	dr.drLog.Log("Delete reverse vreplication streams on source:")
	logs := make([]string, 0)
	for _, t := range dr.ts.Sources() {
		logs = append(logs, fmt.Sprintf("\tKeyspace %s Shard %s Workflow %s DbName %s Tablet %d",
			t.GetShard().Keyspace(), t.GetShard().ShardName(), workflow.ReverseWorkflowName(dr.ts.WorkflowName()), t.GetPrimary().DbName(), t.GetPrimary().Alias.Uid))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switcherDryRun) freezeTargetVReplication(ctx context.Context) error {
	logs := make([]string, 0)
	for _, target := range dr.ts.Targets() {
		logs = append(logs, fmt.Sprintf("\tKeyspace %s, Shard %s, Tablet %d, Workflow %s, DbName %s",
			target.GetPrimary().Keyspace, target.GetPrimary().Shard, target.GetPrimary().Alias.Uid, dr.ts.WorkflowName(), target.GetPrimary().DbName()))
	}
	if len(logs) > 0 {
		dr.drLog.Log("Mark vreplication streams frozen on:")
		dr.drLog.LogSlice(logs)
	}
	return nil
}

func (dr *switcherDryRun) dropSourceDeniedTables(ctx context.Context) error {
	logs := make([]string, 0)
	for _, si := range dr.ts.SourceShards() {
		logs = append(logs, fmt.Sprintf("\tKeyspace %s Shard %s Tablet %d", si.Keyspace(), si.ShardName(), si.PrimaryAlias.Uid))
	}
	if len(logs) > 0 {
		dr.drLog.Log(fmt.Sprintf("Denied tables [%s] will be removed from:", strings.Join(dr.ts.Tables(), ",")))
		dr.drLog.LogSlice(logs)
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
			logs = append(logs, fmt.Sprintf("\tKeyspace %s Shard %s DbName %s Tablet %d Table %s",
				target.GetPrimary().Keyspace, target.GetPrimary().Shard, target.GetPrimary().DbName(), target.GetPrimary().Alias.Uid, tableName))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log(fmt.Sprintf("Dropping these tables from the database and removing from the vschema for keyspace %s:",
			dr.ts.TargetKeyspaceName()))
		dr.drLog.LogSlice(logs)
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
			tabletsList[si.ShardName()] = append(tabletsList[si.ShardName()], fmt.Sprintf("\t\t%d", t.Uid))
		}
		sort.Strings(tabletsList[si.ShardName()])
		logs = append(logs, fmt.Sprintf("\tCell %s Keyspace %s Shard\n%s",
			si.Shard.PrimaryAlias.Cell, si.Keyspace(), si.ShardName()), strings.Join(tabletsList[si.ShardName()], "\n"))
	}
	if len(logs) > 0 {
		dr.drLog.Log("Deleting following shards (and all related tablets):")
		dr.drLog.LogSlice(logs)
	}

	return nil
}
