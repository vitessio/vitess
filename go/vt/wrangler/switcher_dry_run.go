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
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"golang.org/x/net/context"
)

var _ iswitcher = (*switcherDryRun)(nil)

type switcherDryRun struct {
	drLog *LogRecorder
	ts    *trafficSwitcher
}

func (dr *switcherDryRun) switchShardReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error {
	sourceShards := make([]string, 0)
	targetShards := make([]string, 0)
	for _, source := range dr.ts.sources {
		sourceShards = append(sourceShards, source.si.ShardName())
	}
	for _, target := range dr.ts.targets {
		targetShards = append(targetShards, target.si.ShardName())
	}
	sort.Strings(sourceShards)
	sort.Strings(targetShards)
	if direction == DirectionForward {
		dr.drLog.Log(fmt.Sprintf("Switch reads from keyspace %s to keyspace %s for shards %s to shards %s",
			dr.ts.sourceKeyspace, dr.ts.targetKeyspace, strings.Join(sourceShards, ","), strings.Join(targetShards, ",")))
	} else {
		dr.drLog.Log(fmt.Sprintf("Switch reads from keyspace %s to keyspace %s for shards %s to shards %s",
			dr.ts.targetKeyspace, dr.ts.sourceKeyspace, strings.Join(targetShards, ","), strings.Join(sourceShards, ",")))
	}
	return nil
}

func (dr *switcherDryRun) switchTableReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error {
	ks := dr.ts.targetKeyspace
	if direction == DirectionBackward {
		ks = dr.ts.sourceKeyspace
	}
	dr.drLog.Log(fmt.Sprintf("Switch reads for tables %s to keyspace %s", strings.Join(dr.ts.tables, ","), ks))
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
	dr.drLog.Log(fmt.Sprintf("Enable writes on keyspace %s tables %s", dr.ts.targetKeyspace, strings.Join(dr.ts.tables, ",")))
	return nil
}

func (dr *switcherDryRun) changeRouting(ctx context.Context) error {
	rules, err := dr.ts.wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	dr.drLog.Log(fmt.Sprintf("Switch routing from keyspace %s to keyspace %s", dr.ts.sourceKeyspace, dr.ts.targetKeyspace))
	deleteLogs := make([]string, 0)
	addLogs := make([]string, 0)
	if dr.ts.migrationType == binlogdatapb.MigrationType_TABLES {
		for _, table := range dr.ts.tables {
			for _, tabletType := range []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY} {
				tt := strings.ToLower(tabletType.String())
				deleteLogs = append(deleteLogs, fmt.Sprintf("\t%s => %s", table+"@"+tt, strings.Trim(rules[table+"@"+tt][0], "[]")))
				deleteLogs = append(deleteLogs, fmt.Sprintf("\t%s => %s", dr.ts.targetKeyspace+"."+table+"@"+tt, strings.Trim(rules[dr.ts.targetKeyspace+"."+table+"@"+tt][0], "[]")))
				deleteLogs = append(deleteLogs, fmt.Sprintf("\t%s => %s", dr.ts.sourceKeyspace+"."+table+"@"+tt, strings.Trim(rules[dr.ts.sourceKeyspace+"."+table+"@"+tt][0], "[]")))
			}
			addLogs = append(addLogs, fmt.Sprintf("\t%s => %s", table, dr.ts.targetKeyspace+"."+table))
			addLogs = append(addLogs, fmt.Sprintf("\t%s => %s", dr.ts.sourceKeyspace+"."+table, dr.ts.targetKeyspace+"."+table))
		}
		if len(deleteLogs) > 0 {
			dr.drLog.Log("Following rules will be deleted:")
			dr.drLog.LogSlice(deleteLogs)
			dr.drLog.Log("Following rules will be added:")
			dr.drLog.LogSlice(addLogs)
		}
		return nil
	}
	deleteLogs = nil
	addLogs = nil
	for _, source := range dr.ts.sources {
		deleteLogs = append(deleteLogs, fmt.Sprintf("\tShard %s, Tablet %d", source.si.ShardName(), source.si.MasterAlias.Uid))
	}
	for _, target := range dr.ts.targets {
		addLogs = append(addLogs, fmt.Sprintf("\tShard %s, Tablet %d", target.si.ShardName(), target.si.MasterAlias.Uid))
	}
	if len(deleteLogs) > 0 {
		dr.drLog.Log("IsMasterServing will be set to false for:")
		dr.drLog.LogSlice(deleteLogs)
		dr.drLog.Log("IsMasterServing will be set to true for:")
		dr.drLog.LogSlice(addLogs)
	}
	return nil
}

func (dr *switcherDryRun) streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error {
	dr.drLog.Log("SwitchWrites completed, freeze and delete vreplication streams on:")
	logs := make([]string, 0)
	for _, t := range ts.targets {
		logs = append(logs, fmt.Sprintf("\ttablet %d", t.master.Alias.Uid))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switcherDryRun) startReverseVReplication(ctx context.Context) error {
	dr.drLog.Log("Start reverse replication streams on:")
	logs := make([]string, 0)
	for _, t := range dr.ts.sources {
		logs = append(logs, fmt.Sprintf("\ttablet %d", t.master.Alias.Uid))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switcherDryRun) createReverseVReplication(ctx context.Context) error {
	dr.drLog.Log(fmt.Sprintf("Create reverse replication workflow %s", dr.ts.reverseWorkflow))
	return nil
}

func (dr *switcherDryRun) migrateStreams(ctx context.Context, sm *streamMigrater) error {
	if len(sm.templates) == 0 {
		return nil
	}
	logs := make([]string, 0)

	dr.drLog.Log(fmt.Sprintf("Migrate streams to %s:", dr.ts.targetKeyspace))
	for key, streams := range sm.streams {
		for _, stream := range streams {
			logs = append(logs, fmt.Sprintf("\tShard %s Id %d, Workflow %s, Pos %s, BinLogSource %v", key, stream.id, stream.workflow, mysql.EncodePosition(stream.pos), stream.bls))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log("Source streams will be migrated:")
		dr.drLog.LogSlice(logs)
		logs = nil
	}
	for _, target := range dr.ts.targets {
		tabletStreams := copyTabletStreams(sm.templates)
		for _, vrs := range tabletStreams {
			logs = append(logs, fmt.Sprintf("\t Keyspace %s, Shard %s, Tablet %d, Workflow %s, Id %d, Pos %v, BinLogSource %s",
				vrs.bls.Keyspace, vrs.bls.Shard, target.master.Alias.Uid, vrs.workflow, vrs.id, mysql.EncodePosition(vrs.pos), vrs.bls))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log("Target streams will be created (as stopped):")
		dr.drLog.LogSlice(logs)
	}
	return nil
}

func (dr *switcherDryRun) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	dr.drLog.Log(fmt.Sprintf("Wait for VReplication on stopped streams to catchup for upto %v", filteredReplicationWaitTime))
	return nil
}

func (dr *switcherDryRun) stopSourceWrites(ctx context.Context) error {
	logs := make([]string, 0)
	for _, source := range dr.ts.sources {
		position, _ := dr.ts.wr.tmc.MasterPosition(ctx, source.master.Tablet)
		logs = append(logs, fmt.Sprintf("\tKeyspace %s, Shard %s at Position %s", dr.ts.sourceKeyspace, source.si.ShardName(), position))
	}
	if len(logs) > 0 {
		dr.drLog.Log(fmt.Sprintf("Stop writes on keyspace %s, tables %s:", dr.ts.sourceKeyspace, strings.Join(dr.ts.tables, ",")))
		dr.drLog.LogSlice(logs)
	}
	return nil
}

func (dr *switcherDryRun) stopStreams(ctx context.Context, sm *streamMigrater) ([]string, error) {
	logs := make([]string, 0)
	for _, streams := range sm.streams {
		for _, stream := range streams {
			logs = append(logs, fmt.Sprintf("\tId %d Keyspace %s Shard %s Rules %s at Position %v",
				stream.id, stream.bls.Keyspace, stream.bls.Shard, stream.bls.Filter, stream.pos))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log(fmt.Sprintf("Stop streams on keyspace %s", dr.ts.sourceKeyspace))
		dr.drLog.LogSlice(logs)
	}
	return nil, nil
}

func (dr *switcherDryRun) cancelMigration(ctx context.Context, sm *streamMigrater) {
	dr.drLog.Log("Cancel stream migrations as requested")
}

func (dr *switcherDryRun) lockKeyspace(ctx context.Context, keyspace, _ string) (context.Context, func(*error), error) {
	dr.drLog.Log(fmt.Sprintf("Lock keyspace %s", keyspace))
	return ctx, func(e *error) {
		dr.drLog.Log(fmt.Sprintf("Unlock keyspace %s", keyspace))
	}, nil
}

func (dr *switcherDryRun) dropSourceTables(ctx context.Context) error {
	logs := make([]string, 0)
	for _, source := range dr.ts.sources {
		for _, tableName := range dr.ts.tables {
			logs = append(logs, fmt.Sprintf("\tKeyspace %s Shard %s DbName %s Tablet %d Table %s",
				source.master.Keyspace, source.master.Shard, source.master.DbName(), source.master.Alias.Uid, tableName))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log("Dropping following tables:")
		dr.drLog.LogSlice(logs)
	}
	return nil
}

func (dr *switcherDryRun) dropSourceShards(ctx context.Context) error {
	logs := make([]string, 0)
	tabletsList := make(map[string][]string)
	for _, si := range dr.ts.sourceShards() {
		tabletAliases, err := dr.ts.wr.TopoServer().FindAllTabletAliasesInShard(ctx, si.Keyspace(), si.ShardName())
		if err != nil {
			return err
		}
		tabletsList[si.ShardName()] = make([]string, 0)
		for _, t := range tabletAliases {
			tabletsList[si.ShardName()] = append(tabletsList[si.ShardName()], fmt.Sprintf("\t\t%d", t.Uid))
		}
		sort.Strings(tabletsList[si.ShardName()])
		logs = append(logs, fmt.Sprintf("\tCell %s Keyspace %s Shard\n%s",
			si.Shard.MasterAlias.Cell, si.Keyspace(), si.ShardName()), strings.Join(tabletsList[si.ShardName()], "\n"))
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
	for _, t := range dr.ts.targets {
		logs = append(logs, fmt.Sprintf("\tKeyspace %s Shard %s Workflow %s DbName %s Tablet %d",
			t.si.Keyspace(), t.si.ShardName(), dr.ts.workflow, t.master.DbName(), t.master.Alias.Uid))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switcherDryRun) dropSourceReverseVReplicationStreams(ctx context.Context) error {
	dr.drLog.Log("Delete reverse vreplication streams on source:")
	logs := make([]string, 0)
	for _, t := range dr.ts.sources {
		logs = append(logs, fmt.Sprintf("\tKeyspace %s Shard %s Workflow %s DbName %s Tablet %d",
			t.si.Keyspace(), t.si.ShardName(), reverseName(dr.ts.workflow), t.master.DbName(), t.master.Alias.Uid))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switcherDryRun) freezeTargetVReplication(ctx context.Context) error {
	logs := make([]string, 0)
	for _, target := range dr.ts.targets {
		logs = append(logs, fmt.Sprintf("\tKeyspace %s, Shard %s, Tablet %d, Workflow %s, DbName %s",
			target.master.Keyspace, target.master.Shard, target.master.Alias.Uid, dr.ts.workflow, target.master.DbName()))
	}
	if len(logs) > 0 {
		dr.drLog.Log("Mark vreplication streams frozen on:")
		dr.drLog.LogSlice(logs)
	}
	return nil
}

func (dr *switcherDryRun) dropSourceBlacklistedTables(ctx context.Context) error {
	logs := make([]string, 0)
	for _, si := range dr.ts.sourceShards() {
		logs = append(logs, fmt.Sprintf("\tKeyspace %s Shard %s Tablet %d", si.Keyspace(), si.ShardName(), si.MasterAlias.Uid))
	}
	if len(logs) > 0 {
		dr.drLog.Log(fmt.Sprintf("Blacklisted tables %s will be removed from:", strings.Join(dr.ts.tables, ",")))
		dr.drLog.LogSlice(logs)
	}
	return nil
}

func (dr *switcherDryRun) logs() *[]string {
	return &dr.drLog.logs
}
