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

type switcher interface {
	deleteTargetVReplication(ctx context.Context) error
	LockKeyspace(ctx context.Context, keyspace, action string) (context.Context, func(*error), error)
	cancelMigration(ctx context.Context, sm *streamMigrater)
	stopStreams(ctx context.Context, sm *streamMigrater) ([]string, error)
	stopSourceWrites(ctx context.Context) error
	waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error
	migrateStreams(ctx context.Context, sm *streamMigrater) error
	createReverseVReplication(ctx context.Context) error
	createJournals(ctx context.Context, sourceWorkflows []string) error
	allowTargetWrites(ctx context.Context) error
	changeRouting(ctx context.Context) error
	streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error
	startReverseVReplication(ctx context.Context) error
	switchTableReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error
	switchShardReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error
	Logs() *[]string
}

var _ switcher = (*switchForReal)(nil)

type switchForReal struct {
	ts *trafficSwitcher
	wr *Wrangler
}

func (r *switchForReal) switchShardReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error {
	return r.ts.switchShardReads(ctx, cells, servedType, direction)
}

func (r *switchForReal) switchTableReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error {
	return r.ts.switchTableReads(ctx, cells, servedType, direction)
}

func (r *switchForReal) startReverseVReplication(ctx context.Context) error {
	return r.ts.startReverseVReplication(ctx)
}

func (r *switchForReal) createJournals(ctx context.Context, sourceWorkflows []string) error {
	return r.ts.createJournals(ctx, sourceWorkflows)
}

func (r *switchForReal) allowTargetWrites(ctx context.Context) error {
	return r.ts.allowTargetWrites(ctx)
}

func (r *switchForReal) changeRouting(ctx context.Context) error {
	return r.ts.changeRouting(ctx)
}

func (r *switchForReal) streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error {
	return streamMigraterfinalize(ctx, ts, workflows)
}

func (r *switchForReal) createReverseVReplication(ctx context.Context) error {
	return r.ts.createReverseVReplication(ctx)
}

func (r *switchForReal) migrateStreams(ctx context.Context, sm *streamMigrater) error {
	return sm.migrateStreams(ctx)
}

func (r *switchForReal) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	return r.ts.waitForCatchup(ctx, filteredReplicationWaitTime)
}

func (r *switchForReal) stopSourceWrites(ctx context.Context) error {
	return r.ts.stopSourceWrites(ctx)
}

func (r *switchForReal) stopStreams(ctx context.Context, sm *streamMigrater) ([]string, error) {
	return sm.stopStreams(ctx)
}

func (r *switchForReal) Logs() *[]string {
	return nil
}

func (r *switchForReal) cancelMigration(ctx context.Context, sm *streamMigrater) {
	r.ts.wr.Logger().Infof("Cancel was requested.")
	r.ts.cancelMigration(ctx, sm)
}

func (r *switchForReal) LockKeyspace(ctx context.Context, keyspace, action string) (context.Context, func(*error), error) {
	return r.wr.ts.LockKeyspace(ctx, keyspace, action)
}

func (r *switchForReal) deleteTargetVReplication(ctx context.Context) error {
	r.ts.wr.Logger().Infof("Replication has been frozen already. Deleting left-over streams")
	if err := r.ts.deleteTargetVReplication(ctx); err != nil {
		r.ts.wr.Logger().Errorf("deleteTargetVReplication failed: %v", err)
		return err
	}
	return nil
}

var _ switcher = (*switchDryRun)(nil)

type switchDryRun struct {
	drLog *LogRecorder
	ts    *trafficSwitcher
}

func (dr *switchDryRun) switchShardReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error {
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

func (dr *switchDryRun) switchTableReads(ctx context.Context, cells []string, servedType topodatapb.TabletType, direction TrafficSwitchDirection) error {
	ks := dr.ts.targetKeyspace
	if direction == DirectionBackward {
		ks = dr.ts.sourceKeyspace
	}
	dr.drLog.Log(fmt.Sprintf("Switch reads for tables %s to keyspace %s", strings.Join(dr.ts.tables, ","), ks))
	return nil
}

func (dr *switchDryRun) createJournals(ctx context.Context, sourceWorkflows []string) error {
	dr.drLog.Log("Create journal entries on source databases")
	if len(sourceWorkflows) > 0 {
		dr.drLog.Log("Source workflows found: ")
		dr.drLog.LogSlice(sourceWorkflows)
	}
	return nil
}

func (dr *switchDryRun) allowTargetWrites(ctx context.Context) error {
	dr.drLog.Log(fmt.Sprintf("Enable writes on keyspace %s tables %s", dr.ts.targetKeyspace, strings.Join(dr.ts.tables, ",")))
	return nil
}

func (dr *switchDryRun) changeRouting(ctx context.Context) error {
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
		deleteLogs = append(deleteLogs, fmt.Sprintf("\tShard %s, Tablet %s", source.si.ShardName(), source.si.MasterAlias.String()))
	}
	for _, target := range dr.ts.targets {
		addLogs = append(addLogs, fmt.Sprintf("\tShard %s, Tablet %s", target.si.ShardName(), target.si.MasterAlias.String()))
	}
	if len(deleteLogs) > 0 {
		dr.drLog.Log("IsMasterServing will be set to false for:")
		dr.drLog.LogSlice(deleteLogs)
		dr.drLog.Log("IsMasterServing will be set to true for:")
		dr.drLog.LogSlice(addLogs)
	}
	return nil
}

func (dr *switchDryRun) streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error {
	dr.drLog.Log("SwitchWrites completed, freeze and delete vreplication streams on:")
	logs := make([]string, 0)
	for _, t := range ts.targets {
		logs = append(logs, fmt.Sprintf("\ttablet %s", t.master.Alias))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switchDryRun) startReverseVReplication(ctx context.Context) error {
	dr.drLog.Log("Start reverse replication streams on:")
	logs := make([]string, 0)
	for _, t := range dr.ts.sources {
		logs = append(logs, fmt.Sprintf("\ttablet %s", t.master.Alias))
	}
	dr.drLog.LogSlice(logs)
	return nil
}

func (dr *switchDryRun) createReverseVReplication(ctx context.Context) error {
	dr.drLog.Log(fmt.Sprintf("Create reverse replication workflow %s", dr.ts.reverseWorkflow))
	return nil
}

func (dr *switchDryRun) migrateStreams(ctx context.Context, sm *streamMigrater) error {
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
			logs = append(logs, fmt.Sprintf("\t Keyspace %s, Shard %s, Tablet %s, Workflow %s, Id %d, Pos %v, BinLogSource %s",
				vrs.bls.Keyspace, vrs.bls.Shard, target.master.Alias, vrs.workflow, vrs.id, mysql.EncodePosition(vrs.pos), vrs.bls))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log("Target streams will be created (as stopped):")
		dr.drLog.LogSlice(logs)
	}
	return nil
}

func (dr *switchDryRun) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	dr.drLog.Log(fmt.Sprintf("Wait for VReplication on stopped streams to catchup for upto %v", filteredReplicationWaitTime))
	return nil
}

func (dr *switchDryRun) stopSourceWrites(ctx context.Context) error {
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

func (dr *switchDryRun) stopStreams(ctx context.Context, sm *streamMigrater) ([]string, error) {
	logs := make([]string, 0)
	for _, streams := range sm.streams {
		for _, stream := range streams {
			logs = append(logs, fmt.Sprintf("\tId %d Shard %s at Position %v", stream.id, stream.bls.Shard, mysql.EncodePosition(stream.pos)))
		}
	}
	if len(logs) > 0 {
		dr.drLog.Log(fmt.Sprintf("Stop streams on keyspace %s", dr.ts.sourceKeyspace))
		dr.drLog.LogSlice(logs)
	}
	return nil, nil
}

func (dr *switchDryRun) Logs() *[]string {
	return &dr.drLog.logs
}

func (dr *switchDryRun) cancelMigration(ctx context.Context, sm *streamMigrater) {
	dr.drLog.Log("Cancel stream migrations as requested")
}

func (dr *switchDryRun) LockKeyspace(ctx context.Context, keyspace, _ string) (context.Context, func(*error), error) {
	dr.drLog.Log(fmt.Sprintf("Lock keyspace %s", keyspace))
	return ctx, func(e *error) {
		dr.drLog.Log(fmt.Sprintf("Unlock keyspace %s", keyspace))
	}, nil
}

func (dr *switchDryRun) deleteTargetVReplication(ctx context.Context) error {
	logs := make([]string, 0)
	for _, target := range dr.ts.targets {
		logs = append(logs, fmt.Sprintf("\tKeyspace %s, Shard %s, Tablet %d, Workflow %s, DbName %s",
			target.master.Keyspace, target.master.Shard, target.master.Alias.Uid, dr.ts.workflow, target.master.DbName()))
	}
	if len(logs) > 0 {
		dr.drLog.Log("Deleting vreplication streams on:")
		dr.drLog.LogSlice(logs)
	}
	return nil
}
