package wrangler

import (
	"fmt"
	"sort"
	"strings"
	"time"

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
	dr.drLog.Log(fmt.Sprintf("Switch routing from keyspace %s to keyspace %s", dr.ts.sourceKeyspace, dr.ts.targetKeyspace))
	return nil
}

func (dr *switchDryRun) streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error {
	dr.drLog.Log("SwitchWrites completed, freeze and delete migration streams on:")
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
	dr.drLog.Log(fmt.Sprintf("Crate reverse replication workflow %s", dr.ts.reverseWorkflow))
	return nil
}

func (dr *switchDryRun) migrateStreams(ctx context.Context, sm *streamMigrater) error {
	dr.drLog.Log(fmt.Sprintf("Migrate streams to %s", dr.ts.targetKeyspace))
	return nil
}

func (dr *switchDryRun) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	dr.drLog.Log(fmt.Sprintf("Wait for VReplication on all streams to catchup for upto %v", filteredReplicationWaitTime))
	return nil
}

func (dr *switchDryRun) stopSourceWrites(ctx context.Context) error {
	dr.drLog.Log(fmt.Sprintf("Stop writes on keyspace %s, tables %s", dr.ts.sourceKeyspace, strings.Join(dr.ts.tables, ",")))
	return nil
}

func (dr *switchDryRun) stopStreams(ctx context.Context, sm *streamMigrater) ([]string, error) {
	if len(sm.streams) > 0 {
		dr.drLog.Log(fmt.Sprintf("Stop streams on keyspace %s", dr.ts.sourceKeyspace))
		for key, streams := range sm.streams {
			for _, stream := range streams {
				dr.drLog.Log(fmt.Sprintf("\tkey %s shard %s stream %+v", key, stream.bls.Shard, stream.bls))
			}
		}
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
	dr.drLog.Log("Delete left-over streams")
	return nil
}
