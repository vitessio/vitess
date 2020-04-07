package wrangler

import (
	"fmt"
	"strings"
	"time"

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

	Logs() *[]string
}

var _ switcher = (*switchForReal)(nil)

type switchForReal struct {
	ts *trafficSwitcher
	wr *Wrangler
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

func (dr *switchDryRun) createReverseVReplication(ctx context.Context) error {
	dr.drLog.Log(fmt.Sprintf("Reverse replication workflow %s will be created", dr.ts.reverseWorkflow))
	return nil
}

func (dr *switchDryRun) migrateStreams(ctx context.Context, sm *streamMigrater) error {
	dr.drLog.Log(fmt.Sprintf("Streams will be migrated to %s", dr.ts.targetKeyspace))
	return nil
}

func (dr *switchDryRun) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	dr.drLog.Log(fmt.Sprintf("Will wait for VReplication on all streams to catchup for upto %v", filteredReplicationWaitTime))
	return nil
}

func (dr *switchDryRun) stopSourceWrites(ctx context.Context) error {
	dr.drLog.Log(fmt.Sprintf("Writes will be stopped in keyspace %s, tables %s", dr.ts.sourceKeyspace, strings.Join(dr.ts.tables, ",")))
	return nil
}

func (dr *switchDryRun) stopStreams(ctx context.Context, sm *streamMigrater) ([]string, error) {
	if len(sm.streams) > 0 {
		dr.drLog.Log(fmt.Sprintf("Following streams will be stopped on keyspace %s", dr.ts.sourceKeyspace))
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
	dr.drLog.Log("Stream migrations will be cancelled as requested")
}

func (dr *switchDryRun) LockKeyspace(ctx context.Context, keyspace, _ string) (context.Context, func(*error), error) {
	dr.drLog.Log(fmt.Sprintf("Will lock keyspace %s", keyspace))
	return ctx, func(e *error) {
		dr.drLog.Log(fmt.Sprintf("Will unlock keyspace %s", keyspace))
	}, nil
}

func (dr *switchDryRun) deleteTargetVReplication(ctx context.Context) error {
	dr.drLog.Log("Replication has been frozen already. Left-over streams will be deleted")

	return nil
}
