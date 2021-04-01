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
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"context"
)

var _ iswitcher = (*switcher)(nil)

type switcher struct {
	ts *trafficSwitcher
	wr *Wrangler
}

func (r *switcher) addParticipatingTablesToKeyspace(ctx context.Context, keyspace, tableSpecs string) error {
	return r.ts.addParticipatingTablesToKeyspace(ctx, keyspace, tableSpecs)
}

func (r *switcher) deleteRoutingRules(ctx context.Context) error {
	return r.ts.deleteRoutingRules(ctx)
}

func (r *switcher) dropSourceBlacklistedTables(ctx context.Context) error {
	return r.ts.dropSourceBlacklistedTables(ctx)
}

func (r *switcher) validateWorkflowHasCompleted(ctx context.Context) error {
	return r.ts.validateWorkflowHasCompleted(ctx)
}

func (r *switcher) removeSourceTables(ctx context.Context, removalType TableRemovalType) error {
	return r.ts.removeSourceTables(ctx, removalType)
}

func (r *switcher) dropSourceShards(ctx context.Context) error {
	return r.ts.dropSourceShards(ctx)
}

func (r *switcher) switchShardReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction TrafficSwitchDirection) error {
	return r.ts.switchShardReads(ctx, cells, servedTypes, direction)
}

func (r *switcher) switchTableReads(ctx context.Context, cells []string, servedTypes []topodatapb.TabletType, direction TrafficSwitchDirection) error {
	return r.ts.switchTableReads(ctx, cells, servedTypes, direction)
}

func (r *switcher) startReverseVReplication(ctx context.Context) error {
	return r.ts.startReverseVReplication(ctx)
}

func (r *switcher) createJournals(ctx context.Context, sourceWorkflows []string) error {
	return r.ts.createJournals(ctx, sourceWorkflows)
}

func (r *switcher) allowTargetWrites(ctx context.Context) error {
	return r.ts.allowTargetWrites(ctx)
}

func (r *switcher) changeRouting(ctx context.Context) error {
	return r.ts.changeRouting(ctx)
}

func (r *switcher) streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error {
	return streamMigraterfinalize(ctx, ts, workflows)
}

func (r *switcher) createReverseVReplication(ctx context.Context) error {
	return r.ts.createReverseVReplication(ctx)
}

func (r *switcher) migrateStreams(ctx context.Context, sm *streamMigrater) error {
	return sm.migrateStreams(ctx)
}

func (r *switcher) waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	return r.ts.waitForCatchup(ctx, filteredReplicationWaitTime)
}

func (r *switcher) stopSourceWrites(ctx context.Context) error {
	return r.ts.stopSourceWrites(ctx)
}

func (r *switcher) stopStreams(ctx context.Context, sm *streamMigrater) ([]string, error) {
	return sm.stopStreams(ctx)
}

func (r *switcher) cancelMigration(ctx context.Context, sm *streamMigrater) {
	r.ts.wr.Logger().Infof("Cancel was requested.")
	r.ts.cancelMigration(ctx, sm)
}

func (r *switcher) lockKeyspace(ctx context.Context, keyspace, action string) (context.Context, func(*error), error) {
	return r.wr.ts.LockKeyspace(ctx, keyspace, action)
}

func (r *switcher) freezeTargetVReplication(ctx context.Context) error {
	return r.ts.freezeTargetVReplication(ctx)
}

func (r *switcher) dropTargetVReplicationStreams(ctx context.Context) error {
	return r.ts.dropTargetVReplicationStreams(ctx)
}

func (r *switcher) dropSourceReverseVReplicationStreams(ctx context.Context) error {
	return r.ts.dropSourceReverseVReplicationStreams(ctx)
}

func (r *switcher) removeTargetTables(ctx context.Context) error {
	return r.ts.removeTargetTables(ctx)
}

func (r *switcher) dropTargetShards(ctx context.Context) error {
	return r.ts.dropTargetShards(ctx)
}

func (r *switcher) logs() *[]string {
	return nil
}
