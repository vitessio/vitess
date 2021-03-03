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

type iswitcher interface {
	lockKeyspace(ctx context.Context, keyspace, action string) (context.Context, func(*error), error)
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
	switchTableReads(ctx context.Context, cells []string, servedType []topodatapb.TabletType, direction TrafficSwitchDirection) error
	switchShardReads(ctx context.Context, cells []string, servedType []topodatapb.TabletType, direction TrafficSwitchDirection) error
	validateWorkflowHasCompleted(ctx context.Context) error
	removeSourceTables(ctx context.Context, removalType TableRemovalType) error
	dropSourceShards(ctx context.Context) error
	dropSourceBlacklistedTables(ctx context.Context) error
	freezeTargetVReplication(ctx context.Context) error
	dropSourceReverseVReplicationStreams(ctx context.Context) error
	dropTargetVReplicationStreams(ctx context.Context) error
	removeTargetTables(ctx context.Context) error
	dropTargetShards(ctx context.Context) error
	deleteRoutingRules(ctx context.Context) error
	addParticipatingTablesToKeyspace(ctx context.Context, keyspace, tableSpecs string) error
	logs() *[]string
}
