package wrangler

import (
	"context"
	"time"

	"vitess.io/vitess/go/vt/vtctl/workflow"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type iswitcher interface {
	lockKeyspace(ctx context.Context, keyspace, action string) (context.Context, func(*error), error)
	cancelMigration(ctx context.Context, sm *workflow.StreamMigrator)
	stopStreams(ctx context.Context, sm *workflow.StreamMigrator) ([]string, error)
	stopSourceWrites(ctx context.Context) error
	waitForCatchup(ctx context.Context, filteredReplicationWaitTime time.Duration) error
	migrateStreams(ctx context.Context, sm *workflow.StreamMigrator) error
	createReverseVReplication(ctx context.Context) error
	createJournals(ctx context.Context, sourceWorkflows []string) error
	allowTargetWrites(ctx context.Context) error
	changeRouting(ctx context.Context) error
	streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error
	startReverseVReplication(ctx context.Context) error
	switchTableReads(ctx context.Context, cells []string, servedType []topodatapb.TabletType, direction workflow.TrafficSwitchDirection) error
	switchShardReads(ctx context.Context, cells []string, servedType []topodatapb.TabletType, direction workflow.TrafficSwitchDirection) error
	validateWorkflowHasCompleted(ctx context.Context) error
	removeSourceTables(ctx context.Context, removalType workflow.TableRemovalType) error
	dropSourceShards(ctx context.Context) error
	dropSourceDeniedTables(ctx context.Context) error
	freezeTargetVReplication(ctx context.Context) error
	dropSourceReverseVReplicationStreams(ctx context.Context) error
	dropTargetVReplicationStreams(ctx context.Context) error
	removeTargetTables(ctx context.Context) error
	dropTargetShards(ctx context.Context) error
	deleteRoutingRules(ctx context.Context) error
	addParticipatingTablesToKeyspace(ctx context.Context, keyspace, tableSpecs string) error
	logs() *[]string
}
