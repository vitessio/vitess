// Command to generate a mock for this interface with mockgen.
//go:generate mockgen -source resharding_wrangler.go -destination mock_resharding_wrangler_test.go -package resharding -mock_names "Wrangler=MockReshardingWrangler"

package resharding

import (
	"time"

	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Wrangler is the interface to be used in creating mock interface for wrangler, which is used for unit test. It includes a subset of the methods in go/vt/Wrangler.
type Wrangler interface {
	CopySchemaShardFromShard(ctx context.Context, tables, excludeTables []string, includeViews bool, sourceKeyspace, sourceShard, destKeyspace, destShard string, waitReplicasTimeout time.Duration, skipVerify bool) error

	WaitForFilteredReplication(ctx context.Context, keyspace, shard string, maxDelay time.Duration) error

	MigrateServedTypes(ctx context.Context, keyspace, shard string, cells []string, servedType topodatapb.TabletType, reverse, skipReFreshState bool, filteredReplicationWaitTime time.Duration, reverseReplication bool) error
}
