// Command to generate a mock for this interface with mockgen.
//go:generate mockgen -source resharding_wrangler.go -destination mock_resharding_wrangler.go -package resharding

package resharding

import (
	"time"

	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// ReshardingWrangler is the interface to be used in creating mock interface for wrangler, which is used for unit test. It includes a subset of the methods in go/vt/Wrangler.
type ReshardingWrangler interface {
	CopySchemaShardFromShard(ctx context.Context, tables, excludeTables []string, includeViews bool, sourceKeyspace, sourceShard, destKeyspace, destShard string, waitSlaveTimeout time.Duration) error

	WaitForFilteredReplication(ctx context.Context, keyspace, shard string, maxDelay time.Duration) error

	MigrateServedTypes(ctx context.Context, keyspace, shard string, cells []string, servedType topodatapb.TabletType, reverse, skipReFreshState bool, filteredReplicationWaitTime time.Duration) error
}
