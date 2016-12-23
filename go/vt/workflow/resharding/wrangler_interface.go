package resharding

import (
	"time"

	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

//Wrangler is the interface to be used in creating mock interface for wrangler, which is used for unit test.
type Wrangler interface {
	CopySchemaShardFromShard(ctx context.Context, tables, excludeTables []string, includeViews bool, sourceKeyspace, sourceShard, destKeyspace, destShard string, waitSlaveTimeout time.Duration) error
	WaitForFilteredReplication(ctx context.Context, keyspace, shard string) error
	MigrateServedTypes(ctx context.Context, keyspace, shard string, cells []string, servedType topodatapb.TabletType, reverse, skipReFreshState bool, filteredReplicationWaitTime time.Duration) error
}

// Command to generate a mock for this interface with mockgen.
// mockgen -source wrangler_interface.go -package resharding > mock_wrangler_interface.go
