package memorytopo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateShard implements topo.Impl.CreateShard
func (mt *MemoryTopo) CreateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard) error {
	data, err := proto.Marshal(value)
	if err != nil {
		return err
	}

	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	_, err = mt.Create(ctx, topo.GlobalCell, shardPath, data)
	return err
}

// UpdateShard implements topo.Impl.UpdateShard
func (mt *MemoryTopo) UpdateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard, existingVersion int64) (int64, error) {
	data, err := proto.Marshal(value)
	if err != nil {
		return -1, err
	}

	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	version, err := mt.Update(ctx, topo.GlobalCell, shardPath, data, VersionFromInt(existingVersion))
	if err != nil {
		return -1, err
	}
	return int64(version.(NodeVersion)), nil
}

// GetShard implements topo.Impl.GetShard
func (mt *MemoryTopo) GetShard(ctx context.Context, keyspace, shard string) (*topodatapb.Shard, int64, error) {
	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	data, version, err := mt.Get(ctx, topo.GlobalCell, shardPath)
	if err != nil {
		return nil, 0, err
	}

	sh := &topodatapb.Shard{}
	if err = proto.Unmarshal(data, sh); err != nil {
		return nil, 0, fmt.Errorf("bad shard data: %v", err)
	}

	return sh, int64(version.(NodeVersion)), nil
}

// GetShardNames implements topo.Impl.GetShardNames
func (mt *MemoryTopo) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	shardsPath := path.Join(keyspacesPath, keyspace, shardsPath)
	children, err := mt.ListDir(ctx, topo.GlobalCell, shardsPath)
	if err == topo.ErrNoNode {
		// The directory doesn't exist, let's see if the keyspace
		// is here or not.
		_, _, kerr := mt.GetKeyspace(ctx, keyspace)
		if kerr == nil {
			// Keyspace is here, means no shards.
			return nil, nil
		}
		return nil, err
	}
	return children, err
}

// DeleteShard implements topo.Impl.DeleteShard
func (mt *MemoryTopo) DeleteShard(ctx context.Context, keyspace, shard string) error {
	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	return mt.Delete(ctx, topo.GlobalCell, shardPath, nil)
}
