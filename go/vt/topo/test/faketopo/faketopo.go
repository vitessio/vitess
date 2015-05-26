// Package faketopo contains utitlities for tests that have to interact with a
// Vitess topology.
package faketopo

import (
	"errors"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

var errNotImplemented = errors.New("Not implemented")

// FakeTopo is a topo.Server implementation that always returns errNotImplemented errors.
type FakeTopo struct{}

func (ft FakeTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topo.SrvKeyspace, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) Close() {}

func (ft FakeTopo) GetKnownCells(ctx context.Context) ([]string, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) CreateKeyspace(ctx context.Context, keyspace string, value *topo.Keyspace) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateKeyspace(ctx context.Context, ki *topo.KeyspaceInfo, existingVersion int64) (int64, error) {
	return 0, errNotImplemented
}

func (ft FakeTopo) GetKeyspace(ctx context.Context, keyspace string) (*topo.KeyspaceInfo, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetKeyspaces(ctx context.Context) ([]string, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) DeleteKeyspaceShards(ctx context.Context, keyspace string) error {
	return errNotImplemented
}

func (ft FakeTopo) CreateShard(ctx context.Context, keyspace, shard string, value *topo.Shard) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateShard(ctx context.Context, si *topo.ShardInfo, existingVersion int64) (int64, error) {
	return 0, errNotImplemented
}

func (ft FakeTopo) ValidateShard(ctx context.Context, keyspace, shard string) error {
	return errNotImplemented
}

func (ft FakeTopo) GetShard(ctx context.Context, keyspace, shard string) (*topo.ShardInfo, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) DeleteShard(ctx context.Context, keyspace, shard string) error {
	return errNotImplemented
}

func (ft FakeTopo) CreateTablet(ctx context.Context, tablet *topo.Tablet) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateTablet(ctx context.Context, tablet *topo.TabletInfo, existingVersion int64) (newVersion int64, err error) {
	return 0, errNotImplemented
}

func (ft FakeTopo) UpdateTabletFields(ctx context.Context, tabletAlias topo.TabletAlias, update func(*topo.Tablet) error) error {
	return errNotImplemented
}

func (ft FakeTopo) DeleteTablet(ctx context.Context, alias topo.TabletAlias) error {
	return errNotImplemented
}

func (ft FakeTopo) GetTablet(ctx context.Context, alias topo.TabletAlias) (*topo.TabletInfo, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetTabletsByCell(ctx context.Context, cell string) ([]topo.TabletAlias, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*topo.ShardReplication) error) error {
	return errNotImplemented
}

func (ft FakeTopo) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) DeleteShardReplication(ctx context.Context, cell, keyspace, shard string) error {
	return errNotImplemented
}

func (ft FakeTopo) LockSrvShardForAction(ctx context.Context, cell, keyspace, shard, contents string) (string, error) {
	return "", errNotImplemented
}

func (ft FakeTopo) UnlockSrvShardForAction(ctx context.Context, cell, keyspace, shard, lockPath, results string) error {
	return errNotImplemented
}

func (ft FakeTopo) GetSrvTabletTypesPerShard(ctx context.Context, cell, keyspace, shard string) ([]topo.TabletType, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) UpdateEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topo.TabletType, addrs *topo.EndPoints) error {
	return errNotImplemented
}

func (ft FakeTopo) DeleteEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topo.TabletType) error {
	return errNotImplemented
}

func (ft FakeTopo) WatchEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topo.TabletType) (<-chan *topo.EndPoints, chan<- struct{}, error) {
	return nil, nil, errNotImplemented
}

func (ft FakeTopo) UpdateSrvShard(ctx context.Context, cell, keyspace, shard string, srvShard *topo.SrvShard) error {
	return errNotImplemented
}

func (ft FakeTopo) GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*topo.SrvShard, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) DeleteSrvShard(ctx context.Context, cell, keyspace, shard string) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topo.SrvKeyspace) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateTabletEndpoint(ctx context.Context, cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.EndPoint) error {
	return errNotImplemented
}

func (ft FakeTopo) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	return "", errNotImplemented
}

func (ft FakeTopo) UnlockKeyspaceForAction(ctx context.Context, keyspace, lockPath, results string) error {
	return errNotImplemented
}

func (ft FakeTopo) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	return "", errNotImplemented
}

func (ft FakeTopo) UnlockShardForAction(ctx context.Context, keyspace, shard, lockPath, results string) error {
	return errNotImplemented
}
