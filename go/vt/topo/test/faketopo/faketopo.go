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

func (ft FakeTopo) GetSrvKeyspaceNames(cell string) ([]string, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) Close() {}

func (ft FakeTopo) GetKnownCells() ([]string, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) CreateKeyspace(keyspace string, value *topo.Keyspace) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateKeyspace(ki *topo.KeyspaceInfo, existingVersion int64) (int64, error) {
	return 0, errNotImplemented
}

func (ft FakeTopo) GetKeyspace(keyspace string) (*topo.KeyspaceInfo, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetKeyspaces() ([]string, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) DeleteKeyspaceShards(keyspace string) error {
	return errNotImplemented
}

func (ft FakeTopo) CreateShard(keyspace, shard string, value *topo.Shard) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateShard(si *topo.ShardInfo, existingVersion int64) (int64, error) {
	return 0, errNotImplemented
}

func (ft FakeTopo) ValidateShard(keyspace, shard string) error {
	return errNotImplemented
}

func (ft FakeTopo) GetShard(keyspace, shard string) (*topo.ShardInfo, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetShardNames(keyspace string) ([]string, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) DeleteShard(keyspace, shard string) error {
	return errNotImplemented
}

func (ft FakeTopo) CreateTablet(tablet *topo.Tablet) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateTablet(tablet *topo.TabletInfo, existingVersion int64) (newVersion int64, err error) {
	return 0, errNotImplemented
}

func (ft FakeTopo) UpdateTabletFields(tabletAlias topo.TabletAlias, update func(*topo.Tablet) error) error {
	return errNotImplemented
}

func (ft FakeTopo) DeleteTablet(alias topo.TabletAlias) error {
	return errNotImplemented
}

func (ft FakeTopo) GetTablet(alias topo.TabletAlias) (*topo.TabletInfo, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) GetTabletsByCell(cell string) ([]topo.TabletAlias, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) UpdateShardReplicationFields(cell, keyspace, shard string, update func(*topo.ShardReplication) error) error {
	return errNotImplemented
}

func (ft FakeTopo) GetShardReplication(cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) DeleteShardReplication(cell, keyspace, shard string) error {
	return errNotImplemented
}

func (ft FakeTopo) LockSrvShardForAction(ctx context.Context, cell, keyspace, shard, contents string) (string, error) {
	return "", errNotImplemented
}

func (ft FakeTopo) UnlockSrvShardForAction(cell, keyspace, shard, lockPath, results string) error {
	return errNotImplemented
}

func (ft FakeTopo) GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]topo.TabletType, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) UpdateEndPoints(cell, keyspace, shard string, tabletType topo.TabletType, addrs *topo.EndPoints) error {
	return errNotImplemented
}

func (ft FakeTopo) DeleteEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) error {
	return errNotImplemented
}

func (ft FakeTopo) WatchEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (<-chan *topo.EndPoints, chan<- struct{}, error) {
	return nil, nil, errNotImplemented
}

func (ft FakeTopo) UpdateSrvShard(cell, keyspace, shard string, srvShard *topo.SrvShard) error {
	return errNotImplemented
}

func (ft FakeTopo) GetSrvShard(cell, keyspace, shard string) (*topo.SrvShard, error) {
	return nil, errNotImplemented
}

func (ft FakeTopo) DeleteSrvShard(cell, keyspace, shard string) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateSrvKeyspace(cell, keyspace string, srvKeyspace *topo.SrvKeyspace) error {
	return errNotImplemented
}

func (ft FakeTopo) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.EndPoint) error {
	return errNotImplemented
}

func (ft FakeTopo) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	return "", errNotImplemented
}

func (ft FakeTopo) UnlockKeyspaceForAction(keyspace, lockPath, results string) error {
	return errNotImplemented
}

func (ft FakeTopo) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	return "", errNotImplemented
}

func (ft FakeTopo) UnlockShardForAction(keyspace, shard, lockPath, results string) error {
	return errNotImplemented
}
