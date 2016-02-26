// Package faketopo contains utitlities for tests that have to interact with a
// Vitess topology.
package faketopo

import (
	"errors"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var errNotImplemented = errors.New("Not implemented")

// FakeTopo is a topo.Server implementation that always returns errNotImplemented errors.
type FakeTopo struct{}

// GetSrvKeyspaceNames implements topo.Server.
func (ft FakeTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	return nil, errNotImplemented
}

// GetSrvKeyspace implements topo.Server.
func (ft FakeTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return nil, errNotImplemented
}

// GetEndPoints implements topo.Server.
func (ft FakeTopo) GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType) (*topodatapb.EndPoints, int64, error) {
	return nil, -1, errNotImplemented
}

// Close implements topo.Server.
func (ft FakeTopo) Close() {}

// GetKnownCells implements topo.Server.
func (ft FakeTopo) GetKnownCells(ctx context.Context) ([]string, error) {
	return nil, errNotImplemented
}

// CreateKeyspace implements topo.Server.
func (ft FakeTopo) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	return errNotImplemented
}

// UpdateKeyspace implements topo.Server.
func (ft FakeTopo) UpdateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace, existingVersion int64) (int64, error) {
	return 0, errNotImplemented
}

// DeleteKeyspace implements topo.Server.
func (ft FakeTopo) DeleteKeyspace(ctx context.Context, keyspace string) error {
	return errNotImplemented
}

// GetKeyspace implements topo.Server.
func (ft FakeTopo) GetKeyspace(ctx context.Context, keyspace string) (*topodatapb.Keyspace, int64, error) {
	return nil, 0, errNotImplemented
}

// GetKeyspaces implements topo.Server.
func (ft FakeTopo) GetKeyspaces(ctx context.Context) ([]string, error) {
	return nil, errNotImplemented
}

// DeleteKeyspaceShards implements topo.Server.
func (ft FakeTopo) DeleteKeyspaceShards(ctx context.Context, keyspace string) error {
	return errNotImplemented
}

// CreateShard implements topo.Server.
func (ft FakeTopo) CreateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard) error {
	return errNotImplemented
}

// UpdateShard implements topo.Server.
func (ft FakeTopo) UpdateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard, existingVersion int64) (int64, error) {
	return 0, errNotImplemented
}

// ValidateShard implements topo.Server.
func (ft FakeTopo) ValidateShard(ctx context.Context, keyspace, shard string) error {
	return errNotImplemented
}

// GetShard implements topo.Server.
func (ft FakeTopo) GetShard(ctx context.Context, keyspace, shard string) (*topodatapb.Shard, int64, error) {
	return nil, 0, errNotImplemented
}

// GetShardNames implements topo.Server.
func (ft FakeTopo) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	return nil, errNotImplemented
}

// DeleteShard implements topo.Server.
func (ft FakeTopo) DeleteShard(ctx context.Context, keyspace, shard string) error {
	return errNotImplemented
}

// CreateTablet implements topo.Server.
func (ft FakeTopo) CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error {
	return errNotImplemented
}

// UpdateTablet implements topo.Server.
func (ft FakeTopo) UpdateTablet(ctx context.Context, tablet *topodatapb.Tablet, existingVersion int64) (newVersion int64, err error) {
	return 0, errNotImplemented
}

// DeleteTablet implements topo.Server.
func (ft FakeTopo) DeleteTablet(ctx context.Context, alias *topodatapb.TabletAlias) error {
	return errNotImplemented
}

// GetTablet implements topo.Server.
func (ft FakeTopo) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error) {
	return nil, 0, errNotImplemented
}

// GetTabletsByCell implements topo.Server.
func (ft FakeTopo) GetTabletsByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error) {
	return nil, errNotImplemented
}

// UpdateShardReplicationFields implements topo.Server.
func (ft FakeTopo) UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*topodatapb.ShardReplication) error) error {
	return errNotImplemented
}

// GetShardReplication implements topo.Server.
func (ft FakeTopo) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	return nil, errNotImplemented
}

// DeleteShardReplication implements topo.Server.
func (ft FakeTopo) DeleteShardReplication(ctx context.Context, cell, keyspace, shard string) error {
	return errNotImplemented
}

// DeleteKeyspaceReplication implements topo.Server.
func (ft FakeTopo) DeleteKeyspaceReplication(ctx context.Context, cell, keyspace string) error {
	return errNotImplemented
}

// LockSrvShardForAction implements topo.Server.
func (ft FakeTopo) LockSrvShardForAction(ctx context.Context, cell, keyspace, shard, contents string) (string, error) {
	return "", errNotImplemented
}

// UnlockSrvShardForAction implements topo.Server.
func (ft FakeTopo) UnlockSrvShardForAction(ctx context.Context, cell, keyspace, shard, lockPath, results string) error {
	return errNotImplemented
}

// GetSrvTabletTypesPerShard implements topo.Server.
func (ft FakeTopo) GetSrvTabletTypesPerShard(ctx context.Context, cell, keyspace, shard string) ([]topodatapb.TabletType, error) {
	return nil, errNotImplemented
}

// CreateEndPoints implements topo.Server.
func (ft FakeTopo) CreateEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType, addrs *topodatapb.EndPoints) error {
	return errNotImplemented
}

// UpdateEndPoints implements topo.Server.
func (ft FakeTopo) UpdateEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType, addrs *topodatapb.EndPoints, existingVersion int64) error {
	return errNotImplemented
}

// DeleteEndPoints implements topo.Server.
func (ft FakeTopo) DeleteEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType, existingVersion int64) error {
	return errNotImplemented
}

// WatchSrvKeyspace implements topo.Server.WatchSrvKeyspace
func (ft FakeTopo) WatchSrvKeyspace(ctx context.Context, cell, keyspace string) (<-chan *topodatapb.SrvKeyspace, error) {
	return nil, errNotImplemented
}

// UpdateSrvShard implements topo.Server.
func (ft FakeTopo) UpdateSrvShard(ctx context.Context, cell, keyspace, shard string, srvShard *topodatapb.SrvShard) error {
	return errNotImplemented
}

// GetSrvShard implements topo.Server.
func (ft FakeTopo) GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*topodatapb.SrvShard, error) {
	return nil, errNotImplemented
}

// DeleteSrvShard implements topo.Server.
func (ft FakeTopo) DeleteSrvShard(ctx context.Context, cell, keyspace, shard string) error {
	return errNotImplemented
}

// UpdateSrvKeyspace implements topo.Server.
func (ft FakeTopo) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	return errNotImplemented
}

// DeleteSrvKeyspace implements topo.Server.
func (ft FakeTopo) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	return errNotImplemented
}

// LockKeyspaceForAction implements topo.Server.
func (ft FakeTopo) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	return "", errNotImplemented
}

// UnlockKeyspaceForAction implements topo.Server.
func (ft FakeTopo) UnlockKeyspaceForAction(ctx context.Context, keyspace, lockPath, results string) error {
	return errNotImplemented
}

// LockShardForAction implements topo.Server.
func (ft FakeTopo) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	return "", errNotImplemented
}

// UnlockShardForAction implements topo.Server.
func (ft FakeTopo) UnlockShardForAction(ctx context.Context, keyspace, shard, lockPath, results string) error {
	return errNotImplemented
}

// SaveVSchema implements topo.Server.
func (ft FakeTopo) SaveVSchema(context.Context, string) error {
	return errNotImplemented
}

// GetVSchema implements topo.Server.
func (ft FakeTopo) GetVSchema(ctx context.Context) (string, error) {
	return "", errNotImplemented
}
