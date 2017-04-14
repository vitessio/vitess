// Package faketopo contains utitlities for tests that have to interact with a
// Vitess topology.
package faketopo

import (
	"errors"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

var errNotImplemented = errors.New("Not implemented")

// FakeTopo is a topo.Server implementation that always returns errNotImplemented errors.
type FakeTopo struct{}

// Close is part of the topo.Server interface.
func (ft FakeTopo) Close() {}

// ListDir is part of the topo.Backend interface.
func (ft FakeTopo) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	return nil, errNotImplemented
}

// Create is part of the topo.Backend interface.
func (ft FakeTopo) Create(ctx context.Context, cell, filePath string, contents []byte) (topo.Version, error) {
	return nil, errNotImplemented
}

// Update is part of the topo.Backend interface.
func (ft FakeTopo) Update(ctx context.Context, cell, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	return nil, errNotImplemented
}

// Get is part of the topo.Backend interface.
func (ft FakeTopo) Get(ctx context.Context, cell, filePath string) ([]byte, topo.Version, error) {
	return nil, nil, errNotImplemented
}

// Delete is part of the topo.Backend interface.
func (ft FakeTopo) Delete(ctx context.Context, cell, filePath string, version topo.Version) error {
	return errNotImplemented
}

// Watch is part of the topo.Backend interface.
func (ft FakeTopo) Watch(ctx context.Context, cell string, path string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	return &topo.WatchData{
		Err: errNotImplemented,
	}, nil, nil
}

// GetKnownCells is part of the topo.Server interface.
func (ft FakeTopo) GetKnownCells(ctx context.Context) ([]string, error) {
	return nil, errNotImplemented
}

// CreateKeyspace is part of the topo.Server interface.
func (ft FakeTopo) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	return errNotImplemented
}

// UpdateKeyspace is part of the topo.Server interface.
func (ft FakeTopo) UpdateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace, existingVersion int64) (int64, error) {
	return 0, errNotImplemented
}

// DeleteKeyspace is part of the topo.Server interface.
func (ft FakeTopo) DeleteKeyspace(ctx context.Context, keyspace string) error {
	return errNotImplemented
}

// GetKeyspace is part of the topo.Server interface.
func (ft FakeTopo) GetKeyspace(ctx context.Context, keyspace string) (*topodatapb.Keyspace, int64, error) {
	return nil, 0, errNotImplemented
}

// GetKeyspaces is part of the topo.Server interface.
func (ft FakeTopo) GetKeyspaces(ctx context.Context) ([]string, error) {
	return nil, errNotImplemented
}

// CreateShard is part of the topo.Server interface.
func (ft FakeTopo) CreateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard) error {
	return errNotImplemented
}

// UpdateShard is part of the topo.Server interface.
func (ft FakeTopo) UpdateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard, existingVersion int64) (int64, error) {
	return 0, errNotImplemented
}

// GetShard is part of the topo.Server interface.
func (ft FakeTopo) GetShard(ctx context.Context, keyspace, shard string) (*topodatapb.Shard, int64, error) {
	return nil, 0, errNotImplemented
}

// GetShardNames is part of the topo.Server interface.
func (ft FakeTopo) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	return nil, errNotImplemented
}

// DeleteShard is part of the topo.Server interface.
func (ft FakeTopo) DeleteShard(ctx context.Context, keyspace, shard string) error {
	return errNotImplemented
}

// CreateTablet is part of the topo.Server interface.
func (ft FakeTopo) CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error {
	return errNotImplemented
}

// UpdateTablet is part of the topo.Server interface.
func (ft FakeTopo) UpdateTablet(ctx context.Context, tablet *topodatapb.Tablet, existingVersion int64) (newVersion int64, err error) {
	return 0, errNotImplemented
}

// DeleteTablet is part of the topo.Server interface.
func (ft FakeTopo) DeleteTablet(ctx context.Context, alias *topodatapb.TabletAlias) error {
	return errNotImplemented
}

// GetTablet is part of the topo.Server interface.
func (ft FakeTopo) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error) {
	return nil, 0, errNotImplemented
}

// GetTabletsByCell is part of the topo.Server interface.
func (ft FakeTopo) GetTabletsByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error) {
	return nil, errNotImplemented
}

// UpdateShardReplicationFields is part of the topo.Server interface.
func (ft FakeTopo) UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*topodatapb.ShardReplication) error) error {
	return errNotImplemented
}

// GetShardReplication is part of the topo.Server interface.
func (ft FakeTopo) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	return nil, errNotImplemented
}

// DeleteShardReplication is part of the topo.Server interface.
func (ft FakeTopo) DeleteShardReplication(ctx context.Context, cell, keyspace, shard string) error {
	return errNotImplemented
}

// DeleteKeyspaceReplication is part of the topo.Server interface.
func (ft FakeTopo) DeleteKeyspaceReplication(ctx context.Context, cell, keyspace string) error {
	return errNotImplemented
}

// GetSrvKeyspaceNames is part of the topo.Server interface.
func (ft FakeTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	return nil, errNotImplemented
}

// UpdateSrvKeyspace is part of the topo.Server interface.
func (ft FakeTopo) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	return errNotImplemented
}

// DeleteSrvKeyspace is part of the topo.Server interface.
func (ft FakeTopo) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	return errNotImplemented
}

// GetSrvKeyspace is part of the topo.Server interface.
func (ft FakeTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return nil, errNotImplemented
}

// UpdateSrvVSchema is part of the topo.Server interface.
func (ft FakeTopo) UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error {
	return errNotImplemented
}

// GetSrvVSchema is part of the topo.Server interface.
func (ft FakeTopo) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	return nil, errNotImplemented
}

// LockKeyspaceForAction is part of the topo.Server interface.
func (ft FakeTopo) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	return "", errNotImplemented
}

// UnlockKeyspaceForAction is part of the topo.Server interface.
func (ft FakeTopo) UnlockKeyspaceForAction(ctx context.Context, keyspace, lockPath, results string) error {
	return errNotImplemented
}

// LockShardForAction is part of the topo.Server interface.
func (ft FakeTopo) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	return "", errNotImplemented
}

// UnlockShardForAction is part of the topo.Server interface.
func (ft FakeTopo) UnlockShardForAction(ctx context.Context, keyspace, shard, lockPath, results string) error {
	return errNotImplemented
}

// SaveVSchema is part of the topo.Server interface.
func (ft FakeTopo) SaveVSchema(context.Context, string, *vschemapb.Keyspace) error {
	return errNotImplemented
}

// GetVSchema is part of the topo.Server interface.
func (ft FakeTopo) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	return nil, errNotImplemented
}

// NewMasterParticipation is part of the topo.Server interface.
func (ft FakeTopo) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	return nil, errNotImplemented
}

var _ topo.Impl = (*FakeTopo)(nil) // compile-time interface check
