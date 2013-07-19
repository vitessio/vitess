// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"time"

	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/vt/topo"
)

// Tee is an implementation of topo.Server that uses a primary
// underlying topo.Server for all changes, but also duplicates the
// changes to a secondary topo.Server. It also locks both topo servers
// when needed.  It is meant to be used during transitions from one
// topo.Server to another.
type Tee struct {
	primary   topo.Server
	secondary topo.Server
}

func NewTee(primary, secondary topo.Server) *Tee {
	return &Tee{primary, secondary}
}

// topo.Server management interface.

func (tee *Tee) Close() {
	tee.primary.Close()
	tee.secondary.Close()
}

// Cell management, global

func (tee *Tee) GetKnownCells() ([]string, error) {
	return tee.primary.GetKnownCells()
}

// Keyspace management, global.

func (tee *Tee) CreateKeyspace(keyspace string) error {
	if err := tee.primary.CreateKeyspace(keyspace); err != nil {
		return err
	}

	// this is critical enough we want to fail
	if err := tee.secondary.CreateKeyspace(keyspace); err != nil {
		return err
	}
	return nil
}

func (tee *Tee) GetKeyspaces() ([]string, error) {
	return tee.primary.GetKeyspaces()
}

func (tee *Tee) DeleteKeyspaceShards(keyspace string) error {
	if err := tee.primary.DeleteKeyspaceShards(keyspace); err != nil {
		return err
	}

	if err := tee.secondary.DeleteKeyspaceShards(keyspace); err != nil {
		// not critical enough to fail
		relog.Warning("secondary.DeleteKeyspaceShards(%v) failed: %v", keyspace, err)
	}
	return nil
}

// Shard management, global.

func (tee *Tee) CreateShard(keyspace, shard string) error {
	err := tee.primary.CreateShard(keyspace, shard)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	serr := tee.secondary.CreateShard(keyspace, shard)
	if serr != nil && err != topo.ErrNodeExists {
		// not critical enough to fail
		relog.Warning("secondary.CreateShard(%v,%v) failed: %v", keyspace, shard, err)
	}
	return err
}

func (tee *Tee) UpdateShard(si *topo.ShardInfo) error {
	return tee.primary.UpdateShard(si)
}

func (tee *Tee) ValidateShard(keyspace, shard string) error {
	return tee.primary.ValidateShard(keyspace, shard)
}

func (tee *Tee) GetShard(keyspace, shard string) (si *topo.ShardInfo, err error) {
	return tee.primary.GetShard(keyspace, shard)
}

func (tee *Tee) GetShardNames(keyspace string) ([]string, error) {
	return tee.primary.GetShardNames(keyspace )
}

// Tablet management, per cell.

func (tee *Tee) CreateTablet(tablet *topo.Tablet) error {
	return tee.primary.CreateTablet(tablet)
}

func (tee *Tee) UpdateTablet(tablet *topo.TabletInfo, existingVersion int) (newVersion int, err error) {
	return tee.primary.UpdateTablet(tablet, existingVersion)
}

func (tee *Tee) UpdateTabletFields(tabletAlias topo.TabletAlias, update func(*topo.Tablet) error) error {
	return tee.primary.UpdateTabletFields(tabletAlias, update)
}

func (tee *Tee) DeleteTablet(alias topo.TabletAlias) error {
	return tee.primary.DeleteTablet(alias)
}

func (tee *Tee) ValidateTablet(alias topo.TabletAlias) error {
	return tee.primary.ValidateTablet(alias)
}

func (tee *Tee) GetTablet(alias topo.TabletAlias) (*topo.TabletInfo, error) {
	return tee.primary.GetTablet(alias)
}

func (tee *Tee) GetTabletsByCell(cell string) ([]topo.TabletAlias, error) {
	return tee.primary.GetTabletsByCell(cell)
}

// Replication graph management, global.

func (tee *Tee) GetReplicationPaths(keyspace, shard, repPath string) ([]topo.TabletAlias, error) {
	return tee.primary.GetReplicationPaths(keyspace, shard, repPath)
}

func (tee *Tee) CreateReplicationPath(keyspace, shard, repPath string) error {
	return tee.primary.CreateReplicationPath(keyspace, shard, repPath)
}

func (tee *Tee) DeleteReplicationPath(keyspace, shard, repPath string) error {
	return tee.primary.DeleteReplicationPath(keyspace, shard, repPath)
}

// Serving Graph management, per cell.

func (tee *Tee) GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]topo.TabletType, error) {
	return tee.primary.GetSrvTabletTypesPerShard(cell, keyspace, shard)
}

func (tee *Tee) UpdateSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType, addrs *topo.VtnsAddrs) error {
	return tee.primary.UpdateSrvTabletType(cell, keyspace, shard, tabletType, addrs)
}

func (tee *Tee) GetSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.VtnsAddrs, error) {
	return tee.primary.GetSrvTabletType(cell, keyspace, shard, tabletType)
}

func (tee *Tee) DeleteSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType) error {
	return tee.primary.DeleteSrvTabletType(cell, keyspace, shard, tabletType)
}

func (tee *Tee) UpdateSrvShard(cell, keyspace, shard string, srvShard *topo.SrvShard) error {
	return tee.primary.UpdateSrvShard(cell, keyspace, shard, srvShard)
}

func (tee *Tee) GetSrvShard(cell, keyspace, shard string) (*topo.SrvShard, error) {
	return tee.primary.GetSrvShard(cell, keyspace, shard)
}

func (tee *Tee) UpdateSrvKeyspace(cell, keyspace string, srvKeyspace *topo.SrvKeyspace) error {
	return tee.primary.UpdateSrvKeyspace(cell, keyspace, srvKeyspace)
}

func (tee *Tee) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	return tee.primary.GetSrvKeyspace(cell, keyspace)
}

func (tee *Tee) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.VtnsAddr) error {
	return tee.primary.UpdateTabletEndpoint(cell, keyspace, shard, tabletType, addr)
}

// Keyspace and Shard locks for actions, global.

func (tee *Tee) LockKeyspaceForAction(keyspace, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	return tee.primary.LockKeyspaceForAction(keyspace, contents, timeout, interrupted)
}

func (tee *Tee) UnlockKeyspaceForAction(keyspace, lockPath, results string) error {
	return tee.primary.UnlockKeyspaceForAction(keyspace, lockPath, results)
}

func (tee *Tee) LockShardForAction(keyspace, shard, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	return tee.primary.LockShardForAction(keyspace, shard, contents, timeout, interrupted)
}

func (tee *Tee) UnlockShardForAction(keyspace, shard, lockPath, results string) error {
	return tee.primary.UnlockShardForAction(keyspace, shard, lockPath, results)
}

// Remote Tablet Actions, local cell.

func (tee *Tee) WriteTabletAction(tabletAlias topo.TabletAlias, contents string) (string, error) {
	return tee.primary.WriteTabletAction(tabletAlias, contents)
}

func (tee *Tee) WaitForTabletAction(actionPath string, waitTime time.Duration, interrupted chan struct{}) (string, error) {
	return tee.primary.WaitForTabletAction(actionPath, waitTime, interrupted)
}

func (tee *Tee) PurgeTabletActions(tabletAlias topo.TabletAlias, canBePurged func(data string) bool) error {
	return tee.primary.PurgeTabletActions(tabletAlias, canBePurged)
}

// Supporting the local agent process, local cell.

func (tee *Tee) ValidateTabletActions(tabletAlias topo.TabletAlias) error {
	return tee.primary.ValidateTabletActions(tabletAlias)
}

func (tee *Tee) CreateTabletPidNode(tabletAlias topo.TabletAlias, done chan struct{}) error {
	return tee.primary.CreateTabletPidNode(tabletAlias, done)
}

func (tee *Tee) ValidateTabletPidNode(tabletAlias topo.TabletAlias) error {
	return tee.primary.ValidateTabletPidNode(tabletAlias)
}

func (tee *Tee) GetSubprocessFlags() []string {
	return tee.primary.GetSubprocessFlags()
}

func (tee *Tee) ActionEventLoop(tabletAlias topo.TabletAlias, dispatchAction func(actionPath, data string) error, done chan struct{}) {
	tee.primary.ActionEventLoop(tabletAlias, dispatchAction, done)
}

func (tee *Tee) ReadTabletActionPath(actionPath string) (topo.TabletAlias, string, int, error) {
	return tee.primary.ReadTabletActionPath(actionPath)
}

func (tee *Tee) UpdateTabletAction(actionPath, data string, version int) error {
	return tee.primary.UpdateTabletAction(actionPath, data, version)
}

func (tee *Tee) StoreTabletActionResponse(actionPath, data string) error {
	return tee.primary.StoreTabletActionResponse(actionPath, data)
}

func (tee *Tee) UnblockTabletAction(actionPath string) error {
	return tee.primary.UnblockTabletAction(actionPath)
}
