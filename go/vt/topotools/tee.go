// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"fmt"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

// Tee is an implementation of topo.Server that uses a primary
// underlying topo.Server for all changes, but also duplicates the
// changes to a secondary topo.Server. It also locks both topo servers
// when needed.  It is meant to be used during transitions from one
// topo.Server to another.
//
// - primary: we read everything from it, and write to it
// - secondary: we write to it as well, but we usually don't fail.
// - we lock primary/secondary if reverseLockOrder is False,
// or secondary/primary if reverseLockOrder is True.
type Tee struct {
	primary   topo.Server
	secondary topo.Server

	readFrom   topo.Server
	lockFirst  topo.Server
	lockSecond topo.Server

	keyspaceLockPaths map[string]string
	shardLockPaths    map[string]string
}

func NewTee(primary, secondary topo.Server, reverseLockOrder bool) *Tee {
	lockFirst := primary
	lockSecond := secondary
	if reverseLockOrder {
		lockFirst = secondary
		lockSecond = primary
	}
	return &Tee{
		primary:           primary,
		secondary:         secondary,
		readFrom:          primary,
		lockFirst:         lockFirst,
		lockSecond:        lockSecond,
		keyspaceLockPaths: make(map[string]string),
		shardLockPaths:    make(map[string]string),
	}
}

//
// topo.Server management interface.
//

func (tee *Tee) Close() {
	tee.primary.Close()
	tee.secondary.Close()
}

//
// Cell management, global
//

func (tee *Tee) GetKnownCells() ([]string, error) {
	return tee.readFrom.GetKnownCells()
}

//
// Keyspace management, global.
//

func (tee *Tee) CreateKeyspace(keyspace string) error {
	if err := tee.primary.CreateKeyspace(keyspace); err != nil {
		return err
	}

	// this is critical enough that we want to fail
	if err := tee.secondary.CreateKeyspace(keyspace); err != nil {
		return err
	}
	return nil
}

func (tee *Tee) GetKeyspaces() ([]string, error) {
	return tee.readFrom.GetKeyspaces()
}

func (tee *Tee) DeleteKeyspaceShards(keyspace string) error {
	if err := tee.primary.DeleteKeyspaceShards(keyspace); err != nil {
		return err
	}

	if err := tee.secondary.DeleteKeyspaceShards(keyspace); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteKeyspaceShards(%v) failed: %v", keyspace, err)
	}
	return nil
}

//
// Shard management, global.
//

func (tee *Tee) CreateShard(keyspace, shard string, value *topo.Shard) error {
	err := tee.primary.CreateShard(keyspace, shard, value)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	serr := tee.secondary.CreateShard(keyspace, shard, value)
	if serr != nil && serr != topo.ErrNodeExists {
		// not critical enough to fail
		log.Warningf("secondary.CreateShard(%v,%v) failed: %v", keyspace, shard, err)
	}
	return err
}

func (tee *Tee) UpdateShard(si *topo.ShardInfo) error {
	if err := tee.primary.UpdateShard(si); err != nil {
		// failed on primary, not updating secondary
		return err
	}

	if err := tee.secondary.UpdateShard(si); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateShard(%v,%v) failed: %v", si.Keyspace(), si.ShardName(), err)
	}
	return nil
}

func (tee *Tee) ValidateShard(keyspace, shard string) error {
	err := tee.primary.ValidateShard(keyspace, shard)
	if err != nil {
		return err
	}

	if err := tee.secondary.ValidateShard(keyspace, shard); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.ValidateShard(%v,%v) failed: %v", keyspace, shard, err)
	}
	return nil
}

func (tee *Tee) GetShard(keyspace, shard string) (si *topo.ShardInfo, err error) {
	return tee.readFrom.GetShard(keyspace, shard)
}

func (tee *Tee) GetShardNames(keyspace string) ([]string, error) {
	return tee.readFrom.GetShardNames(keyspace)
}

//
// Tablet management, per cell.
//

func (tee *Tee) CreateTablet(tablet *topo.Tablet) error {
	err := tee.primary.CreateTablet(tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	if err := tee.primary.CreateTablet(tablet); err != nil && err != topo.ErrNodeExists {
		// not critical enough to fail
		log.Warningf("secondary.CreateTablet(%v) failed: %v", tablet.Alias(), err)
	}
	return err
}

func (tee *Tee) UpdateTablet(tablet *topo.TabletInfo, existingVersion int64) (newVersion int64, err error) {
	if newVersion, err = tee.primary.UpdateTablet(tablet, existingVersion); err != nil {
		// failed on primary, not updating secondary
		return
	}

	if _, err := tee.secondary.UpdateTablet(tablet, existingVersion); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateTablet(%v) failed: %v", tablet.Alias(), err)
	}
	return
}

func (tee *Tee) UpdateTabletFields(tabletAlias topo.TabletAlias, update func(*topo.Tablet) error) error {
	if err := tee.primary.UpdateTabletFields(tabletAlias, update); err != nil {
		// failed on primary, not updating secondary
		return err
	}

	if err := tee.secondary.UpdateTabletFields(tabletAlias, update); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateTabletFields(%v) failed: %v", tabletAlias, err)
	}
	return nil
}

func (tee *Tee) DeleteTablet(alias topo.TabletAlias) error {
	if err := tee.primary.DeleteTablet(alias); err != nil {
		return err
	}

	if err := tee.secondary.DeleteTablet(alias); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteTablet(%v) failed: %v", alias, err)
	}
	return nil
}

func (tee *Tee) ValidateTablet(alias topo.TabletAlias) error {
	if err := tee.primary.ValidateTablet(alias); err != nil {
		return err
	}

	if err := tee.secondary.ValidateTablet(alias); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.ValidateTablet(%v) failed: %v", alias, err)
	}
	return nil
}

func (tee *Tee) GetTablet(alias topo.TabletAlias) (*topo.TabletInfo, error) {
	return tee.readFrom.GetTablet(alias)
}

func (tee *Tee) GetTabletsByCell(cell string) ([]topo.TabletAlias, error) {
	return tee.readFrom.GetTabletsByCell(cell)
}

//
// Replication graph management, global.
//

func (tee *Tee) GetReplicationPaths(keyspace, shard, repPath string) ([]topo.TabletAlias, error) {
	return tee.readFrom.GetReplicationPaths(keyspace, shard, repPath)
}

func (tee *Tee) CreateReplicationPath(keyspace, shard, repPath string) error {
	if err := tee.primary.CreateReplicationPath(keyspace, shard, repPath); err != nil {
		return err
	}

	if err := tee.secondary.CreateReplicationPath(keyspace, shard, repPath); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.CreateReplicationPath(%v, %v, %v) failed: %v", keyspace, shard, repPath, err)
	}
	return nil
}

func (tee *Tee) DeleteReplicationPath(keyspace, shard, repPath string) error {
	if err := tee.primary.DeleteReplicationPath(keyspace, shard, repPath); err != nil {
		return err
	}

	if err := tee.secondary.DeleteReplicationPath(keyspace, shard, repPath); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteReplicationPath(%v, %v, %v) failed: %v", keyspace, shard, repPath, err)
	}
	return nil
}

//
// Serving Graph management, per cell.
//

func (tee *Tee) GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]topo.TabletType, error) {
	return tee.readFrom.GetSrvTabletTypesPerShard(cell, keyspace, shard)
}

func (tee *Tee) UpdateSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType, addrs *topo.VtnsAddrs) error {
	if err := tee.primary.UpdateSrvTabletType(cell, keyspace, shard, tabletType, addrs); err != nil {
		return err
	}

	if err := tee.secondary.UpdateSrvTabletType(cell, keyspace, shard, tabletType, addrs); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateSrvTabletType(%v, %v, %v, %v) failed: %v", cell, keyspace, shard, tabletType, err)
	}
	return nil
}

func (tee *Tee) GetSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.VtnsAddrs, error) {
	return tee.readFrom.GetSrvTabletType(cell, keyspace, shard, tabletType)
}

func (tee *Tee) DeleteSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType) error {
	if err := tee.primary.DeleteSrvTabletType(cell, keyspace, shard, tabletType); err != nil {
		return err
	}

	if err := tee.secondary.DeleteSrvTabletType(cell, keyspace, shard, tabletType); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteSrvTabletType(%v, %v, %v, %v) failed: %v", cell, keyspace, shard, tabletType, err)
	}
	return nil
}

func (tee *Tee) UpdateSrvShard(cell, keyspace, shard string, srvShard *topo.SrvShard) error {
	if err := tee.primary.UpdateSrvShard(cell, keyspace, shard, srvShard); err != nil {
		return err
	}

	if err := tee.secondary.UpdateSrvShard(cell, keyspace, shard, srvShard); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateSrvShard(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
	}
	return nil
}

func (tee *Tee) GetSrvShard(cell, keyspace, shard string) (*topo.SrvShard, error) {
	return tee.readFrom.GetSrvShard(cell, keyspace, shard)
}

func (tee *Tee) UpdateSrvKeyspace(cell, keyspace string, srvKeyspace *topo.SrvKeyspace) error {
	if err := tee.primary.UpdateSrvKeyspace(cell, keyspace, srvKeyspace); err != nil {
		return err
	}

	if err := tee.secondary.UpdateSrvKeyspace(cell, keyspace, srvKeyspace); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateSrvKeyspace(%v, %v) failed: %v", cell, keyspace, err)
	}
	return nil
}

func (tee *Tee) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	return tee.readFrom.GetSrvKeyspace(cell, keyspace)
}

func (tee *Tee) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.VtnsAddr) error {
	if err := tee.primary.UpdateTabletEndpoint(cell, keyspace, shard, tabletType, addr); err != nil {
		return err
	}

	if err := tee.secondary.UpdateTabletEndpoint(cell, keyspace, shard, tabletType, addr); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateTabletEndpoint(%v, %v, %v, %v) failed: %v", cell, keyspace, shard, tabletType, err)
	}
	return nil
}

//
// Keyspace and Shard locks for actions, global.
//

func (tee *Tee) LockKeyspaceForAction(keyspace, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	// lock lockFirst
	pLockPath, err := tee.lockFirst.LockKeyspaceForAction(keyspace, contents, timeout, interrupted)
	if err != nil {
		return "", err
	}

	// lock lockSecond
	sLockPath, err := tee.lockSecond.LockKeyspaceForAction(keyspace, contents, timeout, interrupted)
	if err != nil {
		if err := tee.lockFirst.UnlockKeyspaceForAction(keyspace, pLockPath, "{}"); err != nil {
			log.Warningf("Failed to unlock lockFirst keyspace after failed lockSecond lock for %v", keyspace)
		}
		return "", err
	}

	// remember both locks, keyed by lockFirst lock path
	tee.keyspaceLockPaths[pLockPath] = sLockPath
	return pLockPath, nil
}

func (tee *Tee) UnlockKeyspaceForAction(keyspace, lockPath, results string) error {
	// get from map
	sLockPath, ok := tee.keyspaceLockPaths[lockPath]
	if !ok {
		return fmt.Errorf("no lockPath %v in keyspaceLockPaths", lockPath)
	}
	delete(tee.keyspaceLockPaths, lockPath)

	// unlock lockSecond, then lockFirst
	serr := tee.lockSecond.UnlockKeyspaceForAction(keyspace, sLockPath, results)
	perr := tee.lockFirst.UnlockKeyspaceForAction(keyspace, lockPath, results)

	if serr != nil {
		if perr != nil {
			log.Warningf("Secondary UnlockKeyspaceForAction(%v, %v) failed: %v", keyspace, sLockPath, serr)
		}
		return serr
	}
	return perr
}

func (tee *Tee) LockShardForAction(keyspace, shard, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	// lock lockFirst
	pLockPath, err := tee.lockFirst.LockShardForAction(keyspace, shard, contents, timeout, interrupted)
	if err != nil {
		return "", err
	}

	// lock lockSecond
	sLockPath, err := tee.lockSecond.LockShardForAction(keyspace, shard, contents, timeout, interrupted)
	if err != nil {
		if err := tee.lockFirst.UnlockShardForAction(keyspace, shard, pLockPath, "{}"); err != nil {
			log.Warningf("Failed to unlock lockFirst shard after failed lockSecond lock for %v/%v", keyspace, shard)
		}
		return "", err
	}

	// remember both locks, keyed by lockFirst lock path
	tee.shardLockPaths[pLockPath] = sLockPath
	return pLockPath, nil
}

func (tee *Tee) UnlockShardForAction(keyspace, shard, lockPath, results string) error {
	// get from map
	sLockPath, ok := tee.shardLockPaths[lockPath]
	if !ok {
		return fmt.Errorf("no lockPath %v in shardLockPaths", lockPath)
	}
	delete(tee.shardLockPaths, lockPath)

	// unlock lockSecond, then lockFirst
	serr := tee.lockSecond.UnlockShardForAction(keyspace, shard, sLockPath, results)
	perr := tee.lockFirst.UnlockShardForAction(keyspace, shard, lockPath, results)

	if serr != nil {
		if perr != nil {
			log.Warningf("Secondary UnlockShardForAction(%v/%v, %v) failed: %v", keyspace, shard, sLockPath, serr)
		}
		return serr
	}
	return perr
}

//
// Remote Tablet Actions, local cell.
// TODO(alainjobart) implement the split
//

func (tee *Tee) WriteTabletAction(tabletAlias topo.TabletAlias, contents string) (string, error) {
	return tee.primary.WriteTabletAction(tabletAlias, contents)
}

func (tee *Tee) WaitForTabletAction(actionPath string, waitTime time.Duration, interrupted chan struct{}) (string, error) {
	return tee.primary.WaitForTabletAction(actionPath, waitTime, interrupted)
}

func (tee *Tee) PurgeTabletActions(tabletAlias topo.TabletAlias, canBePurged func(data string) bool) error {
	return tee.primary.PurgeTabletActions(tabletAlias, canBePurged)
}

//
// Supporting the local agent process, local cell.
// TODO(alainjobart) implement the split
//

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
	p := tee.primary.GetSubprocessFlags()
	p = append(p, tee.secondary.GetSubprocessFlags()...)

	// propagate the VT_TOPOLOGY_SERVER environment too
	name := os.Getenv("VT_TOPOLOGY_SERVER")
	if name != "" {
		p = append(p, "VT_TOPOLOGY_SERVER="+name)
	}

	return p
}

func (tee *Tee) ActionEventLoop(tabletAlias topo.TabletAlias, dispatchAction func(actionPath, data string) error, done chan struct{}) {
	tee.primary.ActionEventLoop(tabletAlias, dispatchAction, done)
}

func (tee *Tee) ReadTabletActionPath(actionPath string) (topo.TabletAlias, string, int64, error) {
	return tee.primary.ReadTabletActionPath(actionPath)
}

func (tee *Tee) UpdateTabletAction(actionPath, data string, version int64) error {
	return tee.primary.UpdateTabletAction(actionPath, data, version)
}

func (tee *Tee) StoreTabletActionResponse(actionPath, data string) error {
	return tee.primary.StoreTabletActionResponse(actionPath, data)
}

func (tee *Tee) UnblockTabletAction(actionPath string) error {
	return tee.primary.UnblockTabletAction(actionPath)
}
