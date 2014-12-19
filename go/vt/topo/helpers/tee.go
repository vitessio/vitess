// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package helpers

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
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

	readFrom       topo.Server
	readFromSecond topo.Server

	lockFirst  topo.Server
	lockSecond topo.Server

	// protects the variables below this point
	mu sync.Mutex

	keyspaceVersionMapping map[string]versionMapping
	shardVersionMapping    map[string]versionMapping
	tabletVersionMapping   map[topo.TabletAlias]versionMapping

	keyspaceLockPaths map[string]string
	shardLockPaths    map[string]string
	srvShardLockPaths map[string]string
}

// when reading a version from 'readFrom', we also read another version
// from 'readFromSecond', and save the mapping to this map. We only keep one
// mapping for a given object, no need to overdo it
type versionMapping struct {
	readFromVersion       int64
	readFromSecondVersion int64
}

func NewTee(primary, secondary topo.Server, reverseLockOrder bool) *Tee {
	lockFirst := primary
	lockSecond := secondary
	if reverseLockOrder {
		lockFirst = secondary
		lockSecond = primary
	}
	return &Tee{
		primary:                primary,
		secondary:              secondary,
		readFrom:               primary,
		readFromSecond:         secondary,
		lockFirst:              lockFirst,
		lockSecond:             lockSecond,
		keyspaceVersionMapping: make(map[string]versionMapping),
		shardVersionMapping:    make(map[string]versionMapping),
		tabletVersionMapping:   make(map[topo.TabletAlias]versionMapping),
		keyspaceLockPaths:      make(map[string]string),
		shardLockPaths:         make(map[string]string),
		srvShardLockPaths:      make(map[string]string),
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

func (tee *Tee) CreateKeyspace(keyspace string, value *topo.Keyspace) error {
	if err := tee.primary.CreateKeyspace(keyspace, value); err != nil {
		return err
	}

	// this is critical enough that we want to fail
	if err := tee.secondary.CreateKeyspace(keyspace, value); err != nil {
		return err
	}
	return nil
}

func (tee *Tee) UpdateKeyspace(ki *topo.KeyspaceInfo, existingVersion int64) (newVersion int64, err error) {
	if newVersion, err = tee.primary.UpdateKeyspace(ki, existingVersion); err != nil {
		// failed on primary, not updating secondary
		return
	}

	// if we have a mapping between keyspace version in first topo
	// and keyspace version in second topo, replace the version number.
	// if not, this will probably fail and log.
	tee.mu.Lock()
	kvm, ok := tee.keyspaceVersionMapping[ki.KeyspaceName()]
	if ok && kvm.readFromVersion == existingVersion {
		existingVersion = kvm.readFromSecondVersion
		delete(tee.keyspaceVersionMapping, ki.KeyspaceName())
	}
	tee.mu.Unlock()
	if newVersion2, serr := tee.secondary.UpdateKeyspace(ki, existingVersion); serr != nil {
		// not critical enough to fail
		if serr == topo.ErrNoNode {
			// the keyspace doesn't exist on the secondary, let's
			// just create it
			if serr = tee.secondary.CreateKeyspace(ki.KeyspaceName(), ki.Keyspace); serr != nil {
				log.Warningf("secondary.CreateKeyspace(%v) failed (after UpdateKeyspace returned ErrNoNode): %v", ki.KeyspaceName(), serr)
			} else {
				log.Infof("secondary.UpdateKeyspace(%v) failed with ErrNoNode, CreateKeyspace then worked.", ki.KeyspaceName())
				ki, gerr := tee.secondary.GetKeyspace(ki.KeyspaceName())
				if gerr != nil {
					log.Warningf("Failed to re-read keyspace(%v) after creating it on secondary: %v", ki.KeyspaceName(), gerr)
				} else {
					tee.mu.Lock()
					tee.keyspaceVersionMapping[ki.KeyspaceName()] = versionMapping{
						readFromVersion:       newVersion,
						readFromSecondVersion: ki.Version(),
					}
					tee.mu.Unlock()
				}
			}
		} else {
			log.Warningf("secondary.UpdateKeyspace(%v) failed: %v", ki.KeyspaceName(), serr)
		}
	} else {
		tee.mu.Lock()
		tee.keyspaceVersionMapping[ki.KeyspaceName()] = versionMapping{
			readFromVersion:       newVersion,
			readFromSecondVersion: newVersion2,
		}
		tee.mu.Unlock()
	}
	return
}

func (tee *Tee) GetKeyspace(keyspace string) (*topo.KeyspaceInfo, error) {
	ki, err := tee.readFrom.GetKeyspace(keyspace)
	if err != nil {
		return nil, err
	}

	ki2, err := tee.readFromSecond.GetKeyspace(keyspace)
	if err != nil {
		// can't read from secondary, so we can's keep version map
		return ki, nil
	}

	tee.mu.Lock()
	tee.keyspaceVersionMapping[keyspace] = versionMapping{
		readFromVersion:       ki.Version(),
		readFromSecondVersion: ki2.Version(),
	}
	tee.mu.Unlock()
	return ki, nil
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

func (tee *Tee) UpdateShard(si *topo.ShardInfo, existingVersion int64) (newVersion int64, err error) {
	if newVersion, err = tee.primary.UpdateShard(si, existingVersion); err != nil {
		// failed on primary, not updating secondary
		return
	}

	// if we have a mapping between shard version in first topo
	// and shard version in second topo, replace the version number.
	// if not, this will probably fail and log.
	tee.mu.Lock()
	svm, ok := tee.shardVersionMapping[si.Keyspace()+"/"+si.ShardName()]
	if ok && svm.readFromVersion == existingVersion {
		existingVersion = svm.readFromSecondVersion
		delete(tee.shardVersionMapping, si.Keyspace()+"/"+si.ShardName())
	}
	tee.mu.Unlock()
	if newVersion2, serr := tee.secondary.UpdateShard(si, existingVersion); serr != nil {
		// not critical enough to fail
		if serr == topo.ErrNoNode {
			// the shard doesn't exist on the secondary, let's
			// just create it
			if serr = tee.secondary.CreateShard(si.Keyspace(), si.ShardName(), si.Shard); serr != nil {
				log.Warningf("secondary.CreateShard(%v,%v) failed (after UpdateShard returned ErrNoNode): %v", si.Keyspace(), si.ShardName(), serr)
			} else {
				log.Infof("secondary.UpdateShard(%v, %v) failed with ErrNoNode, CreateShard then worked.", si.Keyspace(), si.ShardName())
				si, gerr := tee.secondary.GetShard(si.Keyspace(), si.ShardName())
				if gerr != nil {
					log.Warningf("Failed to re-read shard(%v, %v) after creating it on secondary: %v", si.Keyspace(), si.ShardName(), gerr)
				} else {
					tee.mu.Lock()
					tee.shardVersionMapping[si.Keyspace()+"/"+si.ShardName()] = versionMapping{
						readFromVersion:       newVersion,
						readFromSecondVersion: si.Version(),
					}
					tee.mu.Unlock()
				}
			}
		} else {
			log.Warningf("secondary.UpdateShard(%v, %v) failed: %v", si.Keyspace(), si.ShardName(), serr)
		}
	} else {
		tee.mu.Lock()
		tee.shardVersionMapping[si.Keyspace()+"/"+si.ShardName()] = versionMapping{
			readFromVersion:       newVersion,
			readFromSecondVersion: newVersion2,
		}
		tee.mu.Unlock()
	}
	return
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

func (tee *Tee) GetShard(keyspace, shard string) (*topo.ShardInfo, error) {
	si, err := tee.readFrom.GetShard(keyspace, shard)
	if err != nil {
		return nil, err
	}

	si2, err := tee.readFromSecond.GetShard(keyspace, shard)
	if err != nil {
		// can't read from secondary, so we can's keep version map
		return si, nil
	}

	tee.mu.Lock()
	tee.shardVersionMapping[keyspace+"/"+shard] = versionMapping{
		readFromVersion:       si.Version(),
		readFromSecondVersion: si2.Version(),
	}
	tee.mu.Unlock()
	return si, nil
}

func (tee *Tee) GetShardNames(keyspace string) ([]string, error) {
	return tee.readFrom.GetShardNames(keyspace)
}

func (tee *Tee) DeleteShard(keyspace, shard string) error {
	err := tee.primary.DeleteShard(keyspace, shard)
	if err != nil && err != topo.ErrNoNode {
		return err
	}

	if err := tee.secondary.DeleteShard(keyspace, shard); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteShard(%v, %v) failed: %v", keyspace, shard, err)
	}
	return err
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
		log.Warningf("secondary.CreateTablet(%v) failed: %v", tablet.Alias, err)
	}
	return err
}

func (tee *Tee) UpdateTablet(tablet *topo.TabletInfo, existingVersion int64) (newVersion int64, err error) {
	if newVersion, err = tee.primary.UpdateTablet(tablet, existingVersion); err != nil {
		// failed on primary, not updating secondary
		return
	}

	// if we have a mapping between tablet version in first topo
	// and tablet version in second topo, replace the version number.
	// if not, this will probably fail and log.
	tee.mu.Lock()
	tvm, ok := tee.tabletVersionMapping[tablet.Alias]
	if ok && tvm.readFromVersion == existingVersion {
		existingVersion = tvm.readFromSecondVersion
		delete(tee.tabletVersionMapping, tablet.Alias)
	}
	tee.mu.Unlock()
	if newVersion2, serr := tee.secondary.UpdateTablet(tablet, existingVersion); serr != nil {
		// not critical enough to fail
		if serr == topo.ErrNoNode {
			// the tablet doesn't exist on the secondary, let's
			// just create it
			if serr = tee.secondary.CreateTablet(tablet.Tablet); serr != nil {
				log.Warningf("secondary.CreateTablet(%v) failed (after UpdateTablet returned ErrNoNode): %v", tablet.Alias, serr)
			} else {
				log.Infof("secondary.UpdateTablet(%v) failed with ErrNoNode, CreateTablet then worked.", tablet.Alias)
				ti, gerr := tee.secondary.GetTablet(tablet.Alias)
				if gerr != nil {
					log.Warningf("Failed to re-read tablet(%v) after creating it on secondary: %v", tablet.Alias, gerr)
				} else {
					tee.mu.Lock()
					tee.tabletVersionMapping[tablet.Alias] = versionMapping{
						readFromVersion:       newVersion,
						readFromSecondVersion: ti.Version(),
					}
					tee.mu.Unlock()
				}
			}
		} else {
			log.Warningf("secondary.UpdateTablet(%v) failed: %v", tablet.Alias, serr)
		}
	} else {
		tee.mu.Lock()
		tee.tabletVersionMapping[tablet.Alias] = versionMapping{
			readFromVersion:       newVersion,
			readFromSecondVersion: newVersion2,
		}
		tee.mu.Unlock()
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

func (tee *Tee) GetTablet(alias topo.TabletAlias) (*topo.TabletInfo, error) {
	ti, err := tee.readFrom.GetTablet(alias)
	if err != nil {
		return nil, err
	}

	ti2, err := tee.readFromSecond.GetTablet(alias)
	if err != nil {
		// can't read from secondary, so we can's keep version map
		return ti, nil
	}

	tee.mu.Lock()
	tee.tabletVersionMapping[alias] = versionMapping{
		readFromVersion:       ti.Version(),
		readFromSecondVersion: ti2.Version(),
	}
	tee.mu.Unlock()
	return ti, nil
}

func (tee *Tee) GetTabletsByCell(cell string) ([]topo.TabletAlias, error) {
	return tee.readFrom.GetTabletsByCell(cell)
}

//
// Shard replication graph management, local.
//

func (tee *Tee) UpdateShardReplicationFields(cell, keyspace, shard string, update func(*topo.ShardReplication) error) error {
	if err := tee.primary.UpdateShardReplicationFields(cell, keyspace, shard, update); err != nil {
		// failed on primary, not updating secondary
		return err
	}

	if err := tee.secondary.UpdateShardReplicationFields(cell, keyspace, shard, update); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateShardReplicationFields(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
	}
	return nil
}

func (tee *Tee) GetShardReplication(cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	return tee.readFrom.GetShardReplication(cell, keyspace, shard)
}

func (tee *Tee) DeleteShardReplication(cell, keyspace, shard string) error {
	if err := tee.primary.DeleteShardReplication(cell, keyspace, shard); err != nil {
		return err
	}

	if err := tee.secondary.DeleteShardReplication(cell, keyspace, shard); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteShardReplication(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
	}
	return nil
}

//
// Serving Graph management, per cell.
//

func (tee *Tee) LockSrvShardForAction(ctx context.Context, cell, keyspace, shard, contents string) (string, error) {
	// lock lockFirst
	pLockPath, err := tee.lockFirst.LockSrvShardForAction(ctx, cell, keyspace, shard, contents)
	if err != nil {
		return "", err
	}

	// lock lockSecond
	sLockPath, err := tee.lockSecond.LockSrvShardForAction(ctx, cell, keyspace, shard, contents)
	if err != nil {
		if err := tee.lockFirst.UnlockSrvShardForAction(cell, keyspace, shard, pLockPath, "{}"); err != nil {
			log.Warningf("Failed to unlock lockFirst shard after failed lockSecond lock for %v/%v/%v", cell, keyspace, shard)
		}
		return "", err
	}

	// remember both locks, keyed by lockFirst lock path
	tee.mu.Lock()
	tee.srvShardLockPaths[pLockPath] = sLockPath
	tee.mu.Unlock()
	return pLockPath, nil
}

func (tee *Tee) UnlockSrvShardForAction(cell, keyspace, shard, lockPath, results string) error {
	// get from map
	tee.mu.Lock() // not using defer for unlock, to minimize lock time
	sLockPath, ok := tee.srvShardLockPaths[lockPath]
	if !ok {
		tee.mu.Unlock()
		return fmt.Errorf("no lockPath %v in srvShardLockPaths", lockPath)
	}
	delete(tee.srvShardLockPaths, lockPath)
	tee.mu.Unlock()

	// unlock lockSecond, then lockFirst
	serr := tee.lockSecond.UnlockSrvShardForAction(cell, keyspace, shard, sLockPath, results)
	perr := tee.lockFirst.UnlockSrvShardForAction(cell, keyspace, shard, lockPath, results)

	if serr != nil {
		if perr != nil {
			log.Warningf("Secondary UnlockSrvShardForAction(%v/%v/%v, %v) failed: %v", cell, keyspace, shard, sLockPath, serr)
		}
		return serr
	}
	return perr
}

func (tee *Tee) GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]topo.TabletType, error) {
	return tee.readFrom.GetSrvTabletTypesPerShard(cell, keyspace, shard)
}

func (tee *Tee) UpdateEndPoints(cell, keyspace, shard string, tabletType topo.TabletType, addrs *topo.EndPoints) error {
	if err := tee.primary.UpdateEndPoints(cell, keyspace, shard, tabletType, addrs); err != nil {
		return err
	}

	if err := tee.secondary.UpdateEndPoints(cell, keyspace, shard, tabletType, addrs); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateEndPoints(%v, %v, %v, %v) failed: %v", cell, keyspace, shard, tabletType, err)
	}
	return nil
}

func (tee *Tee) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	return tee.readFrom.GetEndPoints(cell, keyspace, shard, tabletType)
}

func (tee *Tee) DeleteEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) error {
	err := tee.primary.DeleteEndPoints(cell, keyspace, shard, tabletType)
	if err != nil && err != topo.ErrNoNode {
		return err
	}

	if err := tee.secondary.DeleteEndPoints(cell, keyspace, shard, tabletType); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteEndPoints(%v, %v, %v, %v) failed: %v", cell, keyspace, shard, tabletType, err)
	}
	return err
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

func (tee *Tee) DeleteSrvShard(cell, keyspace, shard string) error {
	err := tee.primary.DeleteSrvShard(cell, keyspace, shard)
	if err != nil && err != topo.ErrNoNode {
		return err
	}

	if err := tee.secondary.DeleteSrvShard(cell, keyspace, shard); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteSrvShard(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
	}
	return err
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

func (tee *Tee) GetSrvKeyspaceNames(cell string) ([]string, error) {
	return tee.readFrom.GetSrvKeyspaceNames(cell)
}

func (tee *Tee) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.EndPoint) error {
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

func (tee *Tee) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	// lock lockFirst
	pLockPath, err := tee.lockFirst.LockKeyspaceForAction(ctx, keyspace, contents)
	if err != nil {
		return "", err
	}

	// lock lockSecond
	sLockPath, err := tee.lockSecond.LockKeyspaceForAction(ctx, keyspace, contents)
	if err != nil {
		if err := tee.lockFirst.UnlockKeyspaceForAction(keyspace, pLockPath, "{}"); err != nil {
			log.Warningf("Failed to unlock lockFirst keyspace after failed lockSecond lock for %v", keyspace)
		}
		return "", err
	}

	// remember both locks, keyed by lockFirst lock path
	tee.mu.Lock()
	tee.keyspaceLockPaths[pLockPath] = sLockPath
	tee.mu.Unlock()
	return pLockPath, nil
}

func (tee *Tee) UnlockKeyspaceForAction(keyspace, lockPath, results string) error {
	// get from map
	tee.mu.Lock() // not using defer for unlock, to minimize lock time
	sLockPath, ok := tee.keyspaceLockPaths[lockPath]
	if !ok {
		tee.mu.Unlock()
		return fmt.Errorf("no lockPath %v in keyspaceLockPaths", lockPath)
	}
	delete(tee.keyspaceLockPaths, lockPath)
	tee.mu.Unlock()

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

func (tee *Tee) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	// lock lockFirst
	pLockPath, err := tee.lockFirst.LockShardForAction(ctx, keyspace, shard, contents)
	if err != nil {
		return "", err
	}

	// lock lockSecond
	sLockPath, err := tee.lockSecond.LockShardForAction(ctx, keyspace, shard, contents)
	if err != nil {
		if err := tee.lockFirst.UnlockShardForAction(keyspace, shard, pLockPath, "{}"); err != nil {
			log.Warningf("Failed to unlock lockFirst shard after failed lockSecond lock for %v/%v", keyspace, shard)
		}
		return "", err
	}

	// remember both locks, keyed by lockFirst lock path
	tee.mu.Lock()
	tee.shardLockPaths[pLockPath] = sLockPath
	tee.mu.Unlock()
	return pLockPath, nil
}

func (tee *Tee) UnlockShardForAction(keyspace, shard, lockPath, results string) error {
	// get from map
	tee.mu.Lock() // not using defer for unlock, to minimize lock time
	sLockPath, ok := tee.shardLockPaths[lockPath]
	if !ok {
		tee.mu.Unlock()
		return fmt.Errorf("no lockPath %v in shardLockPaths", lockPath)
	}
	delete(tee.shardLockPaths, lockPath)
	tee.mu.Unlock()

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
// Supporting the local agent process, local cell.
//

func (tee *Tee) GetSubprocessFlags() []string {
	p := tee.primary.GetSubprocessFlags()
	return append(p, tee.secondary.GetSubprocessFlags()...)
}
