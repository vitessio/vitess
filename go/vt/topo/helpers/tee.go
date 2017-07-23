/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package helpers

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// Tee is an implementation of topo.Server that uses a primary
// underlying topo.Server for all changes, but also duplicates the
// changes to a secondary topo.Server. It also locks both topo servers
// when needed.  It is meant to be used during transitions from one
// topo.Server to another.
//
// - primary: we read everything from it, and write to it. We also create
//     MasterParticipation from it.
// - secondary: we write to it as well, but we usually don't fail.
// - we lock primary/secondary if reverseLockOrder is False,
// or secondary/primary if reverseLockOrder is True.
type Tee struct {
	primary   topo.Impl
	secondary topo.Impl

	readFrom       topo.Impl
	readFromSecond topo.Impl

	lockFirst  topo.Impl
	lockSecond topo.Impl

	// protects the variables below this point
	mu sync.Mutex

	keyspaceVersionMapping map[string]versionMapping
	shardVersionMapping    map[string]versionMapping
	tabletVersionMapping   map[string]versionMapping

	keyspaceLockPaths map[string]string
	shardLockPaths    map[string]string
}

// When reading a version from 'readFrom', we also read another version
// from 'readFromSecond', and save the mapping to this map. We only keep one
// mapping for a given object, no need to overdo it.
// FIXME(alainjobart) remove this when topo API is all converted to Backend.
type versionMapping struct {
	readFromVersion       int64
	readFromSecondVersion int64
}

// NewTee returns a new topo.Impl object
func NewTee(primary, secondary topo.Impl, reverseLockOrder bool) *Tee {
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
		tabletVersionMapping:   make(map[string]versionMapping),
		keyspaceLockPaths:      make(map[string]string),
		shardLockPaths:         make(map[string]string),
	}
}

//
// topo.Server management interface.
//

// Close is part of the topo.Server interface
func (tee *Tee) Close() {
	tee.primary.Close()
	tee.secondary.Close()
}

//
// Backend API
//

// ListDir is part of the topo.Backend interface.
func (tee *Tee) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	return tee.primary.ListDir(ctx, cell, dirPath)
}

// Create is part of the topo.Backend interface.
func (tee *Tee) Create(ctx context.Context, cell, filePath string, contents []byte) (topo.Version, error) {
	primaryVersion, err := tee.primary.Create(ctx, cell, filePath, contents)
	if err != nil {
		return nil, err
	}

	// This is critical enough that we want to fail. However, we support
	// an unconditional update if the file already exists.
	_, err = tee.secondary.Create(ctx, cell, filePath, contents)
	if err == topo.ErrNodeExists {
		_, err = tee.secondary.Update(ctx, cell, filePath, contents, nil)
	}
	if err != nil {
		return nil, err
	}

	return primaryVersion, nil
}

// Update is part of the topo.Backend interface.
func (tee *Tee) Update(ctx context.Context, cell, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	primaryVersion, err := tee.primary.Update(ctx, cell, filePath, contents, version)
	if err != nil {
		// failed on primary, not updating secondary
		return nil, err
	}

	// Always do an unconditional update on secondary.
	if _, err = tee.secondary.Update(ctx, cell, filePath, contents, nil); err != nil {
		log.Warningf("secondary.Update(%v,%v,unconditonal) failed: %v", cell, filePath, err)
	}
	return primaryVersion, nil
}

// Get is part of the topo.Backend interface.
func (tee *Tee) Get(ctx context.Context, cell, filePath string) ([]byte, topo.Version, error) {
	return tee.primary.Get(ctx, cell, filePath)
}

// Delete is part of the topo.Backend interface.
func (tee *Tee) Delete(ctx context.Context, cell, filePath string, version topo.Version) error {
	// If primary fails, no need to go further.
	if err := tee.primary.Delete(ctx, cell, filePath, version); err != nil {
		return err
	}

	// Always do an unconditonal delete on secondary.
	if err := tee.secondary.Delete(ctx, cell, filePath, nil); err != nil && err != topo.ErrNoNode {
		// Secondary didn't work, and the node wasn't gone already.
		log.Warningf("secondary.Delete(%v,%v) failed: %v", cell, filePath, err)
	}

	return nil
}

// Watch is part of the topo.Backend interface
func (tee *Tee) Watch(ctx context.Context, cell, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	return tee.primary.Watch(ctx, cell, filePath)
}

//
// Cell management, global
//

// GetKnownCells is part of the topo.Server interface
func (tee *Tee) GetKnownCells(ctx context.Context) ([]string, error) {
	return tee.readFrom.GetKnownCells(ctx)
}

//
// Keyspace management, global.
//

// CreateKeyspace is part of the topo.Server interface
func (tee *Tee) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	if err := tee.primary.CreateKeyspace(ctx, keyspace, value); err != nil {
		return err
	}

	// this is critical enough that we want to fail
	if err := tee.secondary.CreateKeyspace(ctx, keyspace, value); err != nil {
		return err
	}
	return nil
}

// UpdateKeyspace is part of the topo.Server interface
func (tee *Tee) UpdateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace, existingVersion int64) (newVersion int64, err error) {
	if newVersion, err = tee.primary.UpdateKeyspace(ctx, keyspace, value, existingVersion); err != nil {
		// failed on primary, not updating secondary
		return
	}

	// if we have a mapping between keyspace version in first topo
	// and keyspace version in second topo, replace the version number.
	// if not, this will probably fail and log.
	tee.mu.Lock()
	kvm, ok := tee.keyspaceVersionMapping[keyspace]
	if ok && kvm.readFromVersion == existingVersion {
		existingVersion = kvm.readFromSecondVersion
		delete(tee.keyspaceVersionMapping, keyspace)
	}
	tee.mu.Unlock()
	if newVersion2, serr := tee.secondary.UpdateKeyspace(ctx, keyspace, value, existingVersion); serr != nil {
		// not critical enough to fail
		if serr == topo.ErrNoNode {
			// the keyspace doesn't exist on the secondary, let's
			// just create it
			if serr = tee.secondary.CreateKeyspace(ctx, keyspace, value); serr != nil {
				log.Warningf("secondary.CreateKeyspace(%v) failed (after UpdateKeyspace returned ErrNoNode): %v", keyspace, serr)
			} else {
				log.Infof("secondary.UpdateKeyspace(%v) failed with ErrNoNode, CreateKeyspace then worked.", keyspace)
				_, secondaryVersion, gerr := tee.secondary.GetKeyspace(ctx, keyspace)
				if gerr != nil {
					log.Warningf("Failed to re-read keyspace(%v) after creating it on secondary: %v", keyspace, gerr)
				} else {
					tee.mu.Lock()
					tee.keyspaceVersionMapping[keyspace] = versionMapping{
						readFromVersion:       newVersion,
						readFromSecondVersion: secondaryVersion,
					}
					tee.mu.Unlock()
				}
			}
		} else {
			log.Warningf("secondary.UpdateKeyspace(%v) failed: %v", keyspace, serr)
		}
	} else {
		tee.mu.Lock()
		tee.keyspaceVersionMapping[keyspace] = versionMapping{
			readFromVersion:       newVersion,
			readFromSecondVersion: newVersion2,
		}
		tee.mu.Unlock()
	}
	return
}

// DeleteKeyspace is part of the topo.Server interface
func (tee *Tee) DeleteKeyspace(ctx context.Context, keyspace string) error {
	err := tee.primary.DeleteKeyspace(ctx, keyspace)
	if err != nil && err != topo.ErrNoNode {
		return err
	}

	if err := tee.secondary.DeleteKeyspace(ctx, keyspace); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteKeyspace(%v) failed: %v", keyspace, err)
	}
	return err
}

// GetKeyspace is part of the topo.Server interface
func (tee *Tee) GetKeyspace(ctx context.Context, keyspace string) (*topodatapb.Keyspace, int64, error) {
	k, version, err := tee.readFrom.GetKeyspace(ctx, keyspace)
	if err != nil {
		return nil, 0, err
	}

	_, version2, err := tee.readFromSecond.GetKeyspace(ctx, keyspace)
	if err != nil {
		// can't read from secondary, so we can's keep version map
		return k, version, nil
	}

	tee.mu.Lock()
	tee.keyspaceVersionMapping[keyspace] = versionMapping{
		readFromVersion:       version,
		readFromSecondVersion: version2,
	}
	tee.mu.Unlock()
	return k, version, nil
}

// GetKeyspaces is part of the topo.Server interface
func (tee *Tee) GetKeyspaces(ctx context.Context) ([]string, error) {
	return tee.readFrom.GetKeyspaces(ctx)
}

//
// Shard management, global.
//

// CreateShard is part of the topo.Server interface
func (tee *Tee) CreateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard) error {
	err := tee.primary.CreateShard(ctx, keyspace, shard, value)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	serr := tee.secondary.CreateShard(ctx, keyspace, shard, value)
	if serr != nil && serr != topo.ErrNodeExists {
		// not critical enough to fail
		log.Warningf("secondary.CreateShard(%v,%v) failed: %v", keyspace, shard, err)
	}
	return err
}

// UpdateShard is part of the topo.Server interface
func (tee *Tee) UpdateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard, existingVersion int64) (newVersion int64, err error) {
	if newVersion, err = tee.primary.UpdateShard(ctx, keyspace, shard, value, existingVersion); err != nil {
		// failed on primary, not updating secondary
		return
	}

	// if we have a mapping between shard version in first topo
	// and shard version in second topo, replace the version number.
	// if not, this will probably fail and log.
	tee.mu.Lock()
	svm, ok := tee.shardVersionMapping[keyspace+"/"+shard]
	if ok && svm.readFromVersion == existingVersion {
		existingVersion = svm.readFromSecondVersion
		delete(tee.shardVersionMapping, keyspace+"/"+shard)
	}
	tee.mu.Unlock()
	if newVersion2, serr := tee.secondary.UpdateShard(ctx, keyspace, shard, value, existingVersion); serr != nil {
		// not critical enough to fail
		if serr == topo.ErrNoNode {
			// the shard doesn't exist on the secondary, let's
			// just create it
			if serr = tee.secondary.CreateShard(ctx, keyspace, shard, value); serr != nil {
				log.Warningf("secondary.CreateShard(%v,%v) failed (after UpdateShard returned ErrNoNode): %v", keyspace, shard, serr)
			} else {
				log.Infof("secondary.UpdateShard(%v, %v) failed with ErrNoNode, CreateShard then worked.", keyspace, shard)
				_, v, gerr := tee.secondary.GetShard(ctx, keyspace, shard)
				if gerr != nil {
					log.Warningf("Failed to re-read shard(%v, %v) after creating it on secondary: %v", keyspace, shard, gerr)
				} else {
					tee.mu.Lock()
					tee.shardVersionMapping[keyspace+"/"+shard] = versionMapping{
						readFromVersion:       newVersion,
						readFromSecondVersion: v,
					}
					tee.mu.Unlock()
				}
			}
		} else {
			log.Warningf("secondary.UpdateShard(%v, %v) failed: %v", keyspace, shard, serr)
		}
	} else {
		tee.mu.Lock()
		tee.shardVersionMapping[keyspace+"/"+shard] = versionMapping{
			readFromVersion:       newVersion,
			readFromSecondVersion: newVersion2,
		}
		tee.mu.Unlock()
	}
	return
}

// GetShard is part of the topo.Server interface
func (tee *Tee) GetShard(ctx context.Context, keyspace, shard string) (*topodatapb.Shard, int64, error) {
	s, v, err := tee.readFrom.GetShard(ctx, keyspace, shard)
	if err != nil {
		return nil, 0, err
	}

	_, v2, err := tee.readFromSecond.GetShard(ctx, keyspace, shard)
	if err != nil {
		// can't read from secondary, so we can's keep version map
		return s, v, nil
	}

	tee.mu.Lock()
	tee.shardVersionMapping[keyspace+"/"+shard] = versionMapping{
		readFromVersion:       v,
		readFromSecondVersion: v2,
	}
	tee.mu.Unlock()
	return s, v, nil
}

// GetShardNames is part of the topo.Server interface
func (tee *Tee) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	return tee.readFrom.GetShardNames(ctx, keyspace)
}

// DeleteShard is part of the topo.Server interface
func (tee *Tee) DeleteShard(ctx context.Context, keyspace, shard string) error {
	err := tee.primary.DeleteShard(ctx, keyspace, shard)
	if err != nil && err != topo.ErrNoNode {
		return err
	}

	if err := tee.secondary.DeleteShard(ctx, keyspace, shard); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteShard(%v, %v) failed: %v", keyspace, shard, err)
	}
	return err
}

//
// Tablet management, per cell.
//

// CreateTablet is part of the topo.Server interface
func (tee *Tee) CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error {
	err := tee.primary.CreateTablet(ctx, tablet)
	if err != nil && err != topo.ErrNodeExists {
		return err
	}

	if err := tee.primary.CreateTablet(ctx, tablet); err != nil && err != topo.ErrNodeExists {
		// not critical enough to fail
		log.Warningf("secondary.CreateTablet(%v) failed: %v", tablet.Alias, err)
	}
	return err
}

// UpdateTablet is part of the topo.Server interface
func (tee *Tee) UpdateTablet(ctx context.Context, tablet *topodatapb.Tablet, existingVersion int64) (newVersion int64, err error) {
	if newVersion, err = tee.primary.UpdateTablet(ctx, tablet, existingVersion); err != nil {
		// failed on primary, not updating secondary
		return
	}

	// if we have a mapping between tablet version in first topo
	// and tablet version in second topo, replace the version number.
	// if not, this will probably fail and log.
	tabletAliasStr := topoproto.TabletAliasString(tablet.Alias)
	tee.mu.Lock()
	tvm, ok := tee.tabletVersionMapping[tabletAliasStr]
	if ok && tvm.readFromVersion == existingVersion {
		existingVersion = tvm.readFromSecondVersion
		delete(tee.tabletVersionMapping, tabletAliasStr)
	}
	tee.mu.Unlock()
	if newVersion2, serr := tee.secondary.UpdateTablet(ctx, tablet, existingVersion); serr != nil {
		// not critical enough to fail
		if serr == topo.ErrNoNode {
			// the tablet doesn't exist on the secondary, let's
			// just create it
			if serr = tee.secondary.CreateTablet(ctx, tablet); serr != nil {
				log.Warningf("secondary.CreateTablet(%v) failed (after UpdateTablet returned ErrNoNode): %v", tablet.Alias, serr)
			} else {
				log.Infof("secondary.UpdateTablet(%v) failed with ErrNoNode, CreateTablet then worked.", tablet.Alias)
				_, v, gerr := tee.secondary.GetTablet(ctx, tablet.Alias)
				if gerr != nil {
					log.Warningf("Failed to re-read tablet(%v) after creating it on secondary: %v", tablet.Alias, gerr)
				} else {
					tee.mu.Lock()
					tee.tabletVersionMapping[tabletAliasStr] = versionMapping{
						readFromVersion:       newVersion,
						readFromSecondVersion: v,
					}
					tee.mu.Unlock()
				}
			}
		} else {
			log.Warningf("secondary.UpdateTablet(%v) failed: %v", tablet.Alias, serr)
		}
	} else {
		tee.mu.Lock()
		tee.tabletVersionMapping[tabletAliasStr] = versionMapping{
			readFromVersion:       newVersion,
			readFromSecondVersion: newVersion2,
		}
		tee.mu.Unlock()
	}
	return
}

// DeleteTablet is part of the topo.Server interface
func (tee *Tee) DeleteTablet(ctx context.Context, alias *topodatapb.TabletAlias) error {
	if err := tee.primary.DeleteTablet(ctx, alias); err != nil {
		return err
	}

	if err := tee.secondary.DeleteTablet(ctx, alias); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteTablet(%v) failed: %v", alias, err)
	}
	return nil
}

// GetTablet is part of the topo.Server interface
func (tee *Tee) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error) {
	t, v, err := tee.readFrom.GetTablet(ctx, alias)
	if err != nil {
		return nil, 0, err
	}

	_, v2, err := tee.readFromSecond.GetTablet(ctx, alias)
	if err != nil {
		// can't read from secondary, so we can's keep version map
		return t, v, nil
	}

	tee.mu.Lock()
	tee.tabletVersionMapping[topoproto.TabletAliasString(alias)] = versionMapping{
		readFromVersion:       v,
		readFromSecondVersion: v2,
	}
	tee.mu.Unlock()
	return t, v, nil
}

// GetTabletsByCell is part of the topo.Server interface
func (tee *Tee) GetTabletsByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error) {
	return tee.readFrom.GetTabletsByCell(ctx, cell)
}

//
// Shard replication graph management, local.
//

// UpdateShardReplicationFields is part of the topo.Server interface
func (tee *Tee) UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*topodatapb.ShardReplication) error) error {
	if err := tee.primary.UpdateShardReplicationFields(ctx, cell, keyspace, shard, update); err != nil {
		// failed on primary, not updating secondary
		return err
	}

	if err := tee.secondary.UpdateShardReplicationFields(ctx, cell, keyspace, shard, update); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateShardReplicationFields(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
	}
	return nil
}

// GetShardReplication is part of the topo.Server interface
func (tee *Tee) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*topo.ShardReplicationInfo, error) {
	return tee.readFrom.GetShardReplication(ctx, cell, keyspace, shard)
}

// DeleteShardReplication is part of the topo.Server interface
func (tee *Tee) DeleteShardReplication(ctx context.Context, cell, keyspace, shard string) error {
	if err := tee.primary.DeleteShardReplication(ctx, cell, keyspace, shard); err != nil {
		return err
	}

	if err := tee.secondary.DeleteShardReplication(ctx, cell, keyspace, shard); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteShardReplication(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
	}
	return nil
}

// DeleteKeyspaceReplication is part of the topo.Server interface
func (tee *Tee) DeleteKeyspaceReplication(ctx context.Context, cell, keyspace string) error {
	if err := tee.primary.DeleteKeyspaceReplication(ctx, cell, keyspace); err != nil {
		return err
	}

	if err := tee.secondary.DeleteKeyspaceReplication(ctx, cell, keyspace); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteKeyspaceReplication(%v, %v) failed: %v", cell, keyspace, err)
	}
	return nil
}

//
// Serving Graph management, per cell.
//

// GetSrvKeyspaceNames is part of the topo.Server interface
func (tee *Tee) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	return tee.readFrom.GetSrvKeyspaceNames(ctx, cell)
}

// UpdateSrvKeyspace is part of the topo.Server interface
func (tee *Tee) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	if err := tee.primary.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace); err != nil {
		return err
	}

	if err := tee.secondary.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateSrvKeyspace(%v, %v) failed: %v", cell, keyspace, err)
	}
	return nil
}

// DeleteSrvKeyspace is part of the topo.Server interface
func (tee *Tee) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	err := tee.primary.DeleteSrvKeyspace(ctx, cell, keyspace)
	if err != nil && err != topo.ErrNoNode {
		return err
	}

	if err := tee.secondary.DeleteSrvKeyspace(ctx, cell, keyspace); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.DeleteSrvKeyspace(%v, %v) failed: %v", cell, keyspace, err)
	}
	return err
}

// GetSrvKeyspace is part of the topo.Server interface
func (tee *Tee) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return tee.readFrom.GetSrvKeyspace(ctx, cell, keyspace)
}

// UpdateSrvVSchema is part of the topo.Server interface
func (tee *Tee) UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error {
	if err := tee.primary.UpdateSrvVSchema(ctx, cell, srvVSchema); err != nil {
		return err
	}

	if err := tee.secondary.UpdateSrvVSchema(ctx, cell, srvVSchema); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.UpdateSrvVSchema(%v) failed: %v", cell, err)
	}
	return nil
}

// GetSrvVSchema is part of the topo.Server interface
func (tee *Tee) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	return tee.readFrom.GetSrvVSchema(ctx, cell)
}

//
// Keyspace and Shard locks for actions, global.
//

// LockKeyspaceForAction is part of the topo.Server interface
func (tee *Tee) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	// lock lockFirst
	pLockPath, err := tee.lockFirst.LockKeyspaceForAction(ctx, keyspace, contents)
	if err != nil {
		return "", err
	}

	// lock lockSecond
	sLockPath, err := tee.lockSecond.LockKeyspaceForAction(ctx, keyspace, contents)
	if err != nil {
		if err := tee.lockFirst.UnlockKeyspaceForAction(ctx, keyspace, pLockPath, "{}"); err != nil {
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

// UnlockKeyspaceForAction is part of the topo.Server interface
func (tee *Tee) UnlockKeyspaceForAction(ctx context.Context, keyspace, lockPath, results string) error {
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
	serr := tee.lockSecond.UnlockKeyspaceForAction(ctx, keyspace, sLockPath, results)
	perr := tee.lockFirst.UnlockKeyspaceForAction(ctx, keyspace, lockPath, results)

	if serr != nil {
		if perr != nil {
			log.Warningf("Secondary UnlockKeyspaceForAction(%v, %v) failed: %v", keyspace, sLockPath, serr)
		}
		return serr
	}
	return perr
}

// LockShardForAction is part of the topo.Server interface
func (tee *Tee) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	// lock lockFirst
	pLockPath, err := tee.lockFirst.LockShardForAction(ctx, keyspace, shard, contents)
	if err != nil {
		return "", err
	}

	// lock lockSecond
	sLockPath, err := tee.lockSecond.LockShardForAction(ctx, keyspace, shard, contents)
	if err != nil {
		if err := tee.lockFirst.UnlockShardForAction(ctx, keyspace, shard, pLockPath, "{}"); err != nil {
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

// UnlockShardForAction is part of the topo.Server interface
func (tee *Tee) UnlockShardForAction(ctx context.Context, keyspace, shard, lockPath, results string) error {
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
	serr := tee.lockSecond.UnlockShardForAction(ctx, keyspace, shard, sLockPath, results)
	perr := tee.lockFirst.UnlockShardForAction(ctx, keyspace, shard, lockPath, results)

	if serr != nil {
		if perr != nil {
			log.Warningf("Secondary UnlockShardForAction(%v/%v, %v) failed: %v", keyspace, shard, sLockPath, serr)
		}
		return serr
	}
	return perr
}

// SaveVSchema is part of the topo.Server interface
func (tee *Tee) SaveVSchema(ctx context.Context, keyspace string, contents *vschemapb.Keyspace) error {
	err := tee.primary.SaveVSchema(ctx, keyspace, contents)
	if err != nil {
		return err
	}

	if err := tee.secondary.SaveVSchema(ctx, keyspace, contents); err != nil {
		// not critical enough to fail
		log.Warningf("secondary.SaveVSchema() failed: %v", err)
	}
	return err
}

// GetVSchema is part of the topo.Server interface
func (tee *Tee) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	return tee.readFrom.GetVSchema(ctx, keyspace)
}

// NewMasterParticipation is part of the topo.Server interface
func (tee *Tee) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	return tee.primary.NewMasterParticipation(name, id)
}

var _ topo.Impl = (*Tee)(nil) // compile-time interface check
