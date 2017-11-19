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
	"golang.org/x/net/context"
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

	keyspaceLockPaths map[string]string
	shardLockPaths    map[string]string
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
		primary:           primary,
		secondary:         secondary,
		readFrom:          primary,
		readFromSecond:    secondary,
		lockFirst:         lockFirst,
		lockSecond:        lockSecond,
		keyspaceLockPaths: make(map[string]string),
		shardLockPaths:    make(map[string]string),
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
		// Failed on primary, not updating secondary.
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

// NewMasterParticipation is part of the topo.Server interface
func (tee *Tee) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	return tee.primary.NewMasterParticipation(name, id)
}

var _ topo.Impl = (*Tee)(nil) // compile-time interface check
