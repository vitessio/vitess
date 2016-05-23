// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package actionnode

// This file contains utility functions to be used with actionnode /
// topology server.

import (
	"flag"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	// DefaultLockTimeout is a good value to use as a default for
	// locking a shard / keyspace.
	DefaultLockTimeout = 30 * time.Second

	// LockTimeout is the command line flag that introduces a shorter
	// timeout for locking topology structures.
	LockTimeout = flag.Duration("lock_timeout", DefaultLockTimeout, "timeout for acquiring topology locks")
)

// lockInfo is an individual info structure for a lock
type lockInfo struct {
	lockPath   string
	actionNode *ActionNode
}

// locksInfo is the structure used to remember which locks we took
type locksInfo struct {
	// mu protects the following members of the structure.
	// Safer to be thread safe here, in case multiple go routines
	// lock different things.
	mu sync.Mutex

	// info contans all the locks we took. It is indexed by
	// keyspace (for keyspaces) or keyspace/shard (for shards).
	info map[string]*lockInfo
}

// Context glue
type key int

var locksKey key

// LockKeyspaceContext will lock the keyspace, and return:
// - a context with a locksInfo structure for future reference.
// - an unlock method
// - an error if anything failed.
//
// We lock a keyspace for the following operations to be guaranteed
// exclusive operation:
// * changing a keyspace sharding info fields (is this one necessary?)
// * changing a keyspace 'ServedFrom' field (is this one necessary?)
// * resharding operations:
//   * horizontal resharding: includes changing the shard's 'ServedType',
//     as well as the associated horizontal resharding operations.
//   * vertical resharding: includes changing the keyspace 'ServedFrom'
//     field, as well as the associated vertical resharding operations.
//   * 'vtctl SetShardServedTypes' emergency operations
//   * 'vtctl SetShardTabletControl' emergency operations
//   * 'vtctl SourceShardAdd' and 'vtctl SourceShardDelete' emergency operations
// * keyspace-wide schema changes
func (n *ActionNode) LockKeyspaceContext(ctx context.Context, ts topo.Server, keyspace string) (context.Context, func(context.Context, *error), error) {
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		i = &locksInfo{
			info: make(map[string]*lockInfo),
		}
		ctx = context.WithValue(ctx, locksKey, i)
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// check that we're not already locked
	if _, ok = i.info[keyspace]; ok {
		return nil, nil, fmt.Errorf("lock for keyspace %v is already held", keyspace)
	}

	// lock
	lockPath, err := n.lockKeyspace(ctx, ts, keyspace)
	if err != nil {
		return nil, nil, err
	}

	// and update our structure
	i.info[keyspace] = &lockInfo{
		lockPath:   lockPath,
		actionNode: n,
	}
	return ctx, func(finalCtx context.Context, finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[keyspace]; !ok {
			*finalErr = fmt.Errorf("trying to unlock keyspace %v multiple times", keyspace)
			return
		}

		*finalErr = n.unlockKeyspace(finalCtx, ts, keyspace, lockPath, *finalErr)
		delete(i.info, keyspace)
	}, nil
}

// CheckKeyspaceLocked can be called on a context to make sure we have the lock
// for a given keyspace.
func CheckKeyspaceLocked(ctx context.Context, keyspace string) error {
	// extract the locksInfo pointer
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		return fmt.Errorf("keyspace %v is not locked (no locksInfo)", keyspace)
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// func the individual entry
	_, ok = i.info[keyspace]
	if !ok {
		return fmt.Errorf("keyspace %v is not locked (no lockInfo in map)", keyspace)
	}

	// TODO(alainjobart): check the lock server implementation
	// still holds the lock. Will need to look at the lockInfo struct.

	// and we're good for now.
	return nil
}

// lockKeyspace will lock the keyspace in the topology server.
// unlockKeyspace should be called if this returns no error.
func (n *ActionNode) lockKeyspace(ctx context.Context, ts topo.Server, keyspace string) (lockPath string, err error) {
	log.Infof("Locking keyspace %v for action %v", keyspace, n.Action)

	ctx, cancel := context.WithTimeout(ctx, *LockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.LockKeyspaceForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	defer span.Finish()

	j, err := n.ToJSON()
	if err != nil {
		return "", err
	}
	return ts.LockKeyspaceForAction(ctx, keyspace, j)
}

// unlockKeyspace unlocks a previously locked keyspace.
func (n *ActionNode) unlockKeyspace(ctx context.Context, ts topo.Server, keyspace string, lockPath string, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, DefaultLockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UnlockKeyspaceForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking keyspace %v for action %v with error %v", keyspace, n.Action, actionError)
		n.Error = actionError.Error()
		n.State = ActionStateFailed
	} else {
		log.Infof("Unlocking keyspace %v for successful action %v", keyspace, n.Action)
		n.Error = ""
		n.State = ActionStateDone
	}
	j, err := n.ToJSON()
	if err != nil {
		if actionError != nil {
			// this will be masked
			log.Warningf("node.ToJSON failed: %v", err)
			return actionError
		}
		return err
	}
	err = ts.UnlockKeyspaceForAction(ctx, keyspace, lockPath, j)
	if actionError != nil {
		if err != nil {
			// this will be masked
			log.Warningf("UnlockKeyspaceForAction failed: %v", err)
		}
		return actionError
	}
	return err
}

// LockShardContext will lock the shard, and return:
// - a context with a locksInfo structure for future reference.
// - an unlock method
// - an error if anything failed.
//
// We are currently only using this method to lock actions that would
// impact each-other. Most changes of the Shard object are done by
// UpdateShardFields, which is not locking the shard object. The
// current list of actions that lock a shard are:
// * all Vitess-controlled re-parenting operations:
//   * InitShardMaster
//   * PlannedReparentShard
//   * EmergencyReparentShard
// * operations that we don't want to conflict with re-parenting:
//   * DeleteTablet when it's the shard's current master
//
func (n *ActionNode) LockShardContext(ctx context.Context, ts topo.Server, keyspace, shard string) (context.Context, func(context.Context, *error), error) {
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		i = &locksInfo{
			info: make(map[string]*lockInfo),
		}
		ctx = context.WithValue(ctx, locksKey, i)
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// check that we're not already locked
	mapKey := keyspace + "/" + shard
	if _, ok = i.info[mapKey]; ok {
		return nil, nil, fmt.Errorf("lock for shard %v/%v is already held", keyspace, shard)
	}

	// lock
	lockPath, err := n.LockShard(ctx, ts, keyspace, shard)
	if err != nil {
		return nil, nil, err
	}

	// and update our structure
	i.info[mapKey] = &lockInfo{
		lockPath:   lockPath,
		actionNode: n,
	}
	return ctx, func(finalCtx context.Context, finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[mapKey]; !ok {
			*finalErr = fmt.Errorf("trying to unlock shard %v/%v multiple times", keyspace, shard)
			return
		}

		*finalErr = n.UnlockShard(finalCtx, ts, keyspace, shard, lockPath, *finalErr)
		delete(i.info, mapKey)
	}, nil
}

// CheckShardLocked can be called on a context to make sure we have the lock
// for a given shard.
func CheckShardLocked(ctx context.Context, keyspace, shard string) error {
	// extract the locksInfo pointer
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		return fmt.Errorf("shard %v/%v is not locked (no locksInfo)", keyspace, shard)
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// func the individual entry
	mapKey := keyspace + "/" + shard
	_, ok = i.info[mapKey]
	if !ok {
		return fmt.Errorf("shard %v/%v is not locked (no lockInfo in map)", keyspace, shard)
	}

	// TODO(alainjobart): check the lock server implementation
	// still holds the lock. Will need to look at the lockInfo struct.

	// and we're good for now.
	return nil
}

// LockShard will lock the shard in the topology server.
// UnlockShard should be called if this returns no error.
func (n *ActionNode) LockShard(ctx context.Context, ts topo.Server, keyspace, shard string) (lockPath string, err error) {
	log.Infof("Locking shard %v/%v for action %v", keyspace, shard, n.Action)

	ctx, cancel := context.WithTimeout(ctx, *LockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.LockShardForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	j, err := n.ToJSON()
	if err != nil {
		return "", err
	}
	return ts.LockShardForAction(ctx, keyspace, shard, j)
}

// UnlockShard unlocks a previously locked shard.
func (n *ActionNode) UnlockShard(ctx context.Context, ts topo.Server, keyspace, shard string, lockPath string, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, DefaultLockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UnlockShardForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking shard %v/%v for action %v with error %v", keyspace, shard, n.Action, actionError)
		n.Error = actionError.Error()
		n.State = ActionStateFailed
	} else {
		log.Infof("Unlocking shard %v/%v for successful action %v", keyspace, shard, n.Action)
		n.Error = ""
		n.State = ActionStateDone
	}
	j, err := n.ToJSON()
	if err != nil {
		if actionError != nil {
			// this will be masked
			log.Warningf("node.ToJSON failed: %v", err)
			return actionError
		}
		return err
	}
	err = ts.UnlockShardForAction(ctx, keyspace, shard, lockPath, j)
	if actionError != nil {
		if err != nil {
			// this will be masked
			log.Warningf("UnlockShardForAction failed: %v", err)
		}
		return actionError
	}
	return err
}
