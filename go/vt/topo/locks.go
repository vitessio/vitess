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

package topo

import (
	"encoding/json"
	"flag"
	"os"
	"os/user"
	"path"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
)

// This file contains utility methods and definitions to lock
// keyspaces and shards.

var (
	// DefaultLockTimeout is a good value to use as a default for
	// locking a shard / keyspace.
	// Now used only for unlock operations
	defaultLockTimeout = 30 * time.Second

	// Deprecated
	// LockTimeout is the command line flag that introduces a shorter
	// timeout for locking topology structures.
	_ = flag.Duration("lock_timeout", defaultLockTimeout, "deprecated: timeout for acquiring topology locks, use remote_operation_timeout")

	// RemoteOperationTimeout is used for operations where we have to
	// call out to another process.
	// Used for RPC calls (including topo server calls)
	RemoteOperationTimeout = flag.Duration("remote_operation_timeout", 30*time.Second, "time to wait for a remote operation")
)

// Lock describes a long-running lock on a keyspace or a shard.
// It needs to be public as we JSON-serialize it.
type Lock struct {
	// Action and the following fields are set at construction time.
	Action   string
	HostName string
	UserName string
	Time     string

	// Status is the current status of the Lock.
	Status string
}

// newLock creates a new Lock.
func newLock(action string) *Lock {
	l := &Lock{
		Action:   action,
		HostName: "unknown",
		UserName: "unknown",
		Time:     time.Now().Format(time.RFC3339),
		Status:   "Running",
	}
	if h, err := os.Hostname(); err == nil {
		l.HostName = h
	}
	if u, err := user.Current(); err == nil {
		l.UserName = u.Username
	}
	return l
}

// ToJSON returns a JSON representation of the object.
func (l *Lock) ToJSON() (string, error) {
	data, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return "", vterrors.Wrapf(err, "cannot JSON-marshal node")
	}
	return string(data), nil
}

// lockInfo is an individual info structure for a lock
type lockInfo struct {
	lockDescriptor LockDescriptor
	actionNode     *Lock
}

// locksInfo is the structure used to remember which locks we took
type locksInfo struct {
	// mu protects the following members of the structure.
	// Safer to be thread safe here, in case multiple go routines
	// lock different things.
	mu sync.Mutex

	// info contains all the locks we took. It is indexed by
	// keyspace (for keyspaces) or keyspace/shard (for shards).
	info map[string]*lockInfo
}

// Context glue
type locksKeyType int

var locksKey locksKeyType

// LockKeyspace will lock the keyspace, and return:
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
//   * 'vtctl SetShardIsMasterServing' emergency operations
//   * 'vtctl SetShardTabletControl' emergency operations
//   * 'vtctl SourceShardAdd' and 'vtctl SourceShardDelete' emergency operations
// * keyspace-wide schema changes
func (ts *Server) LockKeyspace(ctx context.Context, keyspace, action string) (context.Context, func(*error), error) {
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
		return nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "lock for keyspace %v is already held", keyspace)
	}

	// lock
	l := newLock(action)
	lockDescriptor, err := l.lockKeyspace(ctx, ts, keyspace)
	if err != nil {
		return nil, nil, err
	}

	// and update our structure
	i.info[keyspace] = &lockInfo{
		lockDescriptor: lockDescriptor,
		actionNode:     l,
	}
	return ctx, func(finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[keyspace]; !ok {
			if *finalErr != nil {
				log.Errorf("trying to unlock keyspace %v multiple times", keyspace)
			} else {
				*finalErr = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "trying to unlock keyspace %v multiple times", keyspace)
			}
			return
		}

		err := l.unlockKeyspace(ctx, ts, keyspace, lockDescriptor, *finalErr)
		if *finalErr != nil {
			if err != nil {
				// both error are set, just log the unlock error
				log.Errorf("unlockKeyspace(%v) failed: %v", keyspace, err)
			}
		} else {
			*finalErr = err
		}
		delete(i.info, keyspace)
	}, nil
}

// CheckKeyspaceLocked can be called on a context to make sure we have the lock
// for a given keyspace.
func CheckKeyspaceLocked(ctx context.Context, keyspace string) error {
	// extract the locksInfo pointer
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "keyspace %v is not locked (no locksInfo)", keyspace)
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// find the individual entry
	_, ok = i.info[keyspace]
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "keyspace %v is not locked (no lockInfo in map)", keyspace)
	}

	// TODO(alainjobart): check the lock server implementation
	// still holds the lock. Will need to look at the lockInfo struct.

	// and we're good for now.
	return nil
}

// lockKeyspace will lock the keyspace in the topology server.
// unlockKeyspace should be called if this returns no error.
func (l *Lock) lockKeyspace(ctx context.Context, ts *Server, keyspace string) (LockDescriptor, error) {
	log.Infof("Locking keyspace %v for action %v", keyspace, l.Action)

	ctx, cancel := context.WithTimeout(ctx, *RemoteOperationTimeout)
	defer cancel()

	span, ctx := trace.NewSpan(ctx, "TopoServer.LockKeyspaceForAction")
	span.Annotate("action", l.Action)
	span.Annotate("keyspace", keyspace)
	defer span.Finish()

	keyspacePath := path.Join(KeyspacesPath, keyspace)
	j, err := l.ToJSON()
	if err != nil {
		return nil, err
	}
	return ts.globalCell.Lock(ctx, keyspacePath, j)
}

// unlockKeyspace unlocks a previously locked keyspace.
func (l *Lock) unlockKeyspace(ctx context.Context, ts *Server, keyspace string, lockDescriptor LockDescriptor, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent
	// context timed out.
	// Note that we are not using the user provided RemoteOperationTimeout
	// here because it is possible that that timeout is too short.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, defaultLockTimeout)
	defer cancel()

	span, ctx := trace.NewSpan(ctx, "TopoServer.UnlockKeyspaceForAction")
	span.Annotate("action", l.Action)
	span.Annotate("keyspace", keyspace)
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking keyspace %v for action %v with error %v", keyspace, l.Action, actionError)
		l.Status = "Error: " + actionError.Error()
	} else {
		log.Infof("Unlocking keyspace %v for successful action %v", keyspace, l.Action)
		l.Status = "Done"
	}
	return lockDescriptor.Unlock(ctx)
}

// LockShard will lock the shard, and return:
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
func (ts *Server) LockShard(ctx context.Context, keyspace, shard, action string) (context.Context, func(*error), error) {
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
		return nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "lock for shard %v/%v is already held", keyspace, shard)
	}

	// lock
	l := newLock(action)
	lockDescriptor, err := l.lockShard(ctx, ts, keyspace, shard)
	if err != nil {
		return nil, nil, err
	}

	// and update our structure
	i.info[mapKey] = &lockInfo{
		lockDescriptor: lockDescriptor,
		actionNode:     l,
	}
	return ctx, func(finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[mapKey]; !ok {
			if *finalErr != nil {
				log.Errorf("trying to unlock shard %v/%v multiple times", keyspace, shard)
			} else {
				*finalErr = vterrors.Errorf(vtrpc.Code_INTERNAL, "trying to unlock shard %v/%v multiple times", keyspace, shard)
			}
			return
		}

		err := l.unlockShard(ctx, ts, keyspace, shard, lockDescriptor, *finalErr)
		if *finalErr != nil {
			if err != nil {
				// both error are set, just log the unlock error
				log.Warningf("unlockShard(%s/%s) failed: %v", keyspace, shard, err)
			}
		} else {
			*finalErr = err
		}
		delete(i.info, mapKey)
	}, nil
}

// CheckShardLocked can be called on a context to make sure we have the lock
// for a given shard.
func CheckShardLocked(ctx context.Context, keyspace, shard string) error {
	// extract the locksInfo pointer
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "shard %v/%v is not locked (no locksInfo)", keyspace, shard)
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// func the individual entry
	mapKey := keyspace + "/" + shard
	li, ok := i.info[mapKey]
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "shard %v/%v is not locked (no lockInfo in map)", keyspace, shard)
	}

	// Check the lock server implementation still holds the lock.
	return li.lockDescriptor.Check(ctx)
}

// lockShard will lock the shard in the topology server.
// UnlockShard should be called if this returns no error.
func (l *Lock) lockShard(ctx context.Context, ts *Server, keyspace, shard string) (LockDescriptor, error) {
	log.Infof("Locking shard %v/%v for action %v", keyspace, shard, l.Action)

	ctx, cancel := context.WithTimeout(ctx, *RemoteOperationTimeout)
	defer cancel()

	span, ctx := trace.NewSpan(ctx, "TopoServer.LockShardForAction")
	span.Annotate("action", l.Action)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	shardPath := path.Join(KeyspacesPath, keyspace, ShardsPath, shard)
	j, err := l.ToJSON()
	if err != nil {
		return nil, err
	}
	return ts.globalCell.Lock(ctx, shardPath, j)
}

// unlockShard unlocks a previously locked shard.
func (l *Lock) unlockShard(ctx context.Context, ts *Server, keyspace, shard string, lockDescriptor LockDescriptor, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent context timed out.
	// Note that we are not using the user provided RemoteOperationTimeout
	// here because it is possible that that timeout is too short.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, defaultLockTimeout)
	defer cancel()

	span, ctx := trace.NewSpan(ctx, "TopoServer.UnlockShardForAction")
	span.Annotate("action", l.Action)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking shard %v/%v for action %v with error %v", keyspace, shard, l.Action, actionError)
		l.Status = "Error: " + actionError.Error()
	} else {
		log.Infof("Unlocking shard %v/%v for successful action %v", keyspace, shard, l.Action)
		l.Status = "Done"
	}
	return lockDescriptor.Unlock(ctx)
}
