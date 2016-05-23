// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/trace"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains utility methods and definitions to lock
// keyspaces and shards.

var (
	// DefaultLockTimeout is a good value to use as a default for
	// locking a shard / keyspace.
	DefaultLockTimeout = 30 * time.Second

	// LockTimeout is the command line flag that introduces a shorter
	// timeout for locking topology structures.
	LockTimeout = flag.Duration("lock_timeout", DefaultLockTimeout, "timeout for acquiring topology locks")
)

// Action is the type for all Lock objects
type Action string

const (
	//
	// Shard actions - involve all tablets in a shard.
	// These are just descriptive and used for locking / logging.
	//

	// ShardActionReparent handles reparenting of the shard
	ShardActionReparent = Action("ReparentShard")

	// ShardActionExternallyReparented locks the shard when it's
	// been reparented
	ShardActionExternallyReparented = Action("ShardExternallyReparented")

	// ShardActionCheck takes a generic read lock for inexpensive
	// shard-wide actions.
	ShardActionCheck = Action("CheckShard")

	// ShardActionSetServedTypes changes the ServedTypes inside a shard
	ShardActionSetServedTypes = Action("SetShardServedTypes")

	// ShardActionUpdateShard updates the Shard object (Cells, ...)
	ShardActionUpdateShard = Action("UpdateShard")

	//
	// Keyspace actions - require very high level locking for consistency.
	// These are just descriptive and used for locking / logging.
	//

	// KeyspaceActionRebuild rebuilds the keyspace serving graph
	KeyspaceActionRebuild = Action("RebuildKeyspace")

	// KeyspaceActionApplySchema applies a schema change on the keyspace
	KeyspaceActionApplySchema = Action("ApplySchemaKeyspace")

	// KeyspaceActionSetShardingInfo updates the sharding info
	KeyspaceActionSetShardingInfo = Action("SetKeyspaceShardingInfo")

	// KeyspaceActionMigrateServedTypes migrates ServedType from
	// one shard to another in a keyspace
	KeyspaceActionMigrateServedTypes = Action("MigrateServedTypes")

	// KeyspaceActionMigrateServedFrom migrates ServedFrom to
	// another keyspace
	KeyspaceActionMigrateServedFrom = Action("MigrateServedFrom")

	// KeyspaceActionSetServedFrom updates ServedFrom
	KeyspaceActionSetServedFrom = Action("SetKeyspaceServedFrom")

	// KeyspaceActionCreateShard protects shard creation within the keyspace
	KeyspaceActionCreateShard = Action("KeyspaceCreateShard")
)

// Lock describes a long-running lock on a keyspace or a shard.
// Note it cannot be JSON-deserialized, because of the interface{} variable,
// but we only serialize it for debugging / logging purposes.
type Lock struct {
	// Action and the following fields are set at construction time
	Action   Action
	Args     interface{}
	HostName string
	UserName string
	Time     string

	// Status is the current status of the Lock.
	Status string
}

// NewLock creates a new Lock.
func NewLock(action Action, args interface{}) *Lock {
	l := &Lock{
		Action:   action,
		Args:     args,
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
		return "", fmt.Errorf("cannot JSON-marshal node: %v", err)
	}
	return string(data), nil
}

// lockInfo is an individual info structure for a lock
type lockInfo struct {
	lockPath   string
	actionNode *Lock
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
//   * 'vtctl SetShardServedTypes' emergency operations
//   * 'vtctl SetShardTabletControl' emergency operations
//   * 'vtctl SourceShardAdd' and 'vtctl SourceShardDelete' emergency operations
// * keyspace-wide schema changes
func (l *Lock) LockKeyspace(ctx context.Context, ts Server, keyspace string) (context.Context, func(context.Context, *error), error) {
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
	lockPath, err := l.lockKeyspace(ctx, ts, keyspace)
	if err != nil {
		return nil, nil, err
	}

	// and update our structure
	i.info[keyspace] = &lockInfo{
		lockPath:   lockPath,
		actionNode: l,
	}
	return ctx, func(finalCtx context.Context, finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[keyspace]; !ok {
			*finalErr = fmt.Errorf("trying to unlock keyspace %v multiple times", keyspace)
			return
		}

		*finalErr = l.unlockKeyspace(finalCtx, ts, keyspace, lockPath, *finalErr)
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

	// find the individual entry
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
func (l *Lock) lockKeyspace(ctx context.Context, ts Server, keyspace string) (lockPath string, err error) {
	log.Infof("Locking keyspace %v for action %v", keyspace, l.Action)

	ctx, cancel := context.WithTimeout(ctx, *LockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.LockKeyspaceForAction")
	span.Annotate("action", l.Action)
	span.Annotate("keyspace", keyspace)
	defer span.Finish()

	j, err := l.ToJSON()
	if err != nil {
		return "", err
	}
	return ts.LockKeyspaceForAction(ctx, keyspace, j)
}

// unlockKeyspace unlocks a previously locked keyspace.
func (l *Lock) unlockKeyspace(ctx context.Context, ts Server, keyspace string, lockPath string, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, DefaultLockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UnlockKeyspaceForAction")
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
	j, err := l.ToJSON()
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
func (l *Lock) LockShard(ctx context.Context, ts Server, keyspace, shard string) (context.Context, func(context.Context, *error), error) {
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
	lockPath, err := l.lockShard(ctx, ts, keyspace, shard)
	if err != nil {
		return nil, nil, err
	}

	// and update our structure
	i.info[mapKey] = &lockInfo{
		lockPath:   lockPath,
		actionNode: l,
	}
	return ctx, func(finalCtx context.Context, finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[mapKey]; !ok {
			*finalErr = fmt.Errorf("trying to unlock shard %v/%v multiple times", keyspace, shard)
			return
		}

		*finalErr = l.unlockShard(finalCtx, ts, keyspace, shard, lockPath, *finalErr)
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

// lockShard will lock the shard in the topology server.
// UnlockShard should be called if this returns no error.
func (l *Lock) lockShard(ctx context.Context, ts Server, keyspace, shard string) (lockPath string, err error) {
	log.Infof("Locking shard %v/%v for action %v", keyspace, shard, l.Action)

	ctx, cancel := context.WithTimeout(ctx, *LockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.LockShardForAction")
	span.Annotate("action", l.Action)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	j, err := l.ToJSON()
	if err != nil {
		return "", err
	}
	return ts.LockShardForAction(ctx, keyspace, shard, j)
}

// unlockShard unlocks a previously locked shard.
func (l *Lock) unlockShard(ctx context.Context, ts Server, keyspace, shard string, lockPath string, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, DefaultLockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UnlockShardForAction")
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
	j, err := l.ToJSON()
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

//
// shard lock related structures and creation methods
//

// ReparentShardArgs is the payload for ReparentShard
type ReparentShardArgs struct {
	Operation        string
	MasterElectAlias *topodatapb.TabletAlias
}

// ReparentShardLock returns a Lock
func ReparentShardLock(operation string, masterElectAlias *topodatapb.TabletAlias) *Lock {
	return NewLock(ShardActionReparent, &ReparentShardArgs{
		Operation:        operation,
		MasterElectAlias: masterElectAlias,
	})
}

// ShardExternallyReparentedLock returns a Lock
func ShardExternallyReparentedLock(tabletAlias *topodatapb.TabletAlias) *Lock {
	return NewLock(ShardActionExternallyReparented, tabletAlias)
}

// CheckShardLock returns a Lock
func CheckShardLock() *Lock {
	return NewLock(ShardActionCheck, nil)
}

//
// keyspace lock related structures and creation methods
//

// SetShardServedTypesArgs is the payload for SetShardServedTypes
type SetShardServedTypesArgs struct {
	Cells      []string
	ServedType topodatapb.TabletType
}

// SetShardServedTypesLock returns a Lock
func SetShardServedTypesLock(cells []string, servedType topodatapb.TabletType) *Lock {
	return NewLock(ShardActionSetServedTypes, &SetShardServedTypesArgs{
		Cells:      cells,
		ServedType: servedType,
	})
}

// MigrateServedTypesArgs is the payload for MigrateServedTypes
type MigrateServedTypesArgs struct {
	ServedType topodatapb.TabletType
}

// MigrateServedTypesLock returns a Lock
func MigrateServedTypesLock(servedType topodatapb.TabletType) *Lock {
	return NewLock(KeyspaceActionMigrateServedTypes, &MigrateServedTypesArgs{
		ServedType: servedType,
	})
}

// UpdateShardLock returns a Lock
func UpdateShardLock() *Lock {
	return NewLock(ShardActionUpdateShard, nil)
}

// RebuildKeyspaceLock returns a Lock
func RebuildKeyspaceLock() *Lock {
	return NewLock(KeyspaceActionRebuild, nil)
}

// SetKeyspaceShardingInfoLock returns a Lock
func SetKeyspaceShardingInfoLock() *Lock {
	return NewLock(KeyspaceActionSetShardingInfo, nil)
}

// SetKeyspaceServedFromLock returns a Lock
func SetKeyspaceServedFromLock() *Lock {
	return NewLock(KeyspaceActionSetServedFrom, nil)
}

// ApplySchemaKeyspaceArgs is the payload for ApplySchemaKeyspace
type ApplySchemaKeyspaceArgs struct {
	Change string
}

// ApplySchemaKeyspaceLock returns a Lock
func ApplySchemaKeyspaceLock(change string) *Lock {
	return NewLock(KeyspaceActionApplySchema, &ApplySchemaKeyspaceArgs{
		Change: change,
	})
}

// MigrateServedFromArgs is the payload for MigrateServedFrom
type MigrateServedFromArgs struct {
	ServedType topodatapb.TabletType
}

// MigrateServedFromLock returns a Lock
func MigrateServedFromLock(servedType topodatapb.TabletType) *Lock {
	return NewLock(KeyspaceActionMigrateServedFrom, &MigrateServedFromArgs{
		ServedType: servedType,
	})
}

// KeyspaceCreateShardLock returns a Lock to use to lock a keyspace
// for shard creation
func KeyspaceCreateShardLock() *Lock {
	return NewLock(KeyspaceActionCreateShard, nil)
}
