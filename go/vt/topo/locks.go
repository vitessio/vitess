/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"encoding/json"
	"os"
	"os/user"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
)

// This file contains utility methods and definitions to lock resources using topology server.

var (
	// LockTimeout is the maximum duration for which a
	// shard / keyspace lock can be acquired for.
	LockTimeout = 45 * time.Second

	// RemoteOperationTimeout is used for operations where we have to
	// call out to another process.
	// Used for RPC calls (including topo server calls)
	RemoteOperationTimeout = 15 * time.Second
)

// How long named locks are kept in the topo server.
// This ensures that orphaned named locks are not kept around forever.
// This should never happen, but it provides a final safety net.
const NamedLockTTL = 24 * time.Hour

// Lock describes a long-running lock on a keyspace or a shard.
// It needs to be public as we JSON-serialize it.
type Lock struct {
	// Action and the following fields are set at construction time.
	Action   string
	HostName string
	UserName string
	Time     string
	Options  lockOptions

	// Status is the current status of the Lock.
	Status string
}

func init() {
	for _, cmd := range FlagBinaries {
		servenv.OnParseFor(cmd, registerTopoLockFlags)
	}
}

func registerTopoLockFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&RemoteOperationTimeout, "remote_operation_timeout", RemoteOperationTimeout, "time to wait for a remote operation")
	fs.DurationVar(&LockTimeout, "lock-timeout", LockTimeout, "Maximum time to wait when attempting to acquire a lock from the topo server")
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

// Support different lock types.
type LockType int

const (
	// Blocking is the default lock type when no other valid type
	// is specified.
	Blocking    LockType = iota
	NonBlocking          // Uses TryLock
	Named                // Uses LockName
)

func (lt LockType) String() string {
	switch lt {
	case NonBlocking:
		return "non blocking"
	case Named:
		return "named"
	default:
		return "blocking"
	}
}

// iTopoLock is the interface for knowing the resource that is being locked.
// It allows for better controlling nuances for different lock types and log messages.
type iTopoLock interface {
	Type() string
	ResourceName() string
	Path() string
}

// perform the topo lock operation
func (l *Lock) lock(ctx context.Context, ts *Server, lt iTopoLock, opts ...LockOption) (LockDescriptor, error) {
	for _, o := range opts {
		o.apply(&l.Options)
	}
	log.Infof("Locking %s %s for action %s with options: %+v", lt.Type(), lt.ResourceName(), l.Action, l.Options)

	ctx, cancel := context.WithTimeout(ctx, LockTimeout)
	defer cancel()
	span, ctx := trace.NewSpan(ctx, "TopoServer.Lock")
	span.Annotate("action", l.Action)
	span.Annotate("path", lt.Path())
	defer span.Finish()

	j, err := l.ToJSON()
	if err != nil {
		return nil, err
	}

	switch l.Options.lockType {
	case NonBlocking:
		return ts.globalCell.TryLock(ctx, lt.Path(), j)
	case Named:
		return ts.globalCell.LockName(ctx, lt.Path(), j)
	default:
		if l.Options.ttl != 0 {
			return ts.globalCell.LockWithTTL(ctx, lt.Path(), j, l.Options.ttl)
		}
		return ts.globalCell.Lock(ctx, lt.Path(), j)
	}
}

// unlock unlocks a previously locked key.
func (l *Lock) unlock(ctx context.Context, lt iTopoLock, lockDescriptor LockDescriptor, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent
	// context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, RemoteOperationTimeout)
	defer cancel()

	span, ctx := trace.NewSpan(ctx, "TopoServer.Unlock")
	span.Annotate("action", l.Action)
	span.Annotate("path", lt.Path())
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking %v %v for action %v with error %v", lt.Type(), lt.ResourceName(), l.Action, actionError)
		l.Status = "Error: " + actionError.Error()
	} else {
		log.Infof("Unlocking %v %v for successful action %v", lt.Type(), lt.ResourceName(), l.Action)
		l.Status = "Done"
	}
	return lockDescriptor.Unlock(ctx)
}

func (ts *Server) internalLock(ctx context.Context, lt iTopoLock, action string, opts ...LockOption) (context.Context, func(*error), error) {
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		i = &locksInfo{
			info: make(map[string]*lockInfo),
		}
		ctx = context.WithValue(ctx, locksKey, i)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	// check that we are not already locked
	if _, ok := i.info[lt.ResourceName()]; ok {
		return nil, nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "lock for %v %v is already held", lt.Type(), lt.ResourceName())
	}

	// lock it
	l := newLock(action)
	lockDescriptor, err := l.lock(ctx, ts, lt, opts...)
	if err != nil {
		return nil, nil, err
	}
	// and update our structure
	i.info[lt.ResourceName()] = &lockInfo{
		lockDescriptor: lockDescriptor,
		actionNode:     l,
	}
	return ctx, func(finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[lt.ResourceName()]; !ok {
			if *finalErr != nil {
				log.Errorf("trying to unlock %v %v multiple times", lt.Type(), lt.ResourceName())
			} else {
				*finalErr = vterrors.Errorf(vtrpc.Code_INTERNAL, "trying to unlock %v %v multiple times", lt.Type(), lt.ResourceName())
			}
			return
		}

		err := l.unlock(ctx, lt, lockDescriptor, *finalErr)
		// if we have an error, we log it, but we still want to delete the lock
		if *finalErr != nil {
			if err != nil {
				// both error are set, just log the unlock error
				log.Warningf("unlock %v %v failed: %v", lt.Type(), lt.ResourceName(), err)
			}
		} else {
			*finalErr = err
		}
		delete(i.info, lt.ResourceName())
	}, nil
}

// checkLocked checks that the given resource is locked.
func checkLocked(ctx context.Context, lt iTopoLock) error {
	// extract the locksInfo pointer
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "%v %v is not locked (no locksInfo)", lt.Type(), lt.ResourceName())
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// find the individual entry
	li, ok := i.info[lt.ResourceName()]
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "%v %v is not locked (no lockInfo in map)", lt.Type(), lt.ResourceName())
	}

	// Check the lock server implementation still holds the lock.
	return li.lockDescriptor.Check(ctx)
}

// lockOptions configure a Lock call. lockOptions are set by the LockOption
// values passed to the lock functions.
type lockOptions struct {
	lockType LockType
	ttl      time.Duration
}

// LockOption configures how we perform the locking operation.
type LockOption interface {
	apply(*lockOptions)
}

// funcLockOption wraps a function that modifies lockOptions into an
// implementation of the LockOption interface.
type funcLockOption struct {
	f func(*lockOptions)
}

func (flo *funcLockOption) apply(lo *lockOptions) {
	flo.f(lo)
}

func newFuncLockOption(f func(*lockOptions)) *funcLockOption {
	return &funcLockOption{
		f: f,
	}
}

// WithTTL allows you to specify how long the underlying topo server
// implementation should hold the lock before releasing it — even if the caller
// has not explicitly released it. This provides a way to override the global
// ttl values that are set via --topo_consul_lock_session_ttl and
// --topo_etcd_lease_ttl.
// Note: This option is ignored by the ZooKeeper implementation as it does not
// support TTLs.
func WithTTL(ttl time.Duration) LockOption {
	return newFuncLockOption(func(o *lockOptions) {
		o.ttl = ttl
	})
}

// WithType determines the type of lock we take. The options are defined
// by the LockType type.
func WithType(lt LockType) LockOption {
	return newFuncLockOption(func(o *lockOptions) {
		o.lockType = lt
	})
}
