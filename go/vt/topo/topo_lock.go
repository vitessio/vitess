/*
Copyright 2024 The Vitess Authors.

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
	"fmt"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// ITopoLock is the interface for a lock that can be used to lock a key in the topology server.
// The lock is associated with a context and can be unlocked by calling the returned function.
// Note that we don't need an Unlock method on the interface, as the Lock() function
// returns a function that can be used to unlock the lock.
type ITopoLock interface {
	Lock(ctx context.Context) (context.Context, func(*error), error)
}

type TopoLock struct {
	Path string // topo path to lock
	Name string // name, for logging purposes

	ts *Server
}

var _ ITopoLock = (*TopoLock)(nil)

func (ts *Server) NewTopoLock(path, name string) *TopoLock {
	return &TopoLock{
		ts:   ts,
		Path: path,
		Name: name,
	}
}

func (tl *TopoLock) String() string {
	return fmt.Sprintf("TopoLock{Path: %v, Name: %v}", tl.Path, tl.Name)
}

// perform the topo lock operation
func (l *Lock) lock(ctx context.Context, ts *Server, path string) (LockDescriptor, error) {
	ctx, cancel := context.WithTimeout(ctx, LockTimeout)
	defer cancel()
	span, ctx := trace.NewSpan(ctx, "TopoServer.Lock")
	span.Annotate("action", l.Action)
	span.Annotate("path", path)
	defer span.Finish()

	j, err := l.ToJSON()
	if err != nil {
		return nil, err
	}
	return ts.globalCell.Lock(ctx, path, j)
}

// unlock unlocks a previously locked key.
func (l *Lock) unlock(ctx context.Context, path string, lockDescriptor LockDescriptor, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent
	// context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, RemoteOperationTimeout)
	defer cancel()

	span, ctx := trace.NewSpan(ctx, "TopoServer.Unlock")
	span.Annotate("action", l.Action)
	span.Annotate("path", path)
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		l.Status = "Error: " + actionError.Error()
	} else {
		l.Status = "Done"
	}
	return lockDescriptor.Unlock(ctx)
}

// Lock adds lock information to the context, checks that the lock is not already held, and locks it.
// It returns a new context with the lock information and a function to unlock the lock.
func (tl TopoLock) Lock(ctx context.Context) (context.Context, func(*error), error) {
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
	if _, ok := i.info[tl.Path]; ok {
		return nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "lock for %v is already held", tl.Path)
	}

	// lock it
	l := newLock(fmt.Sprintf("lock for %s", tl.Name))
	lockDescriptor, err := l.lock(ctx, tl.ts, tl.Path)
	if err != nil {
		return nil, nil, err
	}
	// and update our structure
	i.info[tl.Path] = &lockInfo{
		lockDescriptor: lockDescriptor,
		actionNode:     l,
	}
	return ctx, func(finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[tl.Path]; !ok {
			if *finalErr != nil {
				log.Errorf("trying to unlock %v multiple times", tl.Path)
			} else {
				*finalErr = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "trying to unlock %v multiple times", tl.Path)
			}
			return
		}

		err := l.unlock(ctx, tl.Path, lockDescriptor, *finalErr)
		// if we have an error, we log it, but we still want to delete the lock
		if *finalErr != nil {
			if err != nil {
				// both error are set, just log the unlock error
				log.Errorf("unlock(%v) failed: %v", tl.Path, err)
			}
		} else {
			*finalErr = err
		}
		delete(i.info, tl.Path)
	}, nil
}

func CheckLocked(ctx context.Context, keyPath string) error {
	// extract the locksInfo pointer
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "%s is not locked (no locksInfo)", keyPath)
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// find the individual entry
	_, ok = i.info[keyPath]
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "%s is not locked (no lockInfo in map)", keyPath)
	}

	// and we're good for now.
	return nil
}
