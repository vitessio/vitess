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

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// lockType is the interface for knowing the resource that is being locked.
// It allows for better controlling nuances for different lock types and log messages.
type lockType interface {
	Type() string
	ResourceName() string
	Path() string
}

// perform the topo lock operation
func (l *Lock) lock(ctx context.Context, ts *Server, lt lockType, isBlocking bool) (LockDescriptor, error) {
	log.Infof("Locking %v %v for action %v", lt.Type(), lt.ResourceName(), l.Action)

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
	if isBlocking {
		return ts.globalCell.Lock(ctx, lt.Path(), j)
	}
	return ts.globalCell.TryLock(ctx, lt.Path(), j)
}

// unlock unlocks a previously locked key.
func (l *Lock) unlock(ctx context.Context, lt lockType, lockDescriptor LockDescriptor, actionError error) error {
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

func (ts *Server) internalLock(ctx context.Context, lt lockType, action string, isBlocking bool) (context.Context, func(*error), error) {
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
		return nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "lock for %v %v is already held", lt.Type(), lt.ResourceName())
	}

	// lock it
	l := newLock(action)
	lockDescriptor, err := l.lock(ctx, ts, lt, isBlocking)
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
				*finalErr = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "trying to unlock %v %v multiple times", lt.Type(), lt.ResourceName())
			}
			return
		}

		err := l.unlock(ctx, lt, lockDescriptor, *finalErr)
		// if we have an error, we log it, but we still want to delete the lock
		if *finalErr != nil {
			if err != nil {
				// both error are set, just log the unlock error
				log.Errorf("unlock %v %v failed: %v", lt.Type(), lt.ResourceName(), err)
			}
		} else {
			*finalErr = err
		}
		delete(i.info, lt.ResourceName())
	}, nil
}

func checkLocked(ctx context.Context, lt lockType) error {
	// extract the locksInfo pointer
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "%v %v is not locked (no locksInfo)", lt.Type(), lt.ResourceName())
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// find the individual entry
	li, ok := i.info[lt.ResourceName()]
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "%v %v is not locked (no lockInfo in map)", lt.Type(), lt.ResourceName())
	}

	// Check the lock server implementation still holds the lock.
	return li.lockDescriptor.Check(ctx)
}
