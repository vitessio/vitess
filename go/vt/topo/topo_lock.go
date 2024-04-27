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
	"path"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type ITopoLock interface {
	Lock(ctx context.Context) (context.Context, func(*error), error)
	Unlock(ctx context.Context) error
	Check(ctx context.Context) error
}

type TopoLock struct {
	Root, Key, Action, Name string

	ts *Server
}

func (l *Lock) lock(ctx context.Context, ts *Server, root, key string) (LockDescriptor, error) {
	log.Infof("Locking %s/%s for action %v", root, key, l.Action)

	ctx, cancel := context.WithTimeout(ctx, getLockTimeout())
	defer cancel()
	span, ctx := trace.NewSpan(ctx, "TopoServer.LockKeyForAction")
	span.Annotate("action", l.Action)
	span.Annotate("path", root)
	span.Annotate("key", key)
	defer span.Finish()

	topoPath := path.Join(root, key)
	j, err := l.ToJSON()
	if err != nil {
		return nil, err
	}
	return ts.globalCell.Lock(ctx, topoPath, j)
}

// unlock unlocks a previously locked key.
func (l *Lock) unlock(ctx context.Context, ts *Server, root, key string, lockDescriptor LockDescriptor, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent
	// context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, RemoteOperationTimeout)
	defer cancel()

	span, ctx := trace.NewSpan(ctx, "TopoServer.UnlockKeyForAction")
	span.Annotate("action", l.Action)
	span.Annotate("key", key)
	span.Annotate("path", root)
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking keyspace %v for action %v with error %v", key, l.Action, actionError)
		l.Status = "Error: " + actionError.Error()
	} else {
		log.Infof("Unlocking keyspace %v for successful action %v", key, l.Action)
		l.Status = "Done"
	}
	return lockDescriptor.Unlock(ctx)
}

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
	if _, ok := i.info[tl.Key]; ok {
		return nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "lock for key %v is already held", tl.Key)
	}

	// lock it
	l := newLock(tl.Action)
	lockDescriptor, err := l.lock(ctx, tl.ts, tl.Root, tl.Key)
	if err != nil {
		return nil, nil, err
	}
	// and update our structure
	i.info[tl.Key] = &lockInfo{
		lockDescriptor: lockDescriptor,
		actionNode:     l,
	}
	return ctx, func(finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[tl.Key]; !ok {
			if *finalErr != nil {
				log.Errorf("trying to unlock key %v multiple times", tl.Key)
			} else {
				*finalErr = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "trying to unlock key %v multiple times", tl.Key)
			}
			return
		}

		err := l.unlock(ctx, tl.ts, tl.Root, tl.Key, lockDescriptor, *finalErr)
		if *finalErr != nil {
			if err != nil {
				// both error are set, just log the unlock error
				log.Errorf("unlock(%v) failed: %v", tl.Key, err)
			}
		} else {
			*finalErr = err
		}
		delete(i.info, tl.Key)
	}, nil
}

func (tl TopoLock) Unlock(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (tl TopoLock) Check(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

var _ ITopoLock = (*TopoLock)(nil)

func (ts *Server) NewTopoLock(root, key, action, name string) TopoLock {
	return TopoLock{
		ts:     ts,
		Root:   root,
		Key:    key,
		Action: action,
		Name:   name,
	}
}

func (ts *Server) GetGlobalConn() Conn {
	return ts.globalCell
}
