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

package consultopo

import (
	"context"
	"fmt"
	"path"

	"github.com/hashicorp/consul/api"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// consulLockDescriptor implements topo.LockDescriptor.
type consulLockDescriptor struct {
	s        *Server
	lockPath string
	lost     <-chan struct{}
}

// Lock is part of the topo.Conn interface.
func (s *Server) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// We list the directory first to make sure it exists.
	if _, err := s.ListDir(ctx, dirPath, false /*full*/); err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	return s.lock(ctx, dirPath, contents)
}

// TryLock is part of the topo.Conn interface.
func (s *Server) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// We list all the entries under dirPath
	entries, err := s.ListDir(ctx, dirPath, true)
	if err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	// If there is a file 'lock' in it then we can assume that someone else already has a lock.
	// Throw error in this case
	for _, e := range entries {
		if e.Name == locksFilename && e.Type == topo.TypeFile && e.Ephemeral {
			return nil, topo.NewError(topo.NodeExists, fmt.Sprintf("lock already exists at path %s", dirPath))
		}
	}

	// everything is good let's acquire the lock.
	return s.lock(ctx, dirPath, contents)
}

// Lock is part of the topo.Conn interface.
func (s *Server) lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	lockPath := path.Join(s.root, dirPath, locksFilename)

	lockOpts := &api.LockOptions{
		Key:   lockPath,
		Value: []byte(contents),
		SessionOpts: &api.SessionEntry{
			Name: api.DefaultLockSessionName,
			TTL:  api.DefaultLockSessionTTL,
		},
	}
	lockOpts.SessionOpts.Checks = s.lockChecks
	if s.lockDelay > 0 {
		lockOpts.SessionOpts.LockDelay = s.lockDelay
	}
	if s.lockTTL != "" {
		lockOpts.SessionOpts.TTL = s.lockTTL
	}
	// Build the lock structure.
	l, err := s.client.LockOpts(lockOpts)
	if err != nil {
		return nil, err
	}

	// Wait until we are the only ones in this client trying to
	// lock that path.
	s.mu.Lock()
	li, ok := s.locks[lockPath]
	for ok {
		// Unlock, wait for something to change.
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, convertError(ctx.Err(), dirPath)
		case <-li.done:
		}

		// The original locker is gone, try to get it again
		s.mu.Lock()
		li, ok = s.locks[lockPath]
	}
	li = &lockInstance{
		lock: l,
		done: make(chan struct{}),
	}
	s.locks[lockPath] = li
	s.mu.Unlock()

	// We are the only ones trying to lock now.
	lost, err := l.Lock(ctx.Done())
	if err != nil || lost == nil {
		// Failed to lock, give up our slot in locks map.
		// Close the channel to unblock anyone else.
		s.mu.Lock()
		delete(s.locks, lockPath)
		s.mu.Unlock()
		close(li.done)
		// Consul will return empty leaderCh with nil error if we cannot get lock before the timeout
		// therefore we return a timeout error here
		if lost == nil {
			return nil, topo.NewError(topo.Timeout, lockPath)
		}
		return nil, err
	}

	// We got the lock, we're good.
	return &consulLockDescriptor{
		s:        s,
		lockPath: lockPath,
		lost:     lost,
	}, nil
}

// Check is part of the topo.LockDescriptor interface.
func (ld *consulLockDescriptor) Check(ctx context.Context) error {
	select {
	case <-ld.lost:
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "lost channel closed")
	default:
	}
	return nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *consulLockDescriptor) Unlock(ctx context.Context) error {
	return ld.s.unlock(ctx, ld.lockPath)
}

// unlock releases a lock acquired by Lock() on the given directory.
func (s *Server) unlock(ctx context.Context, lockPath string) error {
	s.mu.Lock()
	li, ok := s.locks[lockPath]
	s.mu.Unlock()
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "unlock: lock %v not held", lockPath)
	}

	// Try to unlock our lock. We will clean up our entry anyway.
	unlockErr := li.lock.Unlock()

	s.mu.Lock()
	delete(s.locks, lockPath)
	s.mu.Unlock()
	close(li.done)

	// Then try to remove the lock entirely. This will only work if
	// no one else has the lock.
	if err := li.lock.Destroy(); err != nil {
		// If someone else has the lock, we can't remove it,
		// but we don't need to.
		if err != api.ErrLockInUse {
			log.Warningf("failed to clean up lock file %v: %v", lockPath, err)
		}
	}

	return unlockErr
}
