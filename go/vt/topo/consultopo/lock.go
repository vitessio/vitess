/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consultopo

import (
	"fmt"
	"path"

	"github.com/hashicorp/consul/api"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// consulLockDescriptor implements topo.LockDescriptor.
type consulLockDescriptor struct {
	s        *Server
	lockPath string
}

// Lock is part of the topo.Backend interface.
func (s *Server) Lock(ctx context.Context, cell string, dirPath string) (topo.LockDescriptor, error) {
	// We list the directory first to make sure it exists.
	if _, err := s.ListDir(ctx, cell, dirPath); err != nil {
		if err == topo.ErrNoNode {
			return nil, err
		}
		return nil, fmt.Errorf("cannot ListDir(%v,%v) before locking", cell, dirPath)
	}

	lockPath, err := s.lock(ctx, s.global, path.Join(s.global.root, dirPath, locksFilename), "new lock")
	if err != nil {
		return nil, err
	}
	return &consulLockDescriptor{
		s:        s,
		lockPath: lockPath,
	}, nil
}

func (s *Server) lock(ctx context.Context, c *cellClient, lockPath, contents string) (string, error) {
	// Build the lock structure.
	l, err := s.global.client.LockOpts(&api.LockOptions{
		Key:   lockPath,
		Value: []byte(contents),
	})
	if err != nil {
		return "", err
	}

	// Wait until we are the only ones in this client trying to
	// lock that path.
	c.mu.Lock()
	li, ok := c.locks[lockPath]
	for ok {
		// Unlock, wait for something to change.
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", convertError(ctx.Err())
		case <-li.done:
		}

		// The original locker is gone, try to get it again
		c.mu.Lock()
		li, ok = c.locks[lockPath]
	}
	li = &lockInstance{
		lock: l,
		done: make(chan struct{}),
	}
	c.locks[lockPath] = li
	c.mu.Unlock()

	// We are the only ones trying to lock now.
	// FIXME(alainjobart) We don't look at the 'lost' channel
	// returned here. We need to fix this in our code base, to add
	// APIs to make sure a lock is still held.
	_, err = l.Lock(ctx.Done())
	if err != nil {
		// Failed to lock, give up our slot in locks map.
		// Close the channel to unblock anyone else.
		c.mu.Lock()
		delete(c.locks, lockPath)
		c.mu.Unlock()
		close(li.done)

		return "", err
	}

	// We got the lock, we're good.
	return lockPath, nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *consulLockDescriptor) Unlock(ctx context.Context) error {
	return ld.s.unlock(ctx, ld.s.global, ld.lockPath, ld.lockPath)
}

// unlock releases a lock acquired by lock() on the given directory.
// The string returned by lock() should be passed as the actionPath.
func (s *Server) unlock(ctx context.Context, c *cellClient, lockPath, actionPath string) error {
	// Sanity check.
	if lockPath != actionPath {
		return fmt.Errorf("unlock: actionPath doesn't match directory being unlocked: %q != %q", lockPath, actionPath)
	}

	c.mu.Lock()
	li, ok := c.locks[lockPath]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("unlock: lock %v not held", lockPath)
	}

	// Try to unlock our lock. We will clean up our entry anyway.
	unlockErr := li.lock.Unlock()

	c.mu.Lock()
	delete(c.locks, lockPath)
	c.mu.Unlock()
	close(li.done)

	return unlockErr
}
