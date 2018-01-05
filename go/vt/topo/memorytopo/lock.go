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

package memorytopo

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// convertError converts a context error into a topo error.
func convertError(err error) error {
	switch err {
	case context.Canceled:
		return topo.ErrInterrupted
	case context.DeadlineExceeded:
		return topo.ErrTimeout
	}
	return err
}

// memoryTopoLockDescriptor implements topo.LockDescriptor.
type memoryTopoLockDescriptor struct {
	c       *Conn
	dirPath string
}

// Lock is part of the topo.Conn interface.
func (c *Conn) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	for {
		c.factory.mu.Lock()

		n := c.factory.nodeByPath(c.cell, dirPath)
		if n == nil {
			c.factory.mu.Unlock()
			return nil, topo.ErrNoNode
		}

		if l := n.lock; l != nil {
			// Someone else has the lock. Just wait for it.
			c.factory.mu.Unlock()
			select {
			case <-l:
				// Node was unlocked, try again to grab it.
				continue
			case <-ctx.Done():
				// Done waiting
				return nil, convertError(ctx.Err())
			}
		}

		// Noone has the lock, grab it.
		n.lock = make(chan struct{})
		n.lockContents = contents
		c.factory.mu.Unlock()
		return &memoryTopoLockDescriptor{
			c:       c,
			dirPath: dirPath,
		}, nil
	}
}

// Check is part of the topo.LockDescriptor interface.
// We can never lose a lock in this implementation.
func (ld *memoryTopoLockDescriptor) Check(ctx context.Context) error {
	return nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *memoryTopoLockDescriptor) Unlock(ctx context.Context) error {
	return ld.c.unlock(ctx, ld.dirPath)
}

func (c *Conn) unlock(ctx context.Context, dirPath string) error {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	n := c.factory.nodeByPath(c.cell, dirPath)
	if n == nil {
		return topo.ErrNoNode
	}
	if n.lock == nil {
		return fmt.Errorf("node %v is not locked", dirPath)
	}
	close(n.lock)
	n.lock = nil
	n.lockContents = ""
	return nil
}
