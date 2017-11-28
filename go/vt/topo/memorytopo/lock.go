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
	mt      *MemoryTopo
	cell    string
	dirPath string
}

// Lock is part of the topo.Backend interface.
func (mt *MemoryTopo) Lock(ctx context.Context, cell, dirPath, contents string) (topo.LockDescriptor, error) {
	for {
		mt.mu.Lock()

		n := mt.nodeByPath(cell, dirPath)
		if n == nil {
			mt.mu.Unlock()
			return nil, topo.ErrNoNode
		}

		if l := n.lock; l != nil {
			// Someone else has the lock. Just wait for it.
			mt.mu.Unlock()
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
		mt.mu.Unlock()
		return &memoryTopoLockDescriptor{
			mt:      mt,
			cell:    cell,
			dirPath: dirPath,
		}, nil
	}
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *memoryTopoLockDescriptor) Unlock(ctx context.Context) error {
	return ld.mt.unlock(ctx, ld.cell, ld.dirPath)
}

func (mt *MemoryTopo) unlock(ctx context.Context, cell, dirPath string) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	n := mt.nodeByPath(cell, dirPath)
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
