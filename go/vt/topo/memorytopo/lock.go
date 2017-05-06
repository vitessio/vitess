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
	"path"

	log "github.com/golang/glog"
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

func (mt *MemoryTopo) lock(ctx context.Context, nodePath string, contents string) (string, error) {
	for {
		mt.mu.Lock()

		n := mt.nodeByPath(topo.GlobalCell, nodePath)
		if n == nil {
			mt.mu.Unlock()
			return "", topo.ErrNoNode
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
				return "", convertError(ctx.Err())
			}
		}

		// Noone has the lock, grab it.
		n.lock = make(chan struct{})
		n.lockContents = contents
		mt.mu.Unlock()
		return nodePath, nil
	}
}

func (mt *MemoryTopo) unlock(ctx context.Context, nodePath, actionPath string) error {
	if nodePath != actionPath {
		return fmt.Errorf("invalid actionPath %v was expecting %v", actionPath, nodePath)
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	n := mt.nodeByPath(topo.GlobalCell, nodePath)
	if n == nil {
		return topo.ErrNoNode
	}
	if n.lock == nil {
		return fmt.Errorf("node %v is not locked", nodePath)
	}
	close(n.lock)
	n.lock = nil
	n.lockContents = ""
	return nil
}

// LockKeyspaceForAction implements topo.Server.
func (mt *MemoryTopo) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	keyspacePath := path.Join(keyspacesPath, keyspace)
	return mt.lock(ctx, keyspacePath, contents)
}

// UnlockKeyspaceForAction implements topo.Server.
func (mt *MemoryTopo) UnlockKeyspaceForAction(ctx context.Context, keyspace, actionPath, results string) error {
	keyspacePath := path.Join(keyspacesPath, keyspace)
	log.Infof("results of %v: %v", actionPath, results)
	return mt.unlock(ctx, keyspacePath, actionPath)
}

// LockShardForAction implements topo.Server.
func (mt *MemoryTopo) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard)
	return mt.lock(ctx, shardPath, contents)
}

// UnlockShardForAction implements topo.Server.
func (mt *MemoryTopo) UnlockShardForAction(ctx context.Context, keyspace, shard, actionPath, results string) error {
	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard)
	log.Infof("results of %v: %v", actionPath, results)
	return mt.unlock(ctx, shardPath, actionPath)
}
