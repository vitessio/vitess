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

package zk2topo

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/z-division/go-zookeeper/zk"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/fileutil"
)

// CreateRecursive is a helper function on top of Create. It will
// create a path and any pieces required, think mkdir -p.
// Intermediate znodes are always created empty.
// Pass maxCreationDepth=-1 to create all nodes to the top.
func CreateRecursive(ctx context.Context, conn *ZkConn, zkPath string, value []byte, flags int32, aclv []zk.ACL, maxCreationDepth int) (string, error) {
	pathCreated, err := conn.Create(ctx, zkPath, value, flags, aclv)
	if err == zk.ErrNoNode {
		if maxCreationDepth == 0 {
			return "", zk.ErrNoNode
		}

		// Make sure that nodes are either "file" or
		// "directory" to mirror file system semantics.
		dirAclv := make([]zk.ACL, len(aclv))
		for i, acl := range aclv {
			dirAclv[i] = acl
			dirAclv[i].Perms = PermDirectory
		}
		parentPath := path.Dir(zkPath)
		_, err = CreateRecursive(ctx, conn, parentPath, nil, 0, dirAclv, maxCreationDepth-1)
		if err != nil && err != zk.ErrNodeExists {
			return "", err
		}
		pathCreated, err = conn.Create(ctx, zkPath, value, flags, aclv)
	}
	return pathCreated, err
}

// ChildrenRecursive returns the relative path of all the children of
// the provided node.
func ChildrenRecursive(ctx context.Context, zconn *ZkConn, zkPath string) ([]string, error) {
	var err error
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	pathList := make([]string, 0, 32)
	children, _, err := zconn.Children(ctx, zkPath)
	if err != nil {
		return nil, err
	}

	for _, child := range children {
		wg.Add(1)
		go func(child string) {
			childPath := path.Join(zkPath, child)
			rChildren, zkErr := ChildrenRecursive(ctx, zconn, childPath)
			if zkErr != nil {
				// If other processes are deleting nodes, we need to ignore
				// the missing nodes.
				if zkErr != zk.ErrNoNode {
					mutex.Lock()
					err = zkErr
					mutex.Unlock()
				}
			} else {
				mutex.Lock()
				pathList = append(pathList, child)
				for _, rChild := range rChildren {
					pathList = append(pathList, path.Join(child, rChild))
				}
				mutex.Unlock()
			}
			wg.Done()
		}(child)
	}

	wg.Wait()

	mutex.Lock()
	defer mutex.Unlock()
	if err != nil {
		return nil, err
	}
	return pathList, nil
}

// ResolveWildcards resolves paths like:
// /zk/nyc/vt/tablets/*/action
// /zk/global/vt/keyspaces/*/shards/*/action
// /zk/*/vt/tablets/*/action
// into real existing paths
//
// If you send paths that don't contain any wildcard and
// don't exist, this function will return an empty array.
func ResolveWildcards(ctx context.Context, zconn *ZkConn, zkPaths []string) ([]string, error) {
	results := make([][]string, len(zkPaths))
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	var firstError error

	for i, zkPath := range zkPaths {
		wg.Add(1)
		parts := strings.Split(zkPath, "/")
		go func(i int) {
			defer wg.Done()
			subResult, err := resolveRecursive(ctx, zconn, parts, true)
			if err != nil {
				mu.Lock()
				if firstError != nil {
					log.Infof("Multiple error: %v", err)
				} else {
					firstError = err
				}
				mu.Unlock()
			} else {
				results[i] = subResult
			}
		}(i)
	}

	wg.Wait()
	if firstError != nil {
		return nil, firstError
	}

	result := make([]string, 0, 32)
	for i := 0; i < len(zkPaths); i++ {
		subResult := results[i]
		if subResult != nil {
			result = append(result, subResult...)
		}
	}

	return result, nil
}

func resolveRecursive(ctx context.Context, zconn *ZkConn, parts []string, toplevel bool) ([]string, error) {
	for i, part := range parts {
		if fileutil.HasWildcard(part) {
			var children []string
			var err error
			zkParentPath := strings.Join(parts[:i], "/")
			children, _, err = zconn.Children(ctx, zkParentPath)
			if err != nil {
				// we asked for something like
				// /zk/cell/aaa/* and
				// /zk/cell/aaa doesn't exist
				// -> return empty list, no error
				// (note we check both a regular zk
				// error and the error the test
				// produces)
				if err == zk.ErrNoNode {
					return nil, nil
				}
				// otherwise we return the error
				return nil, err
			}
			sort.Strings(children)

			results := make([][]string, len(children))
			wg := &sync.WaitGroup{}
			mu := &sync.Mutex{}
			var firstError error

			for j, child := range children {
				matched, err := path.Match(part, child)
				if err != nil {
					return nil, err
				}
				if matched {
					// we have a match!
					wg.Add(1)
					newParts := make([]string, len(parts))
					copy(newParts, parts)
					newParts[i] = child
					go func(j int) {
						defer wg.Done()
						subResult, err := resolveRecursive(ctx, zconn, newParts, false)
						if err != nil {
							mu.Lock()
							if firstError != nil {
								log.Infof("Multiple error: %v", err)
							} else {
								firstError = err
							}
							mu.Unlock()
						} else {
							results[j] = subResult
						}
					}(j)
				}
			}

			wg.Wait()
			if firstError != nil {
				return nil, firstError
			}

			result := make([]string, 0, 32)
			for j := 0; j < len(children); j++ {
				subResult := results[j]
				if subResult != nil {
					result = append(result, subResult...)
				}
			}

			// we found a part that is a wildcard, we
			// added the children already, we're done
			return result, nil
		}
	}

	// no part contains a wildcard, add the path if it exists, and done
	path := strings.Join(parts, "/")
	if toplevel {
		// for whatever the user typed at the toplevel, we don't
		// check it exists or not, we just return it
		return []string{path}, nil
	}

	// this is an expanded path, we need to check if it exists
	exists, _, err := zconn.Exists(ctx, path)
	if err != nil {
		return nil, err
	}
	if exists {
		return []string{path}, nil
	}
	return nil, nil
}

// DeleteRecursive will delete all children of the given path.
func DeleteRecursive(ctx context.Context, zconn *ZkConn, zkPath string, version int32) error {
	// version: -1 delete any version of the node at path - only applies to the top node
	err := zconn.Delete(ctx, zkPath, version)
	if err == nil {
		return nil
	}
	if err != zk.ErrNotEmpty {
		return err
	}
	// Remove the ability for other nodes to get created while we are trying to delete.
	// Otherwise, you can enter a race condition, or get starved out from deleting.
	err = zconn.SetACL(ctx, zkPath, zk.WorldACL(zk.PermAdmin|zk.PermDelete|zk.PermRead), version)
	if err != nil {
		return err
	}
	children, _, err := zconn.Children(ctx, zkPath)
	if err != nil {
		return err
	}
	for _, child := range children {
		err := DeleteRecursive(ctx, zconn, path.Join(zkPath, child), -1)
		if err != nil && err != zk.ErrNoNode {
			return vterrors.Wrapf(err, "DeleteRecursive: recursive delete failed")
		}
	}

	err = zconn.Delete(ctx, zkPath, version)
	if err != nil && err != zk.ErrNotEmpty {
		err = fmt.Errorf("DeleteRecursive: nodes getting recreated underneath delete (app race condition): %v", zkPath)
	}
	return err
}

// obtainQueueLock waits until we hold the lock in the provided path.
// The lexically lowest node is the lock holder - verify that this
// path holds the lock.  Call this queue-lock because the semantics are
// a hybrid.  Normal Zookeeper locks make assumptions about sequential
// numbering that don't hold when the data in a lock is modified.
func obtainQueueLock(ctx context.Context, conn *ZkConn, zkPath string) error {
	queueNode := path.Dir(zkPath)
	lockNode := path.Base(zkPath)

	for {
		// Get our siblings.
		children, _, err := conn.Children(ctx, queueNode)
		if err != nil {
			return vterrors.Wrap(err, "obtainQueueLock: trylock failed %v")
		}
		sort.Strings(children)
		if len(children) == 0 {
			return fmt.Errorf("obtainQueueLock: empty queue node: %v", queueNode)
		}

		// If we are the first node, we got the lock.
		if children[0] == lockNode {
			return nil
		}

		// If not, find the previous node.
		prevLock := ""
		for i := 1; i < len(children); i++ {
			if children[i] == lockNode {
				prevLock = children[i-1]
				break
			}
		}
		if prevLock == "" {
			return fmt.Errorf("obtainQueueLock: no previous queue node found: %v", zkPath)
		}

		// Set a watch on the previous node.
		zkPrevLock := path.Join(queueNode, prevLock)
		exists, _, watch, err := conn.ExistsW(ctx, zkPrevLock)
		if err != nil {
			return vterrors.Wrapf(err, "obtainQueueLock: unable to watch queued node %v", zkPrevLock)
		}
		if !exists {
			// The lock disappeared, try to read again.
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-watch:
			// Something happened to the previous lock.
			// It doesn't matter what, read again.
		}
	}
}
