// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"launchpad.net/gozk/zookeeper"
)

// Create a path and any pieces required, think mkdir -p.
// Intermediate znodes are always created empty.
func CreateRecursive(zconn Conn, zkPath, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	parts := strings.Split(zkPath, "/")
	if parts[1] != "zk" {
		return "", fmt.Errorf("zkutil: non zk path: %v", zkPath)
	}

	pathCreated, err = zconn.Create(zkPath, value, flags, aclv)
	if zookeeper.IsError(err, zookeeper.ZNONODE) {
		_, err = CreateRecursive(zconn, path.Dir(zkPath), "", flags, aclv)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			return "", err
		}
		pathCreated, err = zconn.Create(zkPath, value, flags, aclv)
	}
	return
}

func CreateOrUpdate(zconn Conn, zkPath, value string, flags int, aclv []zookeeper.ACL, recursive bool) (pathCreated string, err error) {
	if recursive {
		pathCreated, err = CreateRecursive(zconn, zkPath, value, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	} else {
		pathCreated, err = zconn.Create(zkPath, value, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	}
	if err != nil && zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		pathCreated = ""
		_, err = zconn.Set(zkPath, value, -1)
	}
	return
}

type pathItem struct {
	path string
	err  error
}

func ChildrenRecursive(zconn Conn, zkPath string) ([]string, error) {
	var err error
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	pathList := make([]string, 0, 32)
	children, _, err := zconn.Children(zkPath)
	if err != nil {
		return nil, err
	}

	for _, child := range children {
		wg.Add(1)
		go func(child string) {
			childPath := path.Join(zkPath, child)
			rChildren, zkErr := ChildrenRecursive(zconn, childPath)
			if zkErr != nil {
				// If other processes are deleting nodes, we need to ignore
				// the missing nodes.
				if !zookeeper.IsError(zkErr, zookeeper.ZNONODE) {
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

func DeleteRecursive(zconn Conn, zkPath string, version int) error {
	// version: -1 delete any version of the node at path - only applies to the top node
	err := zconn.Delete(zkPath, version)
	if err == nil {
		return nil
	}
	if !zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
		return err
	}
	// Remove the ability for other nodes to get created while we are trying to delete.
	// Otherwise, you can enter a race condition, or get starved out from deleting.
	err = zconn.SetACL(zkPath, zookeeper.WorldACL(zookeeper.PERM_ADMIN|zookeeper.PERM_DELETE|zookeeper.PERM_READ), version)
	if err != nil {
		return err
	}
	children, _, err := zconn.Children(zkPath)
	if err != nil {
		return err
	}
	for _, child := range children {
		err := DeleteRecursive(zconn, path.Join(zkPath, child), -1)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return fmt.Errorf("zkutil: recursive delete failed: %v", err)
		}
	}

	err = zconn.Delete(zkPath, version)
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
		err = fmt.Errorf("zkutil: nodes getting recreated underneath delete (app race condition): %v", zkPath)
	}
	return err
}

// The lexically lowest node is the lock holder - verify that this
// path holds the lock.  Call this queue-lock because the semantics are
// a hybrid.  Normal zookeeper locks make assumptions about sequential
// numbering that don't hold when the data in a lock is modified.
func ObtainQueueLock(zconn Conn, zkPath string, wait time.Duration) error {
	queueNode := path.Dir(zkPath)
	lockNode := path.Base(zkPath)

	timer := time.NewTimer(wait)
trylock:
	children, _, err := zconn.Children(queueNode)
	if err != nil {
		return fmt.Errorf("zkutil: trylock failed %v", err)
	}
	sort.Strings(children)
	if len(children) > 0 {
		if children[0] == lockNode {
			return nil
		}
		if wait > 0 {
			prevLock := ""
			for i := 1; i < len(children); i++ {
				if children[i] == lockNode {
					prevLock = children[i-1]
					break
				}
			}
			if prevLock == "" {
				return fmt.Errorf("zkutil: no previous queue node found: %v", zkPath)
			}

			zkPrevLock := path.Join(queueNode, prevLock)
			stat, watch, err := zconn.ExistsW(zkPrevLock)
			if err != nil {
				return fmt.Errorf("zkutil: unable to watch queued node %v %v", zkPrevLock, err)
			}
			if stat == nil {
				goto trylock
			}
			select {
			case <-timer.C:
				break
			case <-watch:
				// The precise event doesn't matter - try to read again regardless.
				goto trylock
			}
		}
		return fmt.Errorf("zkutil: obtaining lock timed out %v", zkPath)
	}
	return fmt.Errorf("zkutil: empty queue node: %v", queueNode)
}

// Close done when you want to exit cleanly.
func CreatePidNode(zconn Conn, zkPath string, done chan struct{}) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("zkutil: failed creating pid node %v: %v", zkPath, err)
	}
	data := fmt.Sprintf("host:%v\npid:%v\n", hostname, os.Getpid())

	// On the first try, assume the cluster is up and running, that will
	// help hunt down any config issues present at startup
	_, err = zconn.Create(zkPath, data, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			err = zconn.Delete(zkPath, -1)
		}
		if err != nil {
			return fmt.Errorf("zkutil: failed deleting pid node: %v: %v", zkPath, err)
		}
		_, err = zconn.Create(zkPath, data, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			return fmt.Errorf("zkutil: failed creating pid node: %v: %v", zkPath, err)
		}
	}

	go func() {
		for {
			select {
			case <-done:
				log.Printf("INFO: pid watcher stopped on done: %v", zkPath)
				return
			default:
			}
			_, _, watch, err := zconn.GetW(zkPath)
			if err != nil {
				log.Printf("WARNING: failed reading pid node: %v", err)
				if !zookeeper.IsError(err, zookeeper.ZNONODE) {
					time.Sleep(30 * time.Second)
					continue
				}
			} else {
				event := <-watch
				log.Printf("INFO: pid node event: %v", event)
				if event.Ok() {
					if event.Type == zookeeper.EVENT_DELETED {
						// Another process took over (most likely), but no sense in starting
						// a data race. Just stop watching.
						log.Printf("INFO: pid watcher stopped on delete: %v", zkPath)
						return
					}
					continue
				} else {
					time.Sleep(30 * time.Second)
					continue
				}
			}

			_, err = zconn.Create(zkPath, data, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
			if err != nil {
				log.Printf("WARNING: failed recreating pid node: %v: %v", zkPath, err)
				time.Sleep(30 * time.Second)
			} else {
				log.Printf("INFO: recreated pid node: %v", zkPath)
			}
		}
	}()

	return nil
}
