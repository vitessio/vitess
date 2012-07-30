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

/*
Create a path and any pieces required, think mkdir -p.
*/
func CreateRecursive(zconn Conn, zkPath, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	parts := strings.Split(zkPath, "/")
	if parts[1] != "zk" {
		return "", fmt.Errorf("non zk path: %v", zkPath)
	}
	if len(parts) > 2 {
		tmpPath := "/zk"
		for _, p := range parts[2 : len(parts)-1] {
			tmpPath = path.Join(tmpPath, p)
			_, err = zconn.Create(tmpPath, "", flags, aclv)
			if err != nil && err.(*zookeeper.Error).Code != zookeeper.ZNODEEXISTS {
				return "", err
			}
		}
	}
	return zconn.Create(zkPath, value, flags, aclv)
}

func CreateOrUpdate(zconn Conn, zkPath, value string, flags int, aclv []zookeeper.ACL, recursive bool) (pathCreated string, err error) {
	if recursive {
		pathCreated, err = CreateRecursive(zconn, zkPath, value, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	} else {
		pathCreated, err = zconn.Create(zkPath, value, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	}
	if err != nil && err.(*zookeeper.Error).Code == zookeeper.ZNODEEXISTS {
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
		mutex.Lock()
		pathList = append(pathList, child)
		mutex.Unlock()

		childPath := path.Join(zkPath, child)
		childCopy := child

		wg.Add(1)
		go func() {
			rChildren, zkErr := ChildrenRecursive(zconn, childPath)
			if zkErr != nil {
				mutex.Lock()
				err = zkErr
				mutex.Unlock()
			} else {
				mutex.Lock()
				for _, rChild := range rChildren {
					pathList = append(pathList, path.Join(childCopy, rChild))
				}
				mutex.Unlock()
			}
			wg.Done()
		}()
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
	if err.(*zookeeper.Error).Code != zookeeper.ZNOTEMPTY {
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
		if err != nil {
			return fmt.Errorf("recursive delete failed: %v", err)
		}
	}

	err = zconn.Delete(zkPath, version)
	if err != nil && err.(*zookeeper.Error).Code != zookeeper.ZNOTEMPTY {
		err = fmt.Errorf("nodes getting recreated underneath delete (app race condition): %v", zkPath)
	}
	return err
}

/*
The lexically lowest node is the lock holder - verify that this
path holds the lock.  Call this queue-lock because the semantics are
a hybrid.  Normal zookeeper locks make assumptions about sequential
numbering that don't hold when the data in a lock is modified.
*/
func ObtainQueueLock(zconn Conn, zkPath string, wait bool) (bool, error) {
	if wait {
		panic("unimplemented")
	}

	queueNode := path.Dir(zkPath)
	lockNode := path.Base(zkPath)

	children, _, err := zconn.Children(queueNode)
	if err != nil {
		return false, err
	}
	sort.Strings(children)
	if len(children) > 0 {
		return children[0] == lockNode, nil
	}

	return false, fmt.Errorf("empty queue node: %v", queueNode)
}


func CreatePidNode(zconn Conn, zkPath string) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed creating pid node %v: %v", zkPath, err )
	}
	data := fmt.Sprintf("host:%v\npid:%v\n", hostname, os.Getpid())

	// On the first try, assume the cluster is up and running, that will
	// help hunt down any config issues present at startup
	_, err = zconn.Create(zkPath, data, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if err.(*zookeeper.Error).Code == zookeeper.ZNODEEXISTS {
			err = zconn.Delete(zkPath, -1)
		}
		if err != nil {
			return fmt.Errorf("failed deleting pid node: %v: %v", zkPath, err)
		}
		_, err = zconn.Create(zkPath, data, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			return fmt.Errorf("failed creating pid node: %v: %v", zkPath, err)
		}
	}

	go func() {
		for {
			_, _, watch, err := zconn.GetW(zkPath)
			if err != nil {
				log.Printf("WARNING: failed reading pid node: %v: %v", zkPath, err)
			} else {
				event := <-watch
				log.Printf("INFO: pid node event %v: %v", zkPath, event)
				if event.Ok() {
					if event.Type == zookeeper.EVENT_DELETED {
						// Another process took over (most likely), but no sense in starting
						// a data race. Just stop watching.
						log.Printf("INFO: pid watcher stopped, pid node deleted %v: %v", zkPath)
						return
					}
					continue
				}
			}

			_, err = zconn.Create(zkPath, data, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
			if err != nil {
				if zookeeper.IsError(err, zookeeper.ZCLOSING) {
					return
				}
				log.Printf("WARNING: failed recreating pid node: %v: %v", zkPath, err)
				time.Sleep(30*time.Second)
			} else {
				log.Printf("INFO: recreated pid node: %v", zkPath)
			}
		}
	}()

	return nil
}
