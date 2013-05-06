// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// cache for zkocc
package zkocc

import (
	"fmt"
	"sync"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// The cache is a map of entry. The mutex on the cache only protects the
// map itself, not individual entries. Each entry has a mutex too.
// Once an entry is added to the map, it is never removed.

// error used for stale entries
var (
	errStale = fmt.Errorf("Stale entry")
)

// The states for the structure are:
// xxxError == nil && xxxTime.IsZero()
//   we were never interested in this value, never asked, never failed
// xxxError == nil && !xxxTime.IsZero()
//   we asked for the value, and got it with no error
// xxxError != nil && xxxTime.IsZero()
//   the first time we asked for it, we got an error, and we never got
//   a good value after that.
// xxxError != nil && !xxxTime.IsZero()
//   entry is stale: we got a value a long time ago, and we then marked it
//   stale, and were never able to recover it.
type zkCacheEntry struct {
	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex
	node  zk.ZkNode // node has data, children and stat

	dataTime  time.Time // time we last got the data at
	dataError error

	childrenTime  time.Time // time we last got the children at
	childrenError error
}

func (entry *zkCacheEntry) processEvent(watch <-chan zookeeper.Event) {
	for event := range watch {
		// mark the cache so we know what to do
		// note we ignore session events here, as they're handled
		// at the cell level
		switch event.Type {
		case zookeeper.EVENT_DELETED:
			// invalidate both caches
			entry.mutex.Lock()
			entry.dataTime = time.Time{}
			entry.childrenTime = time.Time{}
			entry.mutex.Unlock()
		case zookeeper.EVENT_CHANGED:
			// invalidate the data cache
			entry.mutex.Lock()
			entry.dataTime = time.Time{}
			entry.mutex.Unlock()
		case zookeeper.EVENT_CHILD:
			// invalidate the children cache
			entry.mutex.Lock()
			entry.childrenTime = time.Time{}
			entry.mutex.Unlock()
		}
	}
}

func (entry *zkCacheEntry) get(zcell *zkCell, path string, reply *zk.ZkNode) error {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	cached := true
	if entry.dataTime.IsZero() {
		// the value is not set yet

		// if another process got an error getting it, and we
		// never got a value, we will let the background
		// refresh try to do better
		if entry.dataError != nil {
			return entry.dataError
		}

		// let's ask for it
		// first get the connection
		zconn, err := zcell.getConnection()
		if err != nil {
			entry.dataError = err
			relog.Warning("ZK connection error for path %v: %v", path, err)
			zcell.otherErrors.Add(1)
			return err
		}

		// get the value into the cache
		entry.node.Path = path
		var stat zk.Stat
		var watch <-chan zookeeper.Event
		entry.node.Data, stat, watch, err = zconn.GetW(path)
		if err != nil {
			entry.dataError = err
			relog.Warning("ZK error for path %v: %v", path, err)
			if zookeeper.IsError(err, zookeeper.ZNONODE) {
				zcell.nodeNotFoundErrors.Add(1)
			} else {
				zcell.otherErrors.Add(1)
			}
			return err
		}
		zcell.zkReads.Add(1)
		entry.node.Stat.FromZookeeperStat(stat)
		entry.dataTime = time.Now()

		// set up the update channel
		go entry.processEvent(watch)

		cached = false
	} else {
		// update the stats
		if entry.dataError != nil {
			// we have an error, so the entry is stale
			zcell.staleReads.Add(1)
		} else {
			zcell.cacheReads.Add(1)
		}
	}

	// we have a good value, return it
	*reply = entry.node
	reply.Children = nil
	reply.Cached = cached
	reply.Stale = entry.dataError != nil
	return nil
}

func (entry *zkCacheEntry) children(zcell *zkCell, path string, reply *zk.ZkNode) error {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	cached := true
	if entry.childrenTime.IsZero() {
		// the value is not set yet

		// if another process got an error getting it, and we
		// never got a value, we will let the background
		// refresh try to do better
		if entry.childrenError != nil {
			return entry.childrenError
		}

		// let's ask for it
		// first get the connection
		zconn, err := zcell.getConnection()
		if err != nil {
			entry.childrenError = err
			relog.Warning("ZK connection error for path %v: %v", path, err)
			zcell.otherErrors.Add(1)
			return err
		}

		// get the value into the cache
		entry.node.Path = path
		var stat zk.Stat
		var watch <-chan zookeeper.Event
		entry.node.Children, stat, watch, err = zconn.ChildrenW(path)
		if err != nil {
			entry.childrenError = err
			relog.Warning("ZK error for path %v: %v", path, err)
			if zookeeper.IsError(err, zookeeper.ZNONODE) {
				zcell.nodeNotFoundErrors.Add(1)
			} else {
				zcell.otherErrors.Add(1)
			}
			return err
		}
		zcell.zkReads.Add(1)
		entry.node.Stat.FromZookeeperStat(stat)
		entry.childrenTime = time.Now()

		// set up the update channel
		go entry.processEvent(watch)

		cached = false
	} else {
		// update the stats
		if entry.childrenError != nil {
			zcell.staleReads.Add(1)
		} else {
			zcell.cacheReads.Add(1)
		}
	}

	// we have a good value, return it
	*reply = entry.node
	reply.Data = ""
	reply.Cached = cached
	reply.Stale = entry.childrenError != nil
	return nil
}

func (entry *zkCacheEntry) checkForRefresh(refreshThreshold time.Time) (shouldBeDataRefreshed, shouldBeChildrenRefreshed bool) {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	if entry.dataTime.IsZero() {
		// the entry was never set
		// if we had an error getting it, it means we asked for it,
		// see if we can get a good value
		if entry.dataError != nil {
			if zookeeper.IsError(entry.dataError, zookeeper.ZNONODE) {
				// we had no node, we can try next time a client asks
				entry.dataError = nil
				// at this point, we have both
				// dataTime.IsZero() and
				// entry.dataError = nil, as if we
				// never asked for it
			} else {
				// we had a valid error, let's try again
				shouldBeDataRefreshed = true
			}
		}
	} else {
		// 1. we got a value at some point, then it got stale
		// 2. we got a value a long time ago, refresh it
		shouldBeDataRefreshed = entry.dataError != nil || entry.dataTime.Before(refreshThreshold)
	}

	if entry.childrenTime.IsZero() {
		// the entry was never set
		// if we had an error getting it, it means we asked for it,
		// see if we can get a good value
		if entry.childrenError != nil {
			if zookeeper.IsError(entry.childrenError, zookeeper.ZNONODE) {
				// we had no node, we can try next time a client asks
				entry.childrenError = nil
				// at this point, we have both
				// childrenTime.IsZero() and
				// entry.childrenError = nil, as if we
				// never asked for it
			} else {
				shouldBeChildrenRefreshed = true
				// we had a valid error, let's try again
			}
		}
	} else {
		// 1. we got a value at some point, then it got stale
		// 2. we got a value a long time ago, refresh it
		shouldBeChildrenRefreshed = entry.childrenError != nil || entry.childrenTime.Before(refreshThreshold)
	}
	return
}

func (entry *zkCacheEntry) markForRefresh() {
	entry.mutex.Lock()
	if !entry.dataTime.IsZero() && entry.dataError == nil {
		entry.dataError = errStale
	}
	if !entry.childrenTime.IsZero() && entry.childrenError == nil {
		entry.childrenError = errStale
	}
	entry.mutex.Unlock()
}

func (entry *zkCacheEntry) updateData(data string, stat *zk.ZkStat, watch <-chan zookeeper.Event) {
	entry.mutex.Lock()
	entry.dataTime = time.Now()
	entry.dataError = nil
	entry.node.Data = data
	entry.node.Stat = *stat
	entry.mutex.Unlock()

	go entry.processEvent(watch)
}

func (entry *zkCacheEntry) updateChildren(children []string, stat *zk.ZkStat, watch <-chan zookeeper.Event) {
	entry.mutex.Lock()
	entry.childrenTime = time.Now()
	entry.childrenError = nil
	entry.node.Stat = *stat
	entry.node.Children = children
	entry.mutex.Unlock()

	go entry.processEvent(watch)
}

// the ZkCache is a map from resolved zk path (where 'local' has been replaced
// with the cell name) to the entry
type ZkCache struct {
	Cache map[string]*zkCacheEntry
	mutex sync.Mutex
}

func newZkCache() *ZkCache {
	return &ZkCache{Cache: make(map[string]*zkCacheEntry)}
}

func (zkc *ZkCache) getEntry(path string) *zkCacheEntry {
	zkc.mutex.Lock()
	result, ok := zkc.Cache[path]
	if !ok {
		result = &zkCacheEntry{node: zk.ZkNode{Path: path}}
		zkc.Cache[path] = result
	}
	zkc.mutex.Unlock()
	return result
}

// marks the entire cache as needing a refresh
// all entries will be stale after this
func (zkc *ZkCache) markForRefresh() {
	zkc.mutex.Lock()
	defer zkc.mutex.Unlock()
	for _, entry := range zkc.Cache {
		entry.markForRefresh()
	}
}

// return a few values that need to be refreshed
func (zkc *ZkCache) refreshSomeValues(zconn zk.Conn, maxToRefresh int) {
	// build a list of a few values we want to refresh
	refreshThreshold := time.Now().Add(-10 * time.Minute)

	// range will randomize the traversal order, so we will not always try
	// the same entries in the same order
	dataEntries := make([]*zkCacheEntry, 0, maxToRefresh)
	childrenEntries := make([]*zkCacheEntry, 0, maxToRefresh)
	zkc.mutex.Lock()
	for _, entry := range zkc.Cache {
		shouldBeDataRefreshed, shouldBeChildrenRefreshed := entry.checkForRefresh(refreshThreshold)
		if shouldBeDataRefreshed {
			dataEntries = append(dataEntries, entry)
		}
		if shouldBeChildrenRefreshed {
			childrenEntries = append(childrenEntries, entry)
		}

		// check if we have enough work to do
		if len(dataEntries) == maxToRefresh || len(childrenEntries) == maxToRefresh {
			break
		}
	}
	zkc.mutex.Unlock()

	// now refresh the values
	for _, entry := range dataEntries {
		data, stat, watch, err := zconn.GetW(entry.node.Path)
		if err == nil {
			zkStat := &zk.ZkStat{}
			zkStat.FromZookeeperStat(stat)
			entry.updateData(data, zkStat, watch)
		} else if zookeeper.IsError(err, zookeeper.ZCLOSING) {
			// connection is closing, no point in asking for more
			relog.Warning("failed to refresh cache: %v (and stopping refresh)", err.Error())
			return
		} else {
			// individual failure
			relog.Warning("failed to refresh cache: %v", err.Error())
		}
	}

	for _, entry := range childrenEntries {
		children, stat, watch, err := zconn.ChildrenW(entry.node.Path)
		if err == nil {
			zkStat := &zk.ZkStat{}
			zkStat.FromZookeeperStat(stat)
			entry.updateChildren(children, zkStat, watch)
		} else if zookeeper.IsError(err, zookeeper.ZCLOSING) {
			// connection is closing, no point in asking for more
			relog.Warning("failed to refresh cache: %v (and stopping refresh)", err.Error())
			return
		} else {
			// individual failure
			relog.Warning("failed to refresh cache: %v", err.Error())
		}
	}
}
