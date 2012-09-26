// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// cache for zkocc
package zkocc

import (
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/zk/zkocc/proto"
	"launchpad.net/gozk/zookeeper"
	"sync"
	"time"
)

// The cache is a map of entry. The mutex on the cache only protects the
// map itself, not individual entries. Each entry has a mutex too.
// Once an entry is added to the map, it is never removed.

// cache entry state
const (
	// NOT_ASKED means noone ever asked for this
	// (but someone asked for the other one, data or children)
	// this is the default value
	ENTRY_NOT_ASKED = iota

	// INVALIDATED means there is interest from the client,
	// but we know the value is wrong
	ENTRY_INVALIDATED

	// STALE means we have a value that's probably good,
	// but we're not 100% sure (because of a server disconnect
	// for instance)
	ENTRY_STALE

	// GOOD is a value we are confident in
	ENTRY_GOOD
)

type zkCacheEntry struct {
	mutex         sync.Mutex
	node          proto.ZkNode
	dataState     int
	dataTime      time.Time
	childrenState int
	childrenTime  time.Time
}

func (entry *zkCacheEntry) processEvent(watch <-chan zookeeper.Event) {
	for event := range watch {
		// mark the cache so we know what to do
		// note we ignore session events here, as they're handled
		// at the cell level
		switch event.Type {
		case zookeeper.EVENT_DELETED:
			entry.mutex.Lock()
			entry.dataState = ENTRY_NOT_ASKED
			entry.mutex.Unlock()
		case zookeeper.EVENT_CHANGED:
			// invalidate the data cache
			entry.mutex.Lock()
			entry.dataState = ENTRY_INVALIDATED
			entry.mutex.Unlock()
		case zookeeper.EVENT_CHILD:
			// invalidate the children cache
			entry.mutex.Lock()
			entry.childrenState = ENTRY_INVALIDATED
			entry.mutex.Unlock()
		}
	}
}

type ZkCache struct {
	Cache map[string]*zkCacheEntry
	mutex sync.Mutex
}

func newZkCache() *ZkCache {
	return &ZkCache{Cache: make(map[string]*zkCacheEntry)}
}

func (zkc *ZkCache) get(path string, reply *proto.ZkNode) (found, stale bool) {
	zkc.mutex.Lock()
	result, ok := zkc.Cache[path]
	zkc.mutex.Unlock()

	// not found
	if !ok {
		return false, false
	}

	result.mutex.Lock()
	defer result.mutex.Unlock()
	if result.dataState == ENTRY_STALE {
		*reply = result.node
		reply.Children = nil
		return true, true
	} else if result.dataState == ENTRY_GOOD {
		*reply = result.node
		reply.Children = nil
		return true, false
	}
	return false, false
}

func (zkc *ZkCache) children(path string, reply *proto.ZkNode) (found, stale bool) {
	zkc.mutex.Lock()
	result, ok := zkc.Cache[path]
	zkc.mutex.Unlock()

	// not found
	if !ok {
		return false, false
	}

	result.mutex.Lock()
	defer result.mutex.Unlock()
	if result.childrenState == ENTRY_STALE {
		*reply = result.node
		reply.Data = ""
		return true, true
	} else if result.childrenState == ENTRY_GOOD {
		*reply = result.node
		reply.Data = ""
		return true, false
	}
	return false, false
}

func (zkc *ZkCache) updateData(path, data string, stat *proto.ZkStat, watch <-chan zookeeper.Event) {
	zkc.mutex.Lock()
	existing, ok := zkc.Cache[path]
	if !ok {
		existing = &zkCacheEntry{node: proto.ZkNode{Path: path}}
		zkc.Cache[path] = existing
	}
	zkc.mutex.Unlock()

	existing.mutex.Lock()
	existing.dataState = ENTRY_GOOD
	existing.dataTime = time.Now()
	existing.node.Data = data
	existing.node.Stat = *stat
	existing.mutex.Unlock()

	go existing.processEvent(watch)
}

func (zkc *ZkCache) updateChildren(path string, children []string, stat *proto.ZkStat, watch <-chan zookeeper.Event) {
	zkc.mutex.Lock()
	existing, ok := zkc.Cache[path]
	if !ok {
		existing = &zkCacheEntry{node: proto.ZkNode{Path: path}}
		zkc.Cache[path] = existing
	}
	zkc.mutex.Unlock()

	existing.mutex.Lock()
	existing.childrenState = ENTRY_GOOD
	existing.childrenTime = time.Now()
	existing.node.Stat = *stat
	existing.node.Children = children
	existing.mutex.Unlock()

	go existing.processEvent(watch)
}

// marks the entire cache as needing a refresh
// all entries will be stale after this
func (zkc *ZkCache) markForRefresh() {
	zkc.mutex.Lock()
	defer zkc.mutex.Unlock()
	for _, entry := range zkc.Cache {
		entry.mutex.Lock()
		if entry.dataState == ENTRY_GOOD {
			entry.dataState = ENTRY_STALE
		}
		if entry.childrenState == ENTRY_GOOD {
			entry.childrenState = ENTRY_STALE
		}
		entry.mutex.Unlock()
	}
}

// return a few values that need to be refreshed
// FIXME(alainjobart) configure the refresh count
func (zkc *ZkCache) refreshSomeValues(zconn *zookeeper.Conn, maxToRefresh int) {
	// build a list of a few values we want to refresh
	refreshThreshold := time.Now().Add(-10 * time.Minute)

	dataEntries := make([]string, 0, maxToRefresh)
	childrenEntries := make([]string, 0, maxToRefresh)
	zkc.mutex.Lock()
	for _, entry := range zkc.Cache {
		entry.mutex.Lock()

		if (entry.dataState == ENTRY_GOOD && entry.dataTime.Before(refreshThreshold)) || entry.dataState == ENTRY_STALE || entry.dataState == ENTRY_INVALIDATED {
			dataEntries = append(dataEntries, entry.node.Path)
		}
		if (entry.childrenState == ENTRY_GOOD && entry.childrenTime.Before(refreshThreshold)) || entry.childrenState == ENTRY_STALE || entry.childrenState == ENTRY_INVALIDATED {
			childrenEntries = append(childrenEntries, entry.node.Path)
		}
		entry.mutex.Unlock()

		// check if we have enough work to do
		if len(dataEntries) == maxToRefresh || len(childrenEntries) == maxToRefresh {
			break
		}
	}
	zkc.mutex.Unlock()

	// now refresh the values
	for _, path := range dataEntries {
		data, stat, watch, err := zconn.GetW(path)
		if err == nil {
			zkStat := &proto.ZkStat{}
			zkStatFromZookeeperStat(stat, zkStat)
			zkc.updateData(path, data, zkStat, watch)
		} else if zookeeper.IsError(err, zookeeper.ZCLOSING) {
			// connection is closing, no point in asking for more
			relog.Warning("failed to refresh cache: %v (and stopping refresh)", err.Error())
			return
		} else {
			// individual failure
			relog.Warning("failed to refresh cache: %v", err.Error())
		}
	}

	for _, path := range childrenEntries {
		children, stat, watch, err := zconn.ChildrenW(path)
		if err == nil {
			zkStat := &proto.ZkStat{}
			zkStatFromZookeeperStat(stat, zkStat)
			zkc.updateChildren(path, children, zkStat, watch)
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
