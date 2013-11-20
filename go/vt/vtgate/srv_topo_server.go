// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/topo"
)

var (
	srvTopoCacheTTL = flag.Duration("srv_topo_cache_ttl", 1*time.Second, "how long to use cached entries for topology")
)

// SrvTopoServer is a subset of topo.Server that only contains the serving
// graph read-only calls used by clients to resolve serving addresses.
type SrvTopoServer interface {
	GetSrvKeyspaceNames(cell string) ([]string, error)

	GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error)

	GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error)
}

// ResilientSrvTopoServer is an implementation of SrvTopoServer based
// on another SrvTopoServer that uses a cache for two purposes:
// - limit the QPS to the underlying SrvTopoServer
// - return the last known value of the data if there is an error
type ResilientSrvTopoServer struct {
	Toposerv SrvTopoServer

	// mu protects the cache map itself, not the individual values
	// in the cache.
	mutex                 sync.Mutex
	srvKeyspaceNamesCache map[string]*srvKeyspaceNamesEntry
	srvKeyspaceCache      map[string]*srvKeyspaceEntry
	endPointsCache        map[string]*endPointsEntry
}

type srvKeyspaceNamesEntry struct {
	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex

	insertionTime time.Time
	value         []string
}

type srvKeyspaceEntry struct {
	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex

	insertionTime time.Time
	value         *topo.SrvKeyspace
}

type endPointsEntry struct {
	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex

	insertionTime time.Time
	value         *topo.EndPoints
}

// NewResilientSrvTopoServer creates a new ResilientSrvTopoServer
// based on the provided SrvTopoServer.
func NewResilientSrvTopoServer(base SrvTopoServer) *ResilientSrvTopoServer {
	return &ResilientSrvTopoServer{
		Toposerv:              base,
		srvKeyspaceNamesCache: make(map[string]*srvKeyspaceNamesEntry),
		srvKeyspaceCache:      make(map[string]*srvKeyspaceEntry),
		endPointsCache:        make(map[string]*endPointsEntry),
	}
}

func (server *ResilientSrvTopoServer) GetSrvKeyspaceNames(cell string) ([]string, error) {
	// find the entry in the cache, add it if not there
	key := cell
	server.mutex.Lock()
	entry, ok := server.srvKeyspaceNamesCache[key]
	if !ok {
		entry = &srvKeyspaceNamesEntry{}
		server.srvKeyspaceNamesCache[key] = entry
	}
	server.mutex.Unlock()

	// Lock the entry, and do everything holding the lock.  This
	// means two concurrent requests will only issue one
	// underlying query.
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	// If the entry is fresh enough, return it
	if time.Now().Sub(entry.insertionTime) < *srvTopoCacheTTL {
		return entry.value, nil
	}

	// not in cache or too old, get the real value
	result, err := server.Toposerv.GetSrvKeyspaceNames(cell)
	if err != nil {
		if entry.insertionTime.IsZero() {
			log.Errorf("GetSrvKeyspaceNames(%v) failed: %v (no cached value, returning error)", cell, err)
			return nil, err
		} else {
			log.Warningf("GetSrvKeyspaceNames(%v) failed: %v (returning cached value)", cell, err)
			return entry.value, nil
		}
	}

	// save the value we got and the current time in the cache
	entry.insertionTime = time.Now()
	entry.value = result
	return result, nil
}

func (server *ResilientSrvTopoServer) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	// find the entry in the cache, add it if not there
	key := cell + ":" + keyspace
	server.mutex.Lock()
	entry, ok := server.srvKeyspaceCache[key]
	if !ok {
		entry = &srvKeyspaceEntry{}
		server.srvKeyspaceCache[key] = entry
	}
	server.mutex.Unlock()

	// Lock the entry, and do everything holding the lock.  This
	// means two concurrent requests will only issue one
	// underlying query.
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	// If the entry is fresh enough, return it
	if time.Now().Sub(entry.insertionTime) < *srvTopoCacheTTL {
		return entry.value, nil
	}

	// not in cache or too old, get the real value
	result, err := server.Toposerv.GetSrvKeyspace(cell, keyspace)
	if err != nil {
		if entry.insertionTime.IsZero() {
			log.Errorf("GetSrvKeyspace(%v, %v) failed: %v (no cached value, returning error)", cell, keyspace, err)
			return nil, err
		} else {
			log.Warningf("GetSrvKeyspace(%v, %v) failed: %v (returning cached value)", cell, keyspace, err)
			return entry.value, nil
		}
	}

	// save the value we got and the current time in the cache
	entry.insertionTime = time.Now()
	entry.value = result
	return result, nil
}

func (server *ResilientSrvTopoServer) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	// find the entry in the cache, add it if not there
	key := cell + ":" + keyspace + ":" + shard + ":" + string(tabletType)
	server.mutex.Lock()
	entry, ok := server.endPointsCache[key]
	if !ok {
		entry = &endPointsEntry{}
		server.endPointsCache[key] = entry
	}
	server.mutex.Unlock()

	// Lock the entry, and do everything holding the lock.  This
	// means two concurrent requests will only issue one
	// underlying query.
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	// If the entry is fresh enough, return it
	if time.Now().Sub(entry.insertionTime) < *srvTopoCacheTTL {
		return entry.value, nil
	}

	// not in cache or too old, get the real value
	result, err := server.Toposerv.GetEndPoints(cell, keyspace, shard, tabletType)
	if err != nil {
		if entry.insertionTime.IsZero() {
			log.Errorf("GetEndPoints(%v, %v, %v, %v) failed: %v (no cached value, returning error)", cell, keyspace, shard, tabletType, err)
			return nil, err
		} else {
			log.Warningf("GetEndPoints(%v, %v%, v, %v) failed: %v (returning cached value)", cell, keyspace, shard, tabletType, err)
			return entry.value, nil
		}
	}

	// save the value we got and the current time in the cache
	entry.insertionTime = time.Now()
	entry.value = result
	return result, nil
}
