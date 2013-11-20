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
	srvTopoCacheTTL = flag.Duration("-srv_topo_cache_ttl", 1*time.Second, "how long to use cached entries for topology")
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

	// mutex protects the cache
	mu                    sync.Mutex
	svrKeyspaceNamesCache map[string]svrKeyspaceNamesEntry
	srvKeyspaceCache      map[string]svrKeyspaceEntry
	endPointsCache        map[string]endPointsEntry
}

type svrKeyspaceNamesEntry struct {
	insertionTime time.Time
	value         []string
}

type svrKeyspaceEntry struct {
	insertionTime time.Time
	value         *topo.SrvKeyspace
}

type endPointsEntry struct {
	insertionTime time.Time
	value         *topo.EndPoints
}

// NewResilientSrvTopoServer creates a new ResilientSrvTopoServer
// based on the provided SrvTopoServer.
func NewResilientSrvTopoServer(base SrvTopoServer) *ResilientSrvTopoServer {
	return &ResilientSrvTopoServer{
		Toposerv:              base,
		svrKeyspaceNamesCache: make(map[string]svrKeyspaceNamesEntry),
		srvKeyspaceCache:      make(map[string]svrKeyspaceEntry),
		endPointsCache:        make(map[string]endPointsEntry),
	}
}

func (server *ResilientSrvTopoServer) GetSrvKeyspaceNames(cell string) ([]string, error) {
	// try the cache first
	key := cell
	server.mu.Lock()
	entry, ok := server.svrKeyspaceNamesCache[key]
	server.mu.Unlock()
	if ok && time.Now().Sub(entry.insertionTime) < *srvTopoCacheTTL {
		return entry.value, nil
	}

	// not in cache or too old, get the real value
	result, err := server.Toposerv.GetSrvKeyspaceNames(cell)
	if err != nil {
		if ok {
			log.Warningf("GetSrvKeyspaceNames(%v) failed: %v (returning cached value)", cell, err)
			return entry.value, nil
		} else {
			log.Warningf("GetSrvKeyspaceNames(%v) failed: %v (no cached value, returning error)", cell, err)
			return nil, err
		}
	}

	// save the last value in the cache
	server.mu.Lock()
	server.svrKeyspaceNamesCache[key] = svrKeyspaceNamesEntry{
		insertionTime: time.Now(),
		value:         result,
	}
	server.mu.Unlock()
	return result, nil
}

func (server *ResilientSrvTopoServer) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	// try the cache first
	key := cell + ":" + keyspace
	server.mu.Lock()
	entry, ok := server.srvKeyspaceCache[key]
	server.mu.Unlock()
	if ok && time.Now().Sub(entry.insertionTime) < *srvTopoCacheTTL {
		return entry.value, nil
	}

	// not in cache or too old, get the real value
	result, err := server.Toposerv.GetSrvKeyspace(cell, keyspace)
	if err != nil {
		if ok {
			log.Warningf("GetSrvKeyspace(%v, %v) failed: %v (returning cached value)", cell, keyspace, err)
			return entry.value, nil
		} else {
			log.Warningf("GetSrvKeyspace(%v, %v) failed: %v (no cached value, returning error)", cell, keyspace, err)
			return nil, err
		}
	}

	// save the last value in the cache
	server.mu.Lock()
	server.srvKeyspaceCache[key] = svrKeyspaceEntry{
		insertionTime: time.Now(),
		value:         result,
	}
	server.mu.Unlock()
	return result, nil
}

func (server *ResilientSrvTopoServer) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	// try the cache first
	key := cell + ":" + keyspace + ":" + shard + ":" + string(tabletType)
	server.mu.Lock()
	entry, ok := server.endPointsCache[key]
	server.mu.Unlock()
	if ok && time.Now().Sub(entry.insertionTime) < *srvTopoCacheTTL {
		return entry.value, nil
	}

	// not in cache or too old, get the real value
	result, err := server.Toposerv.GetEndPoints(cell, keyspace, shard, tabletType)
	if err != nil {
		if ok {
			log.Warningf("GetEndPoints(%v, %v%, v, %v) failed: %v (returning cached value)", cell, keyspace, shard, tabletType, err)
			return entry.value, nil
		} else {
			log.Warningf("GetEndPoints(%v, %v, %v, %v) failed: %v (no cached value, returning error)", cell, keyspace, shard, tabletType, err)
			return nil, err
		}
	}

	// save the last value in the cache
	server.mu.Lock()
	server.endPointsCache[key] = endPointsEntry{
		insertionTime: time.Now(),
		value:         result,
	}
	server.mu.Unlock()
	return result, nil
}
