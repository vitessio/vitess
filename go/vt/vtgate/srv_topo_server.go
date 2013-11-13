// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"sync"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/topo"
)

// SrvTopoServer is a subset of topo.Server that only contains the serving
// graph read-only calls used by clients to resolve serving addresses.
type SrvTopoServer interface {
	GetSrvKeyspaceNames(cell string) ([]string, error)

	GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error)

	GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error)
}

// ResilientSrvTopoServer is an implementation of SrvTopoServer based
// on another SrvTopoServer that uses a cache to return the last known
// value of the data if there is an error.
type ResilientSrvTopoServer struct {
	Toposerv SrvTopoServer

	// mutex protects the cache
	mu                    sync.Mutex
	svrKeyspaceNamesCache map[string][]string
	srvKeyspaceCache      map[string]*topo.SrvKeyspace
	srvEndPointsCache     map[string]*topo.EndPoints
}

// NewResilientSrvTopoServer creates a new ResilientSrvTopoServer
// based on the provided SrvTopoServer.
func NewResilientSrvTopoServer(base SrvTopoServer) *ResilientSrvTopoServer {
	return &ResilientSrvTopoServer{
		Toposerv: base,
	}
}

func (server *ResilientSrvTopoServer) GetSrvKeyspaceNames(cell string) ([]string, error) {
	key := cell
	result, err := server.GetSrvKeyspaceNames(cell)
	if err != nil {
		server.mu.Lock()
		result = server.svrKeyspaceNamesCache[key]
		server.mu.Unlock()
		if result != nil {
			log.Warningf("GetSrvKeyspaceNames(%v) failed: %v (returning cached value)", cell, err)
			return result, nil
		} else {
			log.Warningf("GetSrvKeyspaceNames(%v) failed: %v (no cached value, returning error)", cell, err)
			return nil, err
		}
	}

	server.mu.Lock()
	server.svrKeyspaceNamesCache[key] = result
	server.mu.Unlock()
	return result, nil
}

func (server *ResilientSrvTopoServer) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	key := cell + ":" + keyspace
	result, err := server.GetSrvKeyspace(cell, keyspace)
	if err != nil {
		server.mu.Lock()
		result = server.srvKeyspaceCache[key]
		server.mu.Unlock()
		if result != nil {
			log.Warningf("GetSrvKeyspace(%v, %v) failed: %v (returning cached value)", cell, keyspace, err)
			return result, nil
		} else {
			log.Warningf("GetSrvKeyspace(%v, %v) failed: %v (no cached value, returning error)", cell, keyspace, err)
			return nil, err
		}
	}

	server.mu.Lock()
	server.srvKeyspaceCache[key] = result
	server.mu.Unlock()
	return result, nil
}

func (server *ResilientSrvTopoServer) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	key := cell + ":" + keyspace + ":" + shard + ":" + string(tabletType)
	result, err := server.GetEndPoints(cell, keyspace, shard, tabletType)
	if err != nil {
		server.mu.Lock()
		result = server.srvEndPointsCache[key]
		server.mu.Unlock()
		if result != nil {
			log.Warningf("GetEndPoints(%v, %v%, v, %v) failed: %v (returning cached value)", cell, keyspace, shard, tabletType, err)
			return result, nil
		} else {
			log.Warningf("GetEndPoints(%v, %v, %v, %v) failed: %v (no cached value, returning error)", cell, keyspace, shard, tabletType, err)
			return nil, err
		}
	}

	server.mu.Lock()
	server.srvEndPointsCache[key] = result
	server.mu.Unlock()
	return result, nil
}
