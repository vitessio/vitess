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

package srvtopo

import (
	"context"
	"flag"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

var (
	// srvTopoCacheTTL and srvTopoCacheRefresh control the behavior of
	// the caching for both watched and unwatched values.
	//
	// For entries we don't watch (like the list of Keyspaces), we refresh
	// the cached list from the topo after srv_topo_cache_refresh elapses.
	// If the fetch fails, we hold onto the cached value until
	// srv_topo_cache_ttl elapses.
	//
	// For entries we watch (like the SrvKeyspace for a given cell), if
	// setting the watch fails, we will use the last known value until
	// srv_topo_cache_ttl elapses and we only try to re-establish the watch
	// once every srv_topo_cache_refresh interval.
	srvTopoTimeout      = flag.Duration("srv_topo_timeout", 5*time.Second, "topo server timeout")
	srvTopoCacheTTL     = flag.Duration("srv_topo_cache_ttl", 1*time.Second, "how long to use cached entries for topology")
	srvTopoCacheRefresh = flag.Duration("srv_topo_cache_refresh", 1*time.Second, "how frequently to refresh the topology for cached entries")
)

const (
	queryCategory  = "query"
	cachedCategory = "cached"
	errorCategory  = "error"
)

// ResilientServer is an implementation of srvtopo.Server based
// on a topo.Server that uses a cache for two purposes:
// - limit the QPS to the underlying topo.Server
// - return the last known value of the data if there is an error
type ResilientServer struct {
	topoServer *topo.Server
	counts     *stats.CountersWithSingleLabel

	srvKeyspaceWatcher    *SrvKeyspaceWatcher
	srvVSchemaWatcher     *SrvVSchemaWatcher
	srvKeyspaceNamesQuery *SrvKeyspaceNamesQuery
}

type srvKeyspaceNamesEntry struct {
	// unmutable values
	cell string

	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex

	// refreshingChan is used to synchronize requests and avoid hammering
	// the topo server
	refreshingChan chan struct{}

	insertionTime time.Time
	lastQueryTime time.Time
	value         []string
	lastError     error
	lastErrorCtx  context.Context
}

// NewResilientServer creates a new ResilientServer
// based on the provided topo.Server.
func NewResilientServer(base *topo.Server, counterPrefix string) *ResilientServer {
	if *srvTopoCacheRefresh > *srvTopoCacheTTL {
		log.Fatalf("srv_topo_cache_refresh must be less than or equal to srv_topo_cache_ttl")
	}

	var metric string
	if counterPrefix == "" {
		metric = counterPrefix + "Counts"
	} else {
		metric = ""
	}
	counts := stats.NewCountersWithSingleLabel(metric, "Resilient srvtopo server operations", "type")

	return &ResilientServer{
		topoServer:            base,
		counts:                counts,
		srvKeyspaceWatcher:    NewSrvKeyspaceWatcher(base, counts, *srvTopoCacheRefresh, *srvTopoCacheTTL),
		srvVSchemaWatcher:     NewSrvVSchemaWatcher(base, counts, *srvTopoCacheRefresh, *srvTopoCacheTTL),
		srvKeyspaceNamesQuery: NewSrvKeyspaceNamesQuery(base, counts, *srvTopoCacheRefresh, *srvTopoCacheTTL),
	}
}

// GetTopoServer returns the topo.Server that backs the resilient server.
func (server *ResilientServer) GetTopoServer() (*topo.Server, error) {
	return server.topoServer, nil
}

// GetSrvKeyspaceNames returns all keyspace names for the given cell.
func (server *ResilientServer) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	return server.srvKeyspaceNamesQuery.Get(ctx, cell, staleOK)
}

// GetSrvKeyspace returns SrvKeyspace object for the given cell and keyspace.
func (server *ResilientServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return server.srvKeyspaceWatcher.Get(ctx, cell, keyspace)
}

// WatchSrvVSchema is part of the srvtopo.Server interface.
func (server *ResilientServer) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error)) {
	server.srvVSchemaWatcher.Watch(ctx, cell, callback)
}
