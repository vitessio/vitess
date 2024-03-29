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
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
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
	srvTopoTimeout      = 5 * time.Second
	srvTopoCacheTTL     = 1 * time.Second
	srvTopoCacheRefresh = 1 * time.Second
)

func registerFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&srvTopoTimeout, "srv_topo_timeout", srvTopoTimeout, "topo server timeout")
	fs.DurationVar(&srvTopoCacheTTL, "srv_topo_cache_ttl", srvTopoCacheTTL, "how long to use cached entries for topology")
	fs.DurationVar(&srvTopoCacheRefresh, "srv_topo_cache_refresh", srvTopoCacheRefresh, "how frequently to refresh the topology for cached entries")
}

func init() {
	servenv.OnParseFor("vtgate", registerFlags)
	servenv.OnParseFor("vttablet", registerFlags)
	servenv.OnParseFor("vtcombo", registerFlags)
}

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

	*SrvKeyspaceWatcher
	*SrvVSchemaWatcher
	*SrvKeyspaceNamesQuery
}

// NewResilientServer creates a new ResilientServer
// based on the provided topo.Server.
func NewResilientServer(ctx context.Context, base *topo.Server, counts *stats.CountersWithSingleLabel) *ResilientServer {
	if srvTopoCacheRefresh > srvTopoCacheTTL {
		log.Fatalf("srv_topo_cache_refresh must be less than or equal to srv_topo_cache_ttl")
	}

	return &ResilientServer{
		topoServer:            base,
		SrvKeyspaceWatcher:    NewSrvKeyspaceWatcher(ctx, base, counts, srvTopoCacheRefresh, srvTopoCacheTTL),
		SrvVSchemaWatcher:     NewSrvVSchemaWatcher(ctx, base, counts, srvTopoCacheRefresh, srvTopoCacheTTL),
		SrvKeyspaceNamesQuery: NewSrvKeyspaceNamesQuery(base, counts, srvTopoCacheRefresh, srvTopoCacheTTL),
	}
}

// GetTopoServer returns the topo.Server that backs the resilient server.
func (server *ResilientServer) GetTopoServer() (*topo.Server, error) {
	return server.topoServer, nil
}
