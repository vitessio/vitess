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
	"fmt"
	"html/template"
	"sort"
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

	// TopoTemplate is the HTML to use to display the
	// ResilientServerCacheStatus object
	TopoTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.2rem;
  }
</style>
<table>
  <tr>
    <th colspan="4">SrvKeyspace Names Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>SrvKeyspace Names</th>
    <th>TTL</th>
    <th>Error</th>
  </tr>
  {{range $i, $skn := .SrvKeyspaceNames}}
  <tr>
    <td>{{github_com_vitessio_vitess_vtctld_srv_cell $skn.Cell}}</td>
    <td>{{range $j, $value := $skn.Value}}{{github_com_vitessio_vitess_vtctld_srv_keyspace $skn.Cell $value}}&nbsp;{{end}}</td>
    <td>{{github_com_vitessio_vitess_srvtopo_ttl_time $skn.ExpirationTime}}</td>
    <td>{{if $skn.LastError}}({{github_com_vitessio_vitess_srvtopo_time_since $skn.LastQueryTime}}Ago) {{$skn.LastError}}{{end}}</td>
  </tr>
  {{end}}
</table>
<br>
<table>
  <tr>
    <th colspan="5">SrvKeyspace Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>Keyspace</th>
    <th>SrvKeyspace</th>
    <th>TTL</th>
    <th>Error</th>
  </tr>
  {{range $i, $sk := .SrvKeyspaces}}
  <tr>
    <td>{{github_com_vitessio_vitess_vtctld_srv_cell $sk.Cell}}</td>
    <td>{{github_com_vitessio_vitess_vtctld_srv_keyspace $sk.Cell $sk.Keyspace}}</td>
    <td>{{$sk.StatusAsHTML}}</td>
    <td>{{github_com_vitessio_vitess_srvtopo_ttl_time $sk.ExpirationTime}}</td>
    <td>{{if $sk.LastError}}({{github_com_vitessio_vitess_srvtopo_time_since $sk.LastErrorTime}} Ago) {{$sk.LastError}}{{end}}</td>
  </tr>
  {{end}}
</table>
`
)

// ResilientServer is an implementation of srvtopo.Server based
// on a topo.Server that uses a cache for two purposes:
// - limit the QPS to the underlying topo.Server
// - return the last known value of the data if there is an error
type ResilientServer struct {
	topoServer   *topo.Server
	cacheTTL     time.Duration
	cacheRefresh time.Duration
	counts       *stats.CountersWithSingleLabel

	// mutex protects the cache map itself, not the individual
	// values in the cache.
	mutex                 sync.RWMutex
	srvKeyspaceNamesCache map[string]*srvKeyspaceNamesEntry

	srvKeyspaceWatcher *SrvKeyspaceWatcher
	srvVSchemaWatcher  *SrvVSchemaWatcher
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

type watchState int

const (
	watchStateIdle watchState = iota
	watchStateStarting
	watchStateRunning
)

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
		topoServer:   base,
		cacheTTL:     *srvTopoCacheTTL,
		cacheRefresh: *srvTopoCacheRefresh,
		counts:       counts,

		srvKeyspaceNamesCache: make(map[string]*srvKeyspaceNamesEntry),
		srvKeyspaceWatcher:    NewSrvKeyspaceWatcher(base, counts, *srvTopoCacheRefresh, *srvTopoCacheTTL),
		srvVSchemaWatcher:     NewSrvVSchemaWatcher(base, counts, *srvTopoCacheRefresh, *srvTopoCacheTTL),
	}
}

// GetTopoServer returns the topo.Server that backs the resilient server.
func (server *ResilientServer) GetTopoServer() (*topo.Server, error) {
	return server.topoServer, nil
}

// GetSrvKeyspaceNames returns all keyspace names for the given cell.
func (server *ResilientServer) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	server.counts.Add(queryCategory, 1)

	// find the entry in the cache, add it if not there
	key := cell
	server.mutex.Lock()
	entry, ok := server.srvKeyspaceNamesCache[key]
	if !ok {
		entry = &srvKeyspaceNamesEntry{
			cell: cell,
		}
		server.srvKeyspaceNamesCache[key] = entry
	}
	server.mutex.Unlock()

	// Lock the entry, and do everything holding the lock except
	// querying the underlying topo server.
	//
	// This means that even if the topo server is very slow, two concurrent
	// requests will only issue one underlying query.
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	cacheValid := entry.value != nil && (time.Since(entry.insertionTime) < server.cacheTTL)
	if !cacheValid && staleOK {
		// Only allow stale results for a bounded period
		cacheValid = entry.value != nil && (time.Since(entry.insertionTime) < (server.cacheTTL + 2*server.cacheRefresh))
	}
	shouldRefresh := time.Since(entry.lastQueryTime) > server.cacheRefresh

	// If it is not time to check again, then return either the cached
	// value or the cached error but don't ask topo again.
	if !shouldRefresh {
		if cacheValid {
			return entry.value, nil
		}
		return nil, entry.lastError
	}

	// Refresh the state in a background goroutine if no refresh is already
	// in progress none is already running. This way queries are not blocked
	// while the cache is still valid but past the refresh time, and avoids
	// calling out to the topo service while the lock is held.
	if entry.refreshingChan == nil {
		entry.refreshingChan = make(chan struct{})
		entry.lastQueryTime = time.Now()
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("GetSrvKeyspaceNames uncaught panic, cell :%v, err :%v)", cell, err)
				}
			}()
			newCtx, cancel := context.WithTimeout(ctx, *srvTopoTimeout)
			defer cancel()
			result, err := server.topoServer.GetSrvKeyspaceNames(newCtx, cell)
			entry.mutex.Lock()
			defer func() {
				close(entry.refreshingChan)
				entry.refreshingChan = nil
				entry.mutex.Unlock()
			}()

			if err == nil {
				// save the value we got and the current time in the cache
				entry.insertionTime = time.Now()
				// Avoid a tiny race if TTL == refresh time (the default)
				entry.lastQueryTime = entry.insertionTime
				entry.value = result
			} else {
				server.counts.Add(errorCategory, 1)
				if entry.insertionTime.IsZero() {
					log.Errorf("GetSrvKeyspaceNames(%v, %v) failed: %v (no cached value, caching and returning error)", ctx, cell, err)
				} else if newCtx.Err() == context.DeadlineExceeded {
					log.Errorf("GetSrvKeyspaceNames(%v, %v) failed: %v (request timeout), (keeping cached value: %v)", ctx, cell, err, entry.value)
				} else if entry.value != nil && time.Since(entry.insertionTime) < server.cacheTTL {
					server.counts.Add(cachedCategory, 1)
					log.Warningf("GetSrvKeyspaceNames(%v, %v) failed: %v (keeping cached value: %v)", ctx, cell, err, entry.value)
				} else {
					log.Errorf("GetSrvKeyspaceNames(%v, %v) failed: %v (cached value expired)", ctx, cell, err)
					entry.insertionTime = time.Time{}
					entry.value = nil
				}
			}

			entry.lastError = err
			entry.lastErrorCtx = newCtx
		}()
	}

	// If the cached entry is still valid then use it, otherwise wait
	// for the refresh attempt to complete to get a more up to date
	// response.
	//
	// In the onEventLocked that the topo service is slow or unresponsive either
	// on the initial fetch or if the cache TTL expires, then several
	// requests could be blocked on refreshingCond waiting for the response
	// to come back.
	if cacheValid {
		return entry.value, nil
	}

	refreshingChan := entry.refreshingChan
	entry.mutex.Unlock()
	select {
	case <-refreshingChan:
	case <-ctx.Done():
		entry.mutex.Lock()
		return nil, fmt.Errorf("timed out waiting for keyspace names")
	}
	entry.mutex.Lock()

	if entry.value != nil {
		return entry.value, nil
	}

	return nil, entry.lastError
}

// GetSrvKeyspace returns SrvKeyspace object for the given cell and keyspace.
func (server *ResilientServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return server.srvKeyspaceWatcher.Get(ctx, cell, keyspace)
}

// WatchSrvVSchema is part of the srvtopo.Server interface.
func (server *ResilientServer) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error)) {
	server.srvVSchemaWatcher.Watch(ctx, cell, callback)
}

// The next few structures and methods are used to get a displayable
// version of the cache in a status page.

// SrvKeyspaceNamesCacheStatus is the current value for SrvKeyspaceNames
type SrvKeyspaceNamesCacheStatus struct {
	Cell           string
	Value          []string
	ExpirationTime time.Time
	LastQueryTime  time.Time
	LastError      error
	LastErrorCtx   context.Context
}

// SrvKeyspaceNamesCacheStatusList is used for sorting
type SrvKeyspaceNamesCacheStatusList []*SrvKeyspaceNamesCacheStatus

// Len is part of sort.Interface
func (skncsl SrvKeyspaceNamesCacheStatusList) Len() int {
	return len(skncsl)
}

// Less is part of sort.Interface
func (skncsl SrvKeyspaceNamesCacheStatusList) Less(i, j int) bool {
	return skncsl[i].Cell < skncsl[j].Cell
}

// Swap is part of sort.Interface
func (skncsl SrvKeyspaceNamesCacheStatusList) Swap(i, j int) {
	skncsl[i], skncsl[j] = skncsl[j], skncsl[i]
}

// SrvKeyspaceCacheStatus is the current value for a SrvKeyspace object
type SrvKeyspaceCacheStatus struct {
	Cell           string
	Keyspace       string
	Value          *topodatapb.SrvKeyspace
	ExpirationTime time.Time
	LastErrorTime  time.Time
	LastError      error
	LastErrorCtx   context.Context
}

// StatusAsHTML returns an HTML version of our status.
// It works best if there is data in the cache.
func (st *SrvKeyspaceCacheStatus) StatusAsHTML() template.HTML {
	if st.Value == nil {
		return template.HTML("No Data")
	}

	result := "<b>Partitions:</b><br>"
	for _, keyspacePartition := range st.Value.Partitions {
		result += "&nbsp;<b>" + keyspacePartition.ServedType.String() + ":</b>"
		for _, shard := range keyspacePartition.ShardReferences {
			result += "&nbsp;" + shard.Name
		}
		result += "<br>"
	}

	if st.Value.ShardingColumnName != "" {
		result += "<b>ShardingColumnName:</b>&nbsp;" + st.Value.ShardingColumnName + "<br>"
		result += "<b>ShardingColumnType:</b>&nbsp;" + st.Value.ShardingColumnType.String() + "<br>"
	}

	if len(st.Value.ServedFrom) > 0 {
		result += "<b>ServedFrom:</b><br>"
		for _, sf := range st.Value.ServedFrom {
			result += "&nbsp;<b>" + sf.TabletType.String() + ":</b>&nbsp;" + sf.Keyspace + "<br>"
		}
	}

	return template.HTML(result)
}

// SrvKeyspaceCacheStatusList is used for sorting
type SrvKeyspaceCacheStatusList []*SrvKeyspaceCacheStatus

// Len is part of sort.Interface
func (skcsl SrvKeyspaceCacheStatusList) Len() int {
	return len(skcsl)
}

// Less is part of sort.Interface
func (skcsl SrvKeyspaceCacheStatusList) Less(i, j int) bool {
	return skcsl[i].Cell+"."+skcsl[i].Keyspace <
		skcsl[j].Cell+"."+skcsl[j].Keyspace
}

// Swap is part of sort.Interface
func (skcsl SrvKeyspaceCacheStatusList) Swap(i, j int) {
	skcsl[i], skcsl[j] = skcsl[j], skcsl[i]
}

// ResilientServerCacheStatus has the full status of the cache
type ResilientServerCacheStatus struct {
	SrvKeyspaceNames SrvKeyspaceNamesCacheStatusList
	SrvKeyspaces     SrvKeyspaceCacheStatusList
}

// CacheStatus returns a displayable version of the cache
func (server *ResilientServer) CacheStatus() *ResilientServerCacheStatus {
	result := &ResilientServerCacheStatus{}

	server.mutex.Lock()
	for _, entry := range server.srvKeyspaceNamesCache {
		entry.mutex.Lock()

		result.SrvKeyspaceNames = append(result.SrvKeyspaceNames, &SrvKeyspaceNamesCacheStatus{
			Cell:           entry.cell,
			Value:          entry.value,
			ExpirationTime: entry.insertionTime.Add(server.cacheTTL),
			LastQueryTime:  entry.lastQueryTime,
			LastError:      entry.lastError,
			LastErrorCtx:   entry.lastErrorCtx,
		})
		entry.mutex.Unlock()
	}
	server.mutex.Unlock()

	result.SrvKeyspaces = server.srvKeyspaceWatcher.CacheStatus()

	// do the sorting without the mutex
	sort.Sort(result.SrvKeyspaceNames)
	sort.Sort(result.SrvKeyspaces)

	return result
}

// Returns the ttl for the cached entry or "Expired" if it is in the past
func ttlTime(expirationTime time.Time) template.HTML {
	ttl := time.Until(expirationTime).Round(time.Second)
	if ttl < 0 {
		return template.HTML("<b>Expired</b>")
	}
	return template.HTML(ttl.String())
}

func timeSince(t time.Time) template.HTML {
	return template.HTML(time.Since(t).Round(time.Second).String())
}

// StatusFuncs is required for CacheStatus) to work properly.
// We don't register them inside servenv directly so we don't introduce
// a dependency here.
var StatusFuncs = template.FuncMap{
	"github_com_vitessio_vitess_srvtopo_ttl_time":   ttlTime,
	"github_com_vitessio_vitess_srvtopo_time_since": timeSince,
}
