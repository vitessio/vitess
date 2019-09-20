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
	"flag"
	"fmt"
	"html/template"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"

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
	srvKeyspaceCache      map[string]*srvKeyspaceEntry
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

type srvKeyspaceEntry struct {
	// unmutable values
	cell     string
	keyspace string

	// the mutex protects any access to this structure (read or write)
	mutex sync.RWMutex

	// watchState describes if the watch go routine is running.
	// It is easier to have an explicit field instead of guessing
	// based on value and lastError.
	//
	// if the state is watchStateIdle, and the time since the last error is
	// greater than the refresh time, the next time we try to access the
	// keyspace, we will set watchState to watchStarting and kick off the
	// watch in a separate goroutine
	//
	// in watchStateRunning, we are guaranteed to have lastError be
	// non-nil and an up-to-date value (which may be nil)
	watchState watchState

	// watchStartingCond is used to serialize callers for the first attempt
	// to establish the watch
	watchStartingChan chan struct{}

	value     *topodatapb.SrvKeyspace
	lastError error

	// lastValueTime is the time when the cached value is known to be valid,
	// either because the watch last obtained a non-nil value or when a
	// running watch first got an error.
	//
	// It is compared to the TTL to determine if we can return the value
	// when the watch is failing
	lastValueTime time.Time

	// lastErrorCtx tries to remember the context of the query
	// that failed to get the SrvKeyspace, so we can display it in
	// the status UI. The background routine that refreshes the
	// keyspace will not populate this field.
	// The intent is to have the source of a query that for instance
	// has a bad keyspace or cell name.
	lastErrorCtx context.Context

	// lastErrorTime records the time that the watch failed, used for
	// the status page
	lastErrorTime time.Time
}

// NewResilientServer creates a new ResilientServer
// based on the provided topo.Server.
func NewResilientServer(base *topo.Server, counterPrefix string) *ResilientServer {
	if *srvTopoCacheRefresh > *srvTopoCacheTTL {
		log.Fatalf("srv_topo_cache_refresh must be less than or equal to srv_topo_cache_ttl")
	}

	return &ResilientServer{
		topoServer:   base,
		cacheTTL:     *srvTopoCacheTTL,
		cacheRefresh: *srvTopoCacheRefresh,
		counts:       stats.NewCountersWithSingleLabel(counterPrefix+"Counts", "Resilient srvtopo server operations", "type"),

		srvKeyspaceNamesCache: make(map[string]*srvKeyspaceNamesEntry),
		srvKeyspaceCache:      make(map[string]*srvKeyspaceEntry),
	}
}

// GetTopoServer returns the topo.Server that backs the resilient server.
func (server *ResilientServer) GetTopoServer() (*topo.Server, error) {
	return server.topoServer, nil
}

// GetSrvKeyspaceNames returns all keyspace names for the given cell.
func (server *ResilientServer) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
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

	cacheValid := entry.value != nil && time.Since(entry.insertionTime) < server.cacheTTL
	shouldRefresh := time.Since(entry.lastQueryTime) > server.cacheRefresh

	// If it is not time to check again, then return either the cached
	// value or the cached error but don't ask consul again.
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
			result, err := server.topoServer.GetSrvKeyspaceNames(ctx, cell)

			entry.mutex.Lock()
			defer func() {
				close(entry.refreshingChan)
				entry.refreshingChan = nil
				entry.mutex.Unlock()
			}()

			if err == nil {
				// save the value we got and the current time in the cache
				entry.insertionTime = time.Now()
				entry.value = result
			} else {
				server.counts.Add(errorCategory, 1)
				if entry.insertionTime.IsZero() {
					log.Errorf("GetSrvKeyspaceNames(%v, %v) failed: %v (no cached value, caching and returning error)", ctx, cell, err)

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
			entry.lastErrorCtx = ctx
		}()
	}

	// If the cached entry is still valid then use it, otherwise wait
	// for the refresh attempt to complete to get a more up to date
	// response.
	//
	// In the event that the topo service is slow or unresponsive either
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

func (server *ResilientServer) getSrvKeyspaceEntry(cell, keyspace string) *srvKeyspaceEntry {
	// find the entry in the cache, add it if not there
	key := cell + "." + keyspace
	server.mutex.RLock()
	entry, ok := server.srvKeyspaceCache[key]
	if ok {
		server.mutex.RUnlock()
		return entry
	}
	server.mutex.RUnlock()

	server.mutex.Lock()
	entry, ok = server.srvKeyspaceCache[key]
	if !ok {
		entry = &srvKeyspaceEntry{
			cell:     cell,
			keyspace: keyspace,
		}
		server.srvKeyspaceCache[key] = entry
	}
	server.mutex.Unlock()
	return entry
}

// GetSrvKeyspace returns SrvKeyspace object for the given cell and keyspace.
func (server *ResilientServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	entry := server.getSrvKeyspaceEntry(cell, keyspace)

	// If the watch is already running, return the value
	entry.mutex.RLock()
	if entry.watchState == watchStateRunning {
		v, e := entry.value, entry.lastError
		entry.mutex.RUnlock()
		return v, e
	}
	entry.mutex.RUnlock()

	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	// If the watch is already running (now that we have the write lock),
	// return the value
	if entry.watchState == watchStateRunning {
		return entry.value, entry.lastError
	}

	// Watch is not running. Start a new one if it is time to use it and if
	// there isn't one already one  in the process of being started.
	shouldRefresh := time.Since(entry.lastErrorTime) > server.cacheRefresh
	if shouldRefresh && (entry.watchState == watchStateIdle) {
		entry.watchState = watchStateStarting
		entry.watchStartingChan = make(chan struct{})
		go server.watchSrvKeyspace(ctx, entry, cell, keyspace)
	}

	// If the cached value is still valid, use it. Otherwise wait
	// for the watch attempt to complete to get a more up to date
	// response.
	//
	// In the event that the topo service is slow or unresponsive either
	// on the initial fetch or if the cache TTL expires, then several
	// requests could be blocked waiting for the response to come back.
	cacheValid := entry.value != nil && time.Since(entry.lastValueTime) < server.cacheTTL
	if cacheValid {
		server.counts.Add(cachedCategory, 1)
		return entry.value, nil
	}

	if entry.watchState == watchStateStarting {
		watchStartingChan := entry.watchStartingChan
		entry.mutex.Unlock()
		select {
		case <-watchStartingChan:
		case <-ctx.Done():
			entry.mutex.Lock()
			return nil, fmt.Errorf("timed out waiting for keyspace")
		}
		entry.mutex.Lock()
	}

	if entry.value != nil {
		return entry.value, nil
	}

	return nil, entry.lastError
}

// watchSrvKeyspace is started in a separate goroutine and attempts to establish
// a watch. The caller context is provided to show in the UI in case the watch
// fails due to an error like a mistyped keyspace.
func (server *ResilientServer) watchSrvKeyspace(callerCtx context.Context, entry *srvKeyspaceEntry, cell, keyspace string) {
	// We use a background context, as starting the watch should keep going
	// even if the current query context is short-lived.
	newCtx := context.Background()
	current, changes, cancel := server.topoServer.WatchSrvKeyspace(newCtx, cell, keyspace)

	entry.mutex.Lock()

	if current.Err != nil {
		// lastError and lastErrorCtx will be visible from the UI
		// until the next try
		entry.lastError = current.Err
		entry.lastErrorCtx = callerCtx
		entry.lastErrorTime = time.Now()

		// if the node disappears, delete the cached value
		if topo.IsErrType(current.Err, topo.NoNode) {
			entry.value = nil
		}

		server.counts.Add(errorCategory, 1)
		log.Errorf("Initial WatchSrvKeyspace failed for %v/%v: %v", cell, keyspace, current.Err)

		if time.Since(entry.lastValueTime) > server.cacheTTL {
			log.Errorf("WatchSrvKeyspace clearing cached entry for %v/%v", cell, keyspace)
			entry.value = nil
		}

		entry.watchState = watchStateIdle
		close(entry.watchStartingChan)
		entry.watchStartingChan = nil
		entry.mutex.Unlock()
		return
	}

	// we are now watching, cache the first notification
	entry.watchState = watchStateRunning
	close(entry.watchStartingChan)
	entry.watchStartingChan = nil
	entry.value = current.Value
	entry.lastValueTime = time.Now()

	entry.lastError = nil
	entry.lastErrorCtx = nil
	entry.lastErrorTime = time.Time{}

	entry.mutex.Unlock()

	defer cancel()
	for c := range changes {
		if c.Err != nil {
			// Watch errored out.
			//
			// Log it and store the error, but do not clear the value
			// so it can be used until the ttl elapses unless the node
			// was deleted.
			err := fmt.Errorf("WatchSrvKeyspace failed for %v/%v: %v", cell, keyspace, c.Err)
			log.Errorf("%v", err)
			server.counts.Add(errorCategory, 1)
			entry.mutex.Lock()
			if topo.IsErrType(c.Err, topo.NoNode) {
				entry.value = nil
			}
			entry.watchState = watchStateIdle

			// Even though we didn't get a new value, update the lastValueTime
			// here since the watch was successfully running before and we want
			// the value to be cached for the full TTL from here onwards.
			entry.lastValueTime = time.Now()

			entry.lastError = err
			entry.lastErrorCtx = nil
			entry.lastErrorTime = time.Now()
			entry.mutex.Unlock()
			return
		}

		// We got a new value, save it.
		entry.mutex.Lock()
		entry.value = c.Value
		entry.lastError = nil
		entry.lastErrorCtx = nil
		entry.lastErrorTime = time.Time{}
		entry.mutex.Unlock()
	}
}

var watchSrvVSchemaSleepTime = 5 * time.Second

// WatchSrvVSchema is part of the srvtopo.Server interface.
func (server *ResilientServer) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error)) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		foundFirstValue := false

		for {
			current, changes, _ := server.topoServer.WatchSrvVSchema(ctx, cell)
			callback(current.Value, current.Err)
			if !foundFirstValue {
				foundFirstValue = true
				wg.Done()
			}
			if current.Err != nil {
				// Don't log if there is no VSchema to start with.
				if !topo.IsErrType(current.Err, topo.NoNode) {
					log.Warningf("Error watching vschema for cell %s (will wait 5s before retrying): %v", cell, current.Err)
				}
			} else {
				for c := range changes {
					// Note we forward topo.ErrNoNode as is.
					callback(c.Value, c.Err)
					if c.Err != nil {
						log.Warningf("Error while watching vschema for cell %s (will wait 5s before retrying): %v", cell, c.Err)
						break
					}
				}
			}

			// Sleep a bit before trying again.
			time.Sleep(watchSrvVSchemaSleepTime)
		}
	}()

	// Wait for the first value to have been processed.
	wg.Wait()
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

	for _, entry := range server.srvKeyspaceCache {
		entry.mutex.RLock()

		expirationTime := time.Now().Add(server.cacheTTL)
		if entry.watchState != watchStateRunning {
			expirationTime = entry.lastValueTime.Add(server.cacheTTL)
		}

		result.SrvKeyspaces = append(result.SrvKeyspaces, &SrvKeyspaceCacheStatus{
			Cell:           entry.cell,
			Keyspace:       entry.keyspace,
			Value:          entry.value,
			ExpirationTime: expirationTime,
			LastErrorTime:  entry.lastErrorTime,
			LastError:      entry.lastError,
			LastErrorCtx:   entry.lastErrorCtx,
		})
		entry.mutex.RUnlock()
	}

	server.mutex.Unlock()

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
