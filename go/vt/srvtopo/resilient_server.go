/*
Copyright 2017 Google Inc.

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

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

var (
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
    <th colspan="2">SrvKeyspace Names Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>SrvKeyspace Names</th>
  </tr>
  {{range $i, $skn := .SrvKeyspaceNames}}
  <tr>
    <td>{{github_com_youtube_vitess_vtctld_srv_cell $skn.Cell}}</td>
    <td>{{if $skn.LastError}}<b>{{$skn.LastError}}</b>{{else}}{{range $j, $value := $skn.Value}}{{github_com_youtube_vitess_vtctld_srv_keyspace $skn.Cell $value}}&nbsp;{{end}}{{end}}</td>
  </tr>
  {{end}}
</table>
<br>
<table>
  <tr>
    <th colspan="3">SrvKeyspace Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>Keyspace</th>
    <th>SrvKeyspace</th>
  </tr>
  {{range $i, $sk := .SrvKeyspaces}}
  <tr>
    <td>{{github_com_youtube_vitess_vtctld_srv_cell $sk.Cell}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_keyspace $sk.Cell $sk.Keyspace}}</td>
    <td>{{if $sk.LastError}}<b>{{$sk.LastError}}</b>{{else}}{{$sk.StatusAsHTML}}{{end}}</td>
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
	counts       *stats.Counters

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

	insertionTime time.Time
	lastQueryTime time.Time
	value         []string
	lastError     error
	lastErrorCtx  context.Context
}

type srvKeyspaceEntry struct {
	// unmutable values
	cell     string
	keyspace string

	// the mutex protects any access to this structure (read or write)
	mutex sync.RWMutex

	// watchRunning describes if the watch go routine is running.
	// It is easier to have an explicit field instead of guessing
	// based on value and lastError.
	//
	// if watchrunning is not set, the next time we try to access the
	// keyspace, we will start a watch.
	// if watchrunning is set, we are guaranteed to have lastError be
	// non-nil and an up-to-date value (which may be nil)
	watchRunning bool
	value        *topodatapb.SrvKeyspace
	lastError    error

	// valueTime is the time when the watch last obtained a non-nil value.
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

	// lastErrorTime records the time that the watch failed, so that
	// any requests that come in
	lastErrorTime time.Time
}

// NewResilientServer creates a new ResilientServer
// based on the provided topo.Server.
func NewResilientServer(base *topo.Server, counterPrefix string) *ResilientServer {
	return &ResilientServer{
		topoServer:   base,
		cacheTTL:     *srvTopoCacheTTL,
		cacheRefresh: *srvTopoCacheRefresh,
		counts:       stats.NewCounters(counterPrefix + "Counts"),

		srvKeyspaceNamesCache: make(map[string]*srvKeyspaceNamesEntry),
		srvKeyspaceCache:      make(map[string]*srvKeyspaceEntry),
	}
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

	// Lock the entry, and do everything holding the lock.  This
	// means two concurrent requests will only issue one
	// underlying query.
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	// If it is not time to check again, then return either the cached
	// value or the cached error
	cacheValid := entry.value != nil && time.Since(entry.insertionTime) < server.cacheTTL
	shouldRefresh := time.Since(entry.lastQueryTime) > server.cacheRefresh

	if !shouldRefresh {
		if cacheValid {
			return entry.value, nil
		}
		return nil, entry.lastError
	}

	// Not in cache or needs refresh so try to get the real value.
	// We use the context that issued the query here.
	result, err := server.topoServer.GetSrvKeyspaceNames(ctx, cell)
	if err == nil {
		// save the value we got and the current time in the cache
		entry.insertionTime = time.Now()
		entry.value = result
	} else {
		if entry.insertionTime.IsZero() {
			server.counts.Add(errorCategory, 1)
			log.Errorf("GetSrvKeyspaceNames(%v, %v) failed: %v (no cached value, caching and returning error)", ctx, cell, err)

		} else if cacheValid {
			server.counts.Add(cachedCategory, 1)
			log.Warningf("GetSrvKeyspaceNames(%v, %v) failed: %v (returning cached value: %v %v)", ctx, cell, err, entry.value, entry.lastError)
			result = entry.value
			err = nil
		} else {
			server.counts.Add(errorCategory, 1)
			log.Errorf("GetSrvKeyspaceNames(%v, %v) failed: %v (cached value expired)", ctx, cell, err)
			entry.insertionTime = time.Time{}
			entry.value = nil
		}
	}

	entry.lastError = err
	entry.lastQueryTime = time.Now()
	entry.lastErrorCtx = ctx
	return result, err
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
	if entry.watchRunning {
		v, e := entry.value, entry.lastError
		entry.mutex.RUnlock()
		return v, e
	}
	entry.mutex.RUnlock()

	// Lock the entry, and do everything holding the lock.  This
	// means two concurrent requests will only issue one
	// underlying query.
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	// If the watch is already running, return the value
	if entry.watchRunning {
		return entry.value, entry.lastError
	}

	// Watch is not running, but check if the last time we got an error was
	// more recent than the refresh interval.
	//
	// If so return either the last cached value or the last error we got.
	cacheValid := entry.value != nil && time.Since(entry.lastValueTime) < server.cacheTTL
	shouldRefresh := time.Since(entry.lastErrorTime) > server.cacheRefresh

	if !shouldRefresh {
		if cacheValid {
			server.counts.Add(cachedCategory, 1)
			return entry.value, nil
		}
		return nil, entry.lastError
	}

	// Time to try to start the watch again.
	// We use a background context, as starting the watch should keep going
	// even if the current query context is short-lived.
	newCtx := context.Background()
	current, changes, cancel := server.topoServer.WatchSrvKeyspace(newCtx, cell, keyspace)
	if current.Err != nil {
		// lastError and lastErrorCtx will be visible from the UI
		// until the next try
		entry.lastError = current.Err
		entry.lastErrorCtx = ctx
		entry.lastErrorTime = time.Now()

		// if the node disappears, delete the cached value
		if current.Err == topo.ErrNoNode {
			entry.value = nil
		}

		server.counts.Add(errorCategory, 1)
		log.Errorf("Initial WatchSrvKeyspace failed for %v/%v: %v", cell, keyspace, current.Err)

		if cacheValid {
			return entry.value, nil
		}

		return nil, current.Err
	}

	// we are now watching, cache the first notification
	entry.watchRunning = true
	entry.value = current.Value
	entry.lastValueTime = time.Now()
	entry.lastError = nil
	entry.lastErrorCtx = nil
	go func() {
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
				if err == topo.ErrNoNode {
					entry.value = nil
				}
				entry.watchRunning = false
				entry.lastError = err
				entry.lastErrorCtx = nil
				entry.mutex.Unlock()
				cancel()
				return
			}

			// We got a new value, save it.
			entry.mutex.Lock()
			entry.value = c.Value
			entry.lastValueTime = time.Now()
			entry.lastError = nil
			entry.lastErrorCtx = nil
			entry.mutex.Unlock()
		}
	}()

	return entry.value, entry.lastError
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
				if current.Err != topo.ErrNoNode {
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
	Cell         string
	Value        []string
	LastError    error
	LastErrorCtx context.Context
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
	Cell         string
	Keyspace     string
	Value        *topodatapb.SrvKeyspace
	LastError    error
	LastErrorCtx context.Context
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
			Cell:         entry.cell,
			Value:        entry.value,
			LastError:    entry.lastError,
			LastErrorCtx: entry.lastErrorCtx,
		})
		entry.mutex.Unlock()
	}

	for _, entry := range server.srvKeyspaceCache {
		entry.mutex.RLock()
		result.SrvKeyspaces = append(result.SrvKeyspaces, &SrvKeyspaceCacheStatus{
			Cell:         entry.cell,
			Keyspace:     entry.keyspace,
			Value:        entry.value,
			LastError:    entry.lastError,
			LastErrorCtx: entry.lastErrorCtx,
		})
		entry.mutex.RUnlock()
	}

	server.mutex.Unlock()

	// do the sorting without the mutex
	sort.Sort(result.SrvKeyspaceNames)
	sort.Sort(result.SrvKeyspaces)

	return result
}
