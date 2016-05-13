// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"fmt"
	"html/template"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	srvTopoCacheTTL = flag.Duration("srv_topo_cache_ttl", 1*time.Second, "how long to use cached entries for topology")
	srvTopoTimeout  = flag.Duration("srv_topo_timeout", 2*time.Second, "topo server timeout")
)

const (
	queryCategory  = "query"
	cachedCategory = "cached"
	errorCategory  = "error"
)

// ResilientSrvTopoServer is an implementation of SrvTopoServer based
// on a topo.Server that uses a cache for two purposes:
// - limit the QPS to the underlying topo.Server
// - return the last known value of the data if there is an error
type ResilientSrvTopoServer struct {
	topoServer topo.Server
	cacheTTL   time.Duration
	counts     *stats.Counters

	// mutex protects the cache map itself, not the individual
	// values in the cache.
	mutex                 sync.RWMutex
	srvKeyspaceNamesCache map[string]*srvKeyspaceNamesEntry
	srvKeyspaceCache      map[string]*srvKeyspaceEntry
	srvShardCache         map[string]*srvShardEntry
}

type srvKeyspaceNamesEntry struct {
	// unmutable values
	cell string

	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex

	insertionTime time.Time
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
	// if watchrunning is set, we are guaranteed to have exactly one of
	// value or lastError be nil, and the other non-nil.
	watchRunning bool
	value        *topodatapb.SrvKeyspace
	lastError    error

	// lastErrorCtx tries to remember the context of the query
	// that failed to get the SrvKeyspace, so we can display it in
	// the status UI. The background routine that refreshes the
	// keyspace will not populate this field.
	// The intent is to have the source of a query that for instance
	// has a bad keyspace or cell name.
	lastErrorCtx context.Context
}

// setValueLocked remembers the current value (or nil if the node doesn't exist)
// and sets lastError / lastErrorCtx if necessary. ske.mutex
// must be held when calling this function.
func (ske *srvKeyspaceEntry) setValueLocked(ctx context.Context, value *topodatapb.SrvKeyspace) {
	ske.value = value
	if value == nil {
		ske.lastError = fmt.Errorf("no SrvKeyspace %v in cell %v", ske.keyspace, ske.cell)
		ske.lastErrorCtx = ctx
	} else {
		ske.lastError = nil
		ske.lastErrorCtx = nil
	}
}

type srvShardEntry struct {
	// unmutable values
	cell     string
	keyspace string
	shard    string

	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex

	insertionTime time.Time
	value         *topodatapb.SrvShard
	lastError     error
	lastErrorCtx  context.Context
}

// NewResilientSrvTopoServer creates a new ResilientSrvTopoServer
// based on the provided topo.Server.
func NewResilientSrvTopoServer(base topo.Server, counterPrefix string) *ResilientSrvTopoServer {
	return &ResilientSrvTopoServer{
		topoServer: base,
		cacheTTL:   *srvTopoCacheTTL,
		counts:     stats.NewCounters(counterPrefix + "Counts"),

		srvKeyspaceNamesCache: make(map[string]*srvKeyspaceNamesEntry),
		srvKeyspaceCache:      make(map[string]*srvKeyspaceEntry),
		srvShardCache:         make(map[string]*srvShardEntry),
	}
}

// GetSrvKeyspaceNames returns all keyspace names for the given cell.
func (server *ResilientSrvTopoServer) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
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

	// If the entry is fresh enough, return it
	if time.Now().Sub(entry.insertionTime) < server.cacheTTL {
		return entry.value, entry.lastError
	}

	// not in cache or too old, get the real value
	newCtx, cancel := context.WithTimeout(context.Background(), *srvTopoTimeout)
	defer cancel()

	result, err := server.topoServer.GetSrvKeyspaceNames(newCtx, cell)
	if err != nil {
		if entry.insertionTime.IsZero() {
			server.counts.Add(errorCategory, 1)
			log.Errorf("GetSrvKeyspaceNames(%v, %v) failed: %v (no cached value, caching and returning error)", newCtx, cell, err)
		} else {
			server.counts.Add(cachedCategory, 1)
			log.Warningf("GetSrvKeyspaceNames(%v, %v) failed: %v (returning cached value: %v %v)", newCtx, cell, err, entry.value, entry.lastError)
			return entry.value, entry.lastError
		}
	}

	// save the value we got and the current time in the cache
	entry.insertionTime = time.Now()
	entry.value = result
	entry.lastError = err
	entry.lastErrorCtx = newCtx
	return result, err
}

// WatchVSchema is part of the SrvTopoServer API
func (server *ResilientSrvTopoServer) WatchVSchema(ctx context.Context, keyspace string) (notifications <-chan string, err error) {
	return server.topoServer.WatchVSchema(ctx, keyspace)
}

func (server *ResilientSrvTopoServer) getSrvKeyspaceEntry(cell, keyspace string) *srvKeyspaceEntry {
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
func (server *ResilientSrvTopoServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
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

	// Watch is not running, let's try to start it.
	// We use a background context, as the watch should last
	// forever (as opposed to the current query's ctx getting
	// the current value).
	newCtx := context.Background()
	notifications, err := server.topoServer.WatchSrvKeyspace(newCtx, cell, keyspace)
	if err != nil {
		// lastError and lastErrorCtx will be visible from the UI
		// until the next try
		entry.value = nil
		entry.lastError = err
		entry.lastErrorCtx = ctx
		log.Errorf("WatchSrvKeyspace failed for %v/%v: %v", cell, keyspace, err)
		return nil, err
	}
	sk, ok := <-notifications
	if !ok {
		// lastError and lastErrorCtx will be visible from the UI
		// until the next try
		entry.value = nil
		entry.lastError = fmt.Errorf("failed to receive initial value from topology watcher")
		entry.lastErrorCtx = ctx
		log.Errorf("WatchSrvKeyspace first result failed for %v/%v", cell, keyspace)
		return nil, entry.lastError
	}

	// we are now watching, cache the first notification
	entry.watchRunning = true
	entry.setValueLocked(ctx, sk)

	go func() {
		for sk := range notifications {
			entry.mutex.Lock()
			entry.setValueLocked(nil, sk)
			entry.mutex.Unlock()
		}
		log.Errorf("failed to receive from channel")
		entry.mutex.Lock()
		entry.watchRunning = false
		entry.value = nil
		entry.lastError = fmt.Errorf("watch for SrvKeyspace %v in cell %v ended", keyspace, cell)
		entry.lastErrorCtx = nil
		entry.mutex.Unlock()
	}()

	return entry.value, entry.lastError
}

// GetSrvShard returns SrvShard object for the given cell, keyspace, and shard.
func (server *ResilientSrvTopoServer) GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*topodatapb.SrvShard, error) {
	server.counts.Add(queryCategory, 1)

	// find the entry in the cache, add it if not there
	key := cell + "." + keyspace + "." + shard
	server.mutex.Lock()
	entry, ok := server.srvShardCache[key]
	if !ok {
		entry = &srvShardEntry{
			cell:     cell,
			keyspace: keyspace,
			shard:    shard,
		}
		server.srvShardCache[key] = entry
	}
	server.mutex.Unlock()

	// Lock the entry, and do everything holding the lock.  This
	// means two concurrent requests will only issue one
	// underlying query.
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	// If the entry is fresh enough, return it
	if time.Now().Sub(entry.insertionTime) < server.cacheTTL {
		return entry.value, entry.lastError
	}

	// not in cache or too old, get the real value
	newCtx, cancel := context.WithTimeout(context.Background(), *srvTopoTimeout)
	defer cancel()

	result, err := server.topoServer.GetSrvShard(newCtx, cell, keyspace, shard)
	if err != nil {
		if entry.insertionTime.IsZero() {
			server.counts.Add(errorCategory, 1)
			log.Errorf("GetSrvShard(%v, %v, %v, %v) failed: %v (no cached value, caching and returning error)", newCtx, cell, keyspace, shard, err)
		} else {
			server.counts.Add(cachedCategory, 1)
			log.Warningf("GetSrvShard(%v, %v, %v, %v) failed: %v (returning cached value: %v %v)", newCtx, cell, keyspace, shard, err, entry.value, entry.lastError)
			return entry.value, entry.lastError
		}
	}

	// save the value we got and the current time in the cache
	entry.insertionTime = time.Now()
	entry.value = result
	entry.lastError = err
	entry.lastErrorCtx = newCtx
	return result, err
}

// The next few structures and methods are used to get a displayable
// version of the cache in a status page

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
	for tabletType, keyspacePartition := range st.Value.Partitions {
		result += "&nbsp;<b>" + string(tabletType) + "</b>"
		for _, shard := range keyspacePartition.ShardReferences {
			result += "&nbsp;" + shard.Name
		}
		result += "<br>"
	}

	if st.Value.ShardingColumnName != "" {
		result += "<b>ShardingColumnName:</b>&nbsp;" + st.Value.ShardingColumnName + "<br>"
		result += "<b>ShardingColumnType:</b>&nbsp;" + string(st.Value.ShardingColumnType) + "<br>"
	}

	if len(st.Value.ServedFrom) > 0 {
		result += "<b>ServedFrom:</b><br>"
		for _, sf := range st.Value.ServedFrom {
			result += "&nbsp;<b>" + strings.ToLower(sf.TabletType.String()) + "</b>&nbsp;" + sf.Keyspace + "<br>"
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

// SrvShardCacheStatus is the current value for a SrvShard object
type SrvShardCacheStatus struct {
	Cell         string
	Keyspace     string
	Shard        string
	Value        *topodatapb.SrvShard
	LastError    error
	LastErrorCtx context.Context
}

// StatusAsHTML returns an HTML version of our status.
// It works best if there is data in the cache.
func (st *SrvShardCacheStatus) StatusAsHTML() template.HTML {
	if st.Value == nil {
		return template.HTML("No Data")
	}

	result := "<b>Name:</b>&nbsp;" + st.Value.Name + "<br>"
	result += "<b>KeyRange:</b>&nbsp;" + st.Value.KeyRange.String() + "<br>"
	result += "<b>MasterCell:</b>&nbsp;" + st.Value.MasterCell + "<br>"

	return template.HTML(result)
}

// SrvShardCacheStatusList is used for sorting
type SrvShardCacheStatusList []*SrvShardCacheStatus

// Len is part of sort.Interface
func (sscsl SrvShardCacheStatusList) Len() int {
	return len(sscsl)
}

// Less is part of sort.Interface
func (sscsl SrvShardCacheStatusList) Less(i, j int) bool {
	return sscsl[i].Cell+"."+sscsl[i].Keyspace <
		sscsl[j].Cell+"."+sscsl[j].Keyspace
}

// Swap is part of sort.Interface
func (sscsl SrvShardCacheStatusList) Swap(i, j int) {
	sscsl[i], sscsl[j] = sscsl[j], sscsl[i]
}

// ResilientSrvTopoServerCacheStatus has the full status of the cache
type ResilientSrvTopoServerCacheStatus struct {
	SrvKeyspaceNames SrvKeyspaceNamesCacheStatusList
	SrvKeyspaces     SrvKeyspaceCacheStatusList
	SrvShards        SrvShardCacheStatusList
}

// CacheStatus returns a displayable version of the cache
func (server *ResilientSrvTopoServer) CacheStatus() *ResilientSrvTopoServerCacheStatus {
	result := &ResilientSrvTopoServerCacheStatus{}
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

	for _, entry := range server.srvShardCache {
		entry.mutex.Lock()
		result.SrvShards = append(result.SrvShards, &SrvShardCacheStatus{
			Cell:         entry.cell,
			Keyspace:     entry.keyspace,
			Shard:        entry.shard,
			Value:        entry.value,
			LastError:    entry.lastError,
			LastErrorCtx: entry.lastErrorCtx,
		})
		entry.mutex.Unlock()
	}

	server.mutex.Unlock()

	// do the sorting without the mutex
	sort.Sort(result.SrvKeyspaceNames)
	sort.Sort(result.SrvKeyspaces)
	sort.Sort(result.SrvShards)

	return result
}
