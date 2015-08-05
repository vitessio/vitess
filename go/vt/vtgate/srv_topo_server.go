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

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	srvTopoCacheTTL    = flag.Duration("srv_topo_cache_ttl", 1*time.Second, "how long to use cached entries for topology")
	enableRemoteMaster = flag.Bool("enable_remote_master", false, "enable remote master access")
	srvTopoTimeout     = flag.Duration("srv_topo_timeout", 2*time.Second, "topo server timeout")
)

const (
	queryCategory       = "query"
	cachedCategory      = "cached"
	errorCategory       = "error"
	remoteQueryCategory = "remote-query"
	remoteErrorCategory = "remote-error"
)

// SrvTopoServer is a subset of topo.Server that only contains the serving
// graph read-only calls used by clients to resolve serving addresses.
type SrvTopoServer interface {
	GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error)

	GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topo.SrvKeyspace, error)

	GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*pb.SrvShard, error)

	GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topo.TabletType) (*pb.EndPoints, int64, error)
}

// ResilientSrvTopoServer is an implementation of SrvTopoServer based
// on a topo.Server that uses a cache for two purposes:
// - limit the QPS to the underlying topo.Server
// - return the last known value of the data if there is an error
type ResilientSrvTopoServer struct {
	topoServer         topo.Server
	cacheTTL           time.Duration
	enableRemoteMaster bool
	counts             *stats.Counters

	// mutex protects the cache map itself, not the individual
	// values in the cache.
	mutex                 sync.Mutex
	srvKeyspaceNamesCache map[string]*srvKeyspaceNamesEntry
	srvKeyspaceCache      map[string]*srvKeyspaceEntry
	srvShardCache         map[string]*srvShardEntry
	endPointsCache        map[string]*endPointsEntry

	// GetEndPoints stats.
	endPointCounters *endPointCounters
}

type endPointCounters struct {
	queries             *stats.MultiCounters
	errors              *stats.MultiCounters
	emptyResults        *stats.MultiCounters
	remoteQueries       *stats.MultiCounters
	numberReturned      *stats.MultiCounters
	degradedResults     *stats.MultiCounters
	cacheHits           *stats.MultiCounters
	remoteLookups       *stats.MultiCounters
	remoteLookupErrors  *stats.MultiCounters
	lookupErrors        *stats.MultiCounters
	staleCacheFallbacks *stats.MultiCounters
}

func newEndPointCounters(counterPrefix string) *endPointCounters {
	labels := []string{"Cell", "Keyspace", "ShardName", "DbType"}
	return &endPointCounters{
		queries:             stats.NewMultiCounters(counterPrefix+"EndPointQueryCount", labels),
		errors:              stats.NewMultiCounters(counterPrefix+"EndPointErrorCount", labels),
		emptyResults:        stats.NewMultiCounters(counterPrefix+"EndPointEmptyResultCount", labels),
		numberReturned:      stats.NewMultiCounters(counterPrefix+"EndPointsReturnedCount", labels),
		degradedResults:     stats.NewMultiCounters(counterPrefix+"EndPointDegradedResultCount", labels),
		cacheHits:           stats.NewMultiCounters(counterPrefix+"EndPointCacheHitCount", labels),
		remoteQueries:       stats.NewMultiCounters(counterPrefix+"EndPointRemoteQueryCount", labels),
		remoteLookups:       stats.NewMultiCounters(counterPrefix+"EndPointRemoteLookupCount", labels),
		remoteLookupErrors:  stats.NewMultiCounters(counterPrefix+"EndPointRemoteLookupErrorCount", labels),
		lookupErrors:        stats.NewMultiCounters(counterPrefix+"EndPointLookupErrorCount", labels),
		staleCacheFallbacks: stats.NewMultiCounters(counterPrefix+"EndPointStaleCacheFallbackCount", labels),
	}
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
	mutex sync.Mutex

	insertionTime time.Time
	value         *topo.SrvKeyspace
	lastError     error
	lastErrorCtx  context.Context
}

type srvShardEntry struct {
	// unmutable values
	cell     string
	keyspace string
	shard    string

	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex

	insertionTime time.Time
	value         *pb.SrvShard
	lastError     error
	lastErrorCtx  context.Context
}

type endPointsEntry struct {
	// unmutable values
	cell       string
	keyspace   string
	shard      string
	tabletType topo.TabletType
	remote     bool

	// the mutex protects any access to this structure (read or write)
	mutex sync.Mutex

	insertionTime time.Time

	// value is the end points that were returned to the client.
	value *pb.EndPoints

	// originalValue is the end points that were returned from
	// the topology server.
	originalValue *pb.EndPoints

	lastError    error
	lastErrorCtx context.Context
}

func endPointIsHealthy(ep *pb.EndPoint) bool {
	// if we are behind on replication, we're not 100% healthy
	return ep.HealthMap == nil || ep.HealthMap[topo.ReplicationLag] != topo.ReplicationLagHigh
}

// filterUnhealthyServers removes the unhealthy servers from the list,
// unless all servers are unhealthy, then it keeps them all.
func filterUnhealthyServers(endPoints *pb.EndPoints) *pb.EndPoints {

	// no endpoints, return right away
	if endPoints == nil || len(endPoints.Entries) == 0 {
		return endPoints
	}

	healthyEndPoints := make([]*pb.EndPoint, 0, len(endPoints.Entries))
	for _, ep := range endPoints.Entries {
		// if we are behind on replication, we're not 100% healthy
		if !endPointIsHealthy(ep) {
			continue
		}

		healthyEndPoints = append(healthyEndPoints, ep)
	}

	// we have healthy guys, we return them
	if len(healthyEndPoints) > 0 {
		return &pb.EndPoints{Entries: healthyEndPoints}
	}

	// we only have unhealthy guys, return them
	return endPoints
}

// NewResilientSrvTopoServer creates a new ResilientSrvTopoServer
// based on the provided SrvTopoServer.
func NewResilientSrvTopoServer(base topo.Server, counterPrefix string) *ResilientSrvTopoServer {
	return &ResilientSrvTopoServer{
		topoServer:         base,
		cacheTTL:           *srvTopoCacheTTL,
		enableRemoteMaster: *enableRemoteMaster,
		counts:             stats.NewCounters(counterPrefix + "Counts"),

		srvKeyspaceNamesCache: make(map[string]*srvKeyspaceNamesEntry),
		srvKeyspaceCache:      make(map[string]*srvKeyspaceEntry),
		srvShardCache:         make(map[string]*srvShardEntry),
		endPointsCache:        make(map[string]*endPointsEntry),

		endPointCounters: newEndPointCounters(counterPrefix),
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

// GetSrvKeyspace returns SrvKeyspace object for the given cell and keyspace.
func (server *ResilientSrvTopoServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topo.SrvKeyspace, error) {
	server.counts.Add(queryCategory, 1)

	// find the entry in the cache, add it if not there
	key := cell + "." + keyspace
	server.mutex.Lock()
	entry, ok := server.srvKeyspaceCache[key]
	if !ok {
		entry = &srvKeyspaceEntry{
			cell:     cell,
			keyspace: keyspace,
		}
		server.srvKeyspaceCache[key] = entry
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

	result, err := server.topoServer.GetSrvKeyspace(newCtx, cell, keyspace)
	if err != nil {
		if entry.insertionTime.IsZero() {
			server.counts.Add(errorCategory, 1)
			log.Errorf("GetSrvKeyspace(%v, %v, %v) failed: %v (no cached value, caching and returning error)", newCtx, cell, keyspace, err)
		} else {
			server.counts.Add(cachedCategory, 1)
			log.Warningf("GetSrvKeyspace(%v, %v, %v) failed: %v (returning cached value: %v %v)", newCtx, cell, keyspace, err, entry.value, entry.lastError)
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

// GetSrvShard returns SrvShard object for the given cell, keyspace, and shard.
func (server *ResilientSrvTopoServer) GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*pb.SrvShard, error) {
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

// GetEndPoints return all endpoints for the given cell, keyspace, shard, and tablet type.
func (server *ResilientSrvTopoServer) GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topo.TabletType) (result *pb.EndPoints, version int64, err error) {
	shard = strings.ToLower(shard)
	key := []string{cell, keyspace, shard, string(tabletType)}

	server.counts.Add(queryCategory, 1)
	server.endPointCounters.queries.Add(key, 1)

	// find the entry in the cache, add it if not there
	keyStr := strings.Join(key, ".")
	server.mutex.Lock()
	entry, ok := server.endPointsCache[keyStr]
	if !ok {
		entry = &endPointsEntry{
			cell:       cell,
			keyspace:   keyspace,
			shard:      shard,
			tabletType: tabletType,
		}
		server.endPointsCache[keyStr] = entry
	}
	server.mutex.Unlock()

	// Lock the entry, and do everything holding the lock.  This
	// means two concurrent requests will only issue one
	// underlying query.
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	// Whether the query was serviced with remote endpoints.
	remote := false

	// Record some stats regardless of cache status.
	defer func() {
		if remote {
			server.endPointCounters.remoteQueries.Add(key, 1)
		}
		if err != nil {
			server.endPointCounters.errors.Add(key, 1)
			return
		}
		if result == nil || len(result.Entries) == 0 {
			server.endPointCounters.emptyResults.Add(key, 1)
			return
		}
		server.endPointCounters.numberReturned.Add(key, int64(len(result.Entries)))
		// We either serve all healthy endpoints or all degraded endpoints, so the first entry is representative.
		if !endPointIsHealthy(result.Entries[0]) {
			server.endPointCounters.degradedResults.Add(key, 1)
			return
		}
	}()

	// If the entry is fresh enough, return it
	if time.Now().Sub(entry.insertionTime) < server.cacheTTL {
		server.endPointCounters.cacheHits.Add(key, 1)
		remote = entry.remote
		return entry.value, -1, entry.lastError
	}

	// not in cache or too old, get the real value
	newCtx, cancel := context.WithTimeout(context.Background(), *srvTopoTimeout)
	defer cancel()

	result, _, err = server.topoServer.GetEndPoints(newCtx, cell, keyspace, shard, tabletType)
	// get remote endpoints for master if enabled
	if err != nil && server.enableRemoteMaster && tabletType == topo.TYPE_MASTER {
		remote = true
		server.counts.Add(remoteQueryCategory, 1)
		server.endPointCounters.remoteLookups.Add(key, 1)
		var ss *pb.SrvShard
		ss, err = server.topoServer.GetSrvShard(newCtx, cell, keyspace, shard)
		if err != nil {
			server.counts.Add(remoteErrorCategory, 1)
			server.endPointCounters.remoteLookupErrors.Add(key, 1)
			log.Errorf("GetEndPoints(%v, %v, %v, %v, %v) failed to get SrvShard for remote master: %v",
				newCtx, cell, keyspace, shard, tabletType, err)
		} else {
			if ss.MasterCell != "" && ss.MasterCell != cell {
				result, _, err = server.topoServer.GetEndPoints(newCtx, ss.MasterCell, keyspace, shard, tabletType)
			}
		}
	}
	if err != nil {
		server.endPointCounters.lookupErrors.Add(key, 1)
		if entry.insertionTime.IsZero() {
			server.counts.Add(errorCategory, 1)
			log.Errorf("GetEndPoints(%v, %v, %v, %v, %v) failed: %v (no cached value, caching and returning error)", newCtx, cell, keyspace, shard, tabletType, err)
		} else {
			server.counts.Add(cachedCategory, 1)
			server.endPointCounters.staleCacheFallbacks.Add(key, 1)
			log.Warningf("GetEndPoints(%v, %v, %v, %v, %v) failed: %v (returning cached value: %v %v)", newCtx, cell, keyspace, shard, tabletType, err, entry.value, entry.lastError)
			return entry.value, -1, entry.lastError
		}
	}

	// save the value we got and the current time in the cache
	entry.insertionTime = time.Now()
	entry.originalValue = result
	entry.value = filterUnhealthyServers(result)
	entry.lastError = err
	entry.lastErrorCtx = newCtx
	entry.remote = remote
	return entry.value, -1, err
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
	Value        *topo.SrvKeyspace
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
		for tabletType, keyspace := range st.Value.ServedFrom {
			result += "&nbsp;<b>" + string(tabletType) + "</b>&nbsp;" + keyspace + "<br>"
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
	Value        *pb.SrvShard
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

// EndPointsCacheStatus is the current value for an EndPoints object
type EndPointsCacheStatus struct {
	Cell          string
	Keyspace      string
	Shard         string
	TabletType    topo.TabletType
	Value         *pb.EndPoints
	OriginalValue *pb.EndPoints
	LastError     error
	LastErrorCtx  context.Context
}

// StatusAsHTML returns an HTML version of our status.
// It works best if there is data in the cache.
func (st *EndPointsCacheStatus) StatusAsHTML() template.HTML {
	ovl := 0
	if st.OriginalValue != nil {
		ovl = len(st.OriginalValue.Entries)
	}
	vl := 0
	if st.Value != nil {
		vl = len(st.Value.Entries)
	}
	// Assemble links to individual endpoints
	epLinks := "{ "
	if ovl > 0 {
		for _, ove := range st.OriginalValue.Entries {
			healthColor := "red"
			var vtPort int32
			if vl > 0 {
				for _, ve := range st.Value.Entries {
					if ove.Uid == ve.Uid {
						ok := false
						if vtPort, ok = ve.PortMap["vt"]; ok {
							// EndPoint is healthy
							healthColor = "green"
							if len(ve.HealthMap) > 0 {
								// EndPoint is half healthy
								healthColor = "orange"
							}
						}
					}
				}
			}
			epLinks += fmt.Sprintf(
				"<a href=\"http://%v:%d\" style=\"color:%v\">%v:%d</a> ",
				ove.Host, vtPort, healthColor, ove.Host, vtPort)
		}
	}
	epLinks += "}"
	if ovl == vl {
		if vl == 0 {
			return template.HTML(fmt.Sprintf("<b>No healthy endpoints</b>, %v", epLinks))
		}
		if len(st.OriginalValue.Entries[0].HealthMap) > 0 {
			return template.HTML(fmt.Sprintf("<b>Serving from %v degraded endpoints</b>, %v", vl, epLinks))
		}
		return template.HTML(fmt.Sprintf("All %v endpoints are healthy, %v", vl, epLinks))
	}
	return template.HTML(fmt.Sprintf("Serving from %v healthy endpoints out of %v, %v", vl, ovl, epLinks))
}

// EndPointsCacheStatusList is used for sorting
type EndPointsCacheStatusList []*EndPointsCacheStatus

// Len is part of sort.Interface
func (epcsl EndPointsCacheStatusList) Len() int {
	return len(epcsl)
}

// Less is part of sort.Interface
func (epcsl EndPointsCacheStatusList) Less(i, j int) bool {
	return epcsl[i].Cell+"."+epcsl[i].Keyspace+"."+epcsl[i].Shard+"."+string(epcsl[i].TabletType) <
		epcsl[j].Cell+"."+epcsl[j].Keyspace+"."+epcsl[j].Shard+"."+string(epcsl[j].TabletType)
}

// Swap is part of sort.Interface
func (epcsl EndPointsCacheStatusList) Swap(i, j int) {
	epcsl[i], epcsl[j] = epcsl[j], epcsl[i]
}

// ResilientSrvTopoServerCacheStatus has the full status of the cache
type ResilientSrvTopoServerCacheStatus struct {
	SrvKeyspaceNames SrvKeyspaceNamesCacheStatusList
	SrvKeyspaces     SrvKeyspaceCacheStatusList
	SrvShards        SrvShardCacheStatusList
	EndPoints        EndPointsCacheStatusList
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
		entry.mutex.Lock()
		result.SrvKeyspaces = append(result.SrvKeyspaces, &SrvKeyspaceCacheStatus{
			Cell:         entry.cell,
			Keyspace:     entry.keyspace,
			Value:        entry.value,
			LastError:    entry.lastError,
			LastErrorCtx: entry.lastErrorCtx,
		})
		entry.mutex.Unlock()
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

	for _, entry := range server.endPointsCache {
		entry.mutex.Lock()
		result.EndPoints = append(result.EndPoints, &EndPointsCacheStatus{
			Cell:          entry.cell,
			Keyspace:      entry.keyspace,
			Shard:         entry.shard,
			TabletType:    entry.tabletType,
			Value:         entry.value,
			OriginalValue: entry.originalValue,
			LastError:     entry.lastError,
			LastErrorCtx:  entry.lastErrorCtx,
		})
		entry.mutex.Unlock()
	}

	server.mutex.Unlock()

	// do the sorting without the mutex
	sort.Sort(result.SrvKeyspaceNames)
	sort.Sort(result.SrvKeyspaces)
	sort.Sort(result.SrvShards)
	sort.Sort(result.EndPoints)

	return result
}
