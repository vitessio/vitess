// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	// GatewayImplementation controls the implementation of Gateway.
	GatewayImplementation = flag.String("gateway_implementation", "shardgateway", "The implementation of gateway")
)

// A Gateway is the query processing module for each shard,
// which is used by ScatterConn.
type Gateway interface {
	// InitializeConnections creates connections to VTTablets.
	InitializeConnections(ctx context.Context) error

	// Execute executes the non-streaming query for the specified keyspace, shard, and tablet type.
	Execute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error)

	// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
	ExecuteBatch(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error)

	// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
	StreamExecute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc)

	// Begin starts a transaction for the specified keyspace, shard, and tablet type.
	// It returns the transaction ID.
	Begin(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType) (int64, error)

	// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
	Commit(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error

	// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
	Rollback(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error

	// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
	SplitQuery(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error)

	// Close shuts down underlying connections.
	Close(ctx context.Context) error

	// CacheStatus returns a list of GatewayEndPointCacheStatus per endpoint.
	CacheStatus() GatewayEndPointCacheStatusList
}

// GatewayCreator is the func which can create the actual gateway object.
type GatewayCreator func(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, connTimeoutTotal, connTimeoutPerConn, connLife time.Duration, connTimings *stats.MultiTimings, tabletTypesToWait []topodatapb.TabletType) Gateway

var gatewayCreators = make(map[string]GatewayCreator)

// RegisterGatewayCreator registers a GatewayCreator with given name.
func RegisterGatewayCreator(name string, gc GatewayCreator) {
	if _, ok := gatewayCreators[name]; ok {
		log.Fatalf("Gateway %s already exists", name)
	}
	gatewayCreators[name] = gc
}

// GetGatewayCreator returns the GatewayCreator specified by GatewayImplementation flag.
func GetGatewayCreator() GatewayCreator {
	gc, ok := gatewayCreators[*GatewayImplementation]
	if !ok {
		log.Fatalf("No gateway registered as %s", *GatewayImplementation)
	}
	return gc
}

// GetGatewayCreatorByName returns the GatewayCreator specified by the given name.
func GetGatewayCreatorByName(name string) GatewayCreator {
	gc, ok := gatewayCreators[name]
	if !ok {
		log.Errorf("No gateway registered as %s", name)
		return nil
	}
	return gc
}

// GatewayEndPointCacheStatusList is a slice of GatewayEndPointCacheStatus.
type GatewayEndPointCacheStatusList []*GatewayEndPointCacheStatus

// Len is part of sort.Interface.
func (gepcsl GatewayEndPointCacheStatusList) Len() int {
	return len(gepcsl)
}

// Less is part of sort.Interface.
func (gepcsl GatewayEndPointCacheStatusList) Less(i, j int) bool {
	iKey := strings.Join([]string{gepcsl[i].Keyspace, gepcsl[i].Shard, string(gepcsl[i].TabletType), gepcsl[i].Name}, ".")
	jKey := strings.Join([]string{gepcsl[j].Keyspace, gepcsl[j].Shard, string(gepcsl[j].TabletType), gepcsl[j].Name}, ".")
	return iKey < jKey
}

// Swap is part of sort.Interface.
func (gepcsl GatewayEndPointCacheStatusList) Swap(i, j int) {
	gepcsl[i], gepcsl[j] = gepcsl[j], gepcsl[i]
}

// GatewayEndPointCacheStatus contains the status per endpoint for a gateway.
type GatewayEndPointCacheStatus struct {
	Keyspace   string
	Shard      string
	TabletType topodatapb.TabletType
	Name       string
	Addr       string

	QueryCount uint64
	QueryError uint64
	QPS        uint64
	AvgLatency uint64 // in milliseconds
}

// NewGatewayEndPointStatusAggregator creates a GatewayEndPointStatusAggregator.
func NewGatewayEndPointStatusAggregator() *GatewayEndPointStatusAggregator {
	gepsa := &GatewayEndPointStatusAggregator{}
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			gepsa.resetNextSlot()
		}
	}()
	return gepsa
}

// GatewayEndPointStatusAggregator tracks endpoint status for a gateway.
type GatewayEndPointStatusAggregator struct {
	Keyspace   string
	Shard      string
	TabletType topodatapb.TabletType
	Name       string // the alternative name of an endpoint
	Addr       string // the host:port of an endpoint

	// mu protects below fields.
	mu         sync.RWMutex
	QueryCount uint64
	QueryError uint64
	// for QPS and latency (avg value over a minute)
	queryCountInMinute [60]uint64
	latencyInMinute    [60]time.Duration
}

// UpdateQueryInfo updates the aggregator with the given information about a query.
func (gepsa *GatewayEndPointStatusAggregator) UpdateQueryInfo(tabletType topodatapb.TabletType, elapsed time.Duration, hasError bool) {
	gepsa.mu.Lock()
	defer gepsa.mu.Unlock()
	gepsa.TabletType = tabletType
	idx := time.Now().Second() % 60
	gepsa.QueryCount++
	gepsa.queryCountInMinute[idx]++
	gepsa.latencyInMinute[idx] += elapsed
	if hasError {
		gepsa.QueryError++
	}
}

// GetCacheStatus returns a GatewayEndPointCacheStatus representing the current gateway status.
func (gepsa *GatewayEndPointStatusAggregator) GetCacheStatus() *GatewayEndPointCacheStatus {
	status := &GatewayEndPointCacheStatus{
		Keyspace:   gepsa.Keyspace,
		Shard:      gepsa.Shard,
		TabletType: gepsa.TabletType,
		Name:       gepsa.Name,
		Addr:       gepsa.Addr,
	}
	gepsa.mu.RLock()
	defer gepsa.mu.RUnlock()
	status.QueryCount = gepsa.QueryCount
	status.QueryError = gepsa.QueryError
	var totalQuery uint64
	for _, c := range gepsa.queryCountInMinute {
		totalQuery += c
	}
	var totalLatency time.Duration
	for _, d := range gepsa.latencyInMinute {
		totalLatency += d
	}
	status.QPS = totalQuery / 60
	if totalQuery > 0 {
		status.AvgLatency = uint64(totalLatency.Nanoseconds()) / totalQuery / 100000
	}
	return status
}

// resetNextSlot resets the next tracking slot.
func (gepsa *GatewayEndPointStatusAggregator) resetNextSlot() {
	gepsa.mu.Lock()
	defer gepsa.mu.Unlock()
	idx := (time.Now().Second() + 1) % 60
	gepsa.queryCountInMinute[idx] = 0
	gepsa.latencyInMinute[idx] = 0
}
