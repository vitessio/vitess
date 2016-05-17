// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	// GatewayImplementation controls the implementation of Gateway.
	GatewayImplementation = flag.String("gateway_implementation", "discoverygateway", "The implementation of gateway")
)

// A Gateway is the query processing module for each shard,
// which is used by ScatterConn.
type Gateway interface {
	// Execute executes the non-streaming query for the specified keyspace, shard, and tablet type.
	Execute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error)

	// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
	ExecuteBatch(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error)

	// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
	StreamExecute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}) (sqltypes.ResultStream, error)

	// Begin starts a transaction for the specified keyspace, shard, and tablet type.
	// It returns the transaction ID.
	Begin(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType) (int64, error)

	// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
	Commit(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error

	// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
	Rollback(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error

	// BeginExecute executes a begin and the non-streaming query
	// for the specified keyspace, shard, and tablet type.
	BeginExecute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}) (*sqltypes.Result, int64, error)

	// BeginExecuteBatch executes a begin and a group of queries
	// for the specified keyspace, shard, and tablet type.
	BeginExecuteBatch(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, queries []querytypes.BoundQuery, asTransaction bool) ([]sqltypes.Result, int64, error)

	// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
	SplitQuery(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error)

	// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
	// TODO(erez): Rename to SplitQuery after migration to SplitQuery V2.
	SplitQueryV2(
		ctx context.Context,
		keyspace,
		shard string,
		tabletType topodatapb.TabletType,
		sql string,
		bindVariables map[string]interface{},
		splitColumns []string,
		splitCount int64,
		numRowsPerQueryPart int64,
		algorithm querypb.SplitQueryRequest_Algorithm) ([]querytypes.QuerySplit, error)

	// Close shuts down underlying connections.
	Close(ctx context.Context) error

	// CacheStatus returns a list of GatewayTabletCacheStatus per tablet.
	CacheStatus() GatewayTabletCacheStatusList
}

// GatewayCreator is the func which can create the actual gateway object.
type GatewayCreator func(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, cell string, retryCount int, tabletTypesToWait []topodatapb.TabletType) Gateway

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

// ShardError is the error about a specific shard.
// It implements vterrors.VtError.
type ShardError struct {
	// ShardIdentifier is the keyspace+shard.
	ShardIdentifier string
	// InTransaction indicates if it is inside a transaction.
	InTransaction bool
	// Err preserves the original error, so that we don't need to parse the error string.
	Err error
	// ErrorCode is the error code to use for all the tablet errors in aggregate
	ErrorCode vtrpcpb.ErrorCode
}

// Error returns the error string.
func (e *ShardError) Error() string {
	if e.ShardIdentifier == "" {
		return fmt.Sprintf("%v", e.Err)
	}
	return fmt.Sprintf("shard, host: %s, %v", e.ShardIdentifier, e.Err)
}

// VtErrorCode returns the underlying Vitess error code.
// This is part of vterrors.VtError interface.
func (e *ShardError) VtErrorCode() vtrpcpb.ErrorCode {
	return e.ErrorCode
}

// GatewayTabletCacheStatusList is a slice of GatewayTabletCacheStatus.
type GatewayTabletCacheStatusList []*GatewayTabletCacheStatus

// Len is part of sort.Interface.
func (gtcsl GatewayTabletCacheStatusList) Len() int {
	return len(gtcsl)
}

// Less is part of sort.Interface.
func (gtcsl GatewayTabletCacheStatusList) Less(i, j int) bool {
	iKey := strings.Join([]string{gtcsl[i].Keyspace, gtcsl[i].Shard, string(gtcsl[i].TabletType), gtcsl[i].Name}, ".")
	jKey := strings.Join([]string{gtcsl[j].Keyspace, gtcsl[j].Shard, string(gtcsl[j].TabletType), gtcsl[j].Name}, ".")
	return iKey < jKey
}

// Swap is part of sort.Interface.
func (gtcsl GatewayTabletCacheStatusList) Swap(i, j int) {
	gtcsl[i], gtcsl[j] = gtcsl[j], gtcsl[i]
}

// GatewayTabletCacheStatus contains the status per tablet for a gateway.
type GatewayTabletCacheStatus struct {
	Keyspace   string
	Shard      string
	TabletType topodatapb.TabletType
	Name       string
	Addr       string

	QueryCount uint64
	QueryError uint64
	QPS        uint64
	AvgLatency float64 // in milliseconds
}

const (
	aggrChanSize = 10000
)

var (
	// aggrChan buffers queryInfo objects to be processed.
	aggrChan chan *queryInfo
	// muAggr protects below vars.
	muAggr sync.Mutex
	// aggregators holds all Aggregators created.
	aggregators []*GatewayTabletStatusAggregator
	// gatewayStatsChanFull tracks the number of times
	// aggrChan becomes full.
	gatewayStatsChanFull *stats.Int
)

func init() {
	// init global goroutines to aggregate stats.
	aggrChan = make(chan *queryInfo, aggrChanSize)
	gatewayStatsChanFull = stats.NewInt("GatewayStatsChanFullCount")
	go resetAggregators()
	go processQueryInfo()
}

// registerAggregator registers an aggregator to the global list.
func registerAggregator(a *GatewayTabletStatusAggregator) {
	muAggr.Lock()
	defer muAggr.Unlock()
	aggregators = append(aggregators, a)
}

// resetAggregators resets the next stats slot for all aggregators every second.
func resetAggregators() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		muAggr.Lock()
		for _, a := range aggregators {
			a.resetNextSlot()
		}
		muAggr.Unlock()
	}
}

// processQueryInfo processes the next queryInfo object.
func processQueryInfo() {
	for qi := range aggrChan {
		qi.aggr.processQueryInfo(qi)
	}
}

// NewGatewayTabletStatusAggregator creates a GatewayTabletStatusAggregator.
func NewGatewayTabletStatusAggregator() *GatewayTabletStatusAggregator {
	gepsa := &GatewayTabletStatusAggregator{}
	registerAggregator(gepsa)
	return gepsa
}

// GatewayTabletStatusAggregator tracks tablet status for a gateway.
type GatewayTabletStatusAggregator struct {
	Keyspace   string
	Shard      string
	TabletType topodatapb.TabletType
	Name       string // the alternative name of a tablet
	Addr       string // the host:port of a tablet

	// mu protects below fields.
	mu         sync.RWMutex
	QueryCount uint64
	QueryError uint64
	// for QPS and latency (avg value over a minute)
	tick               uint32
	queryCountInMinute [60]uint64
	latencyInMinute    [60]time.Duration
}

type queryInfo struct {
	aggr       *GatewayTabletStatusAggregator
	addr       string
	tabletType topodatapb.TabletType
	elapsed    time.Duration
	hasError   bool
}

// UpdateQueryInfo updates the aggregator with the given information about a query.
func (gepsa *GatewayTabletStatusAggregator) UpdateQueryInfo(addr string, tabletType topodatapb.TabletType, elapsed time.Duration, hasError bool) {
	qi := &queryInfo{
		aggr:       gepsa,
		addr:       addr,
		tabletType: tabletType,
		elapsed:    elapsed,
		hasError:   hasError,
	}
	select {
	case aggrChan <- qi:
	default:
		gatewayStatsChanFull.Add(1)
	}
}

func (gepsa *GatewayTabletStatusAggregator) processQueryInfo(qi *queryInfo) {
	gepsa.mu.Lock()
	defer gepsa.mu.Unlock()
	if gepsa.TabletType != qi.tabletType {
		gepsa.TabletType = qi.tabletType
		// reset counters
		gepsa.QueryCount = 0
		gepsa.QueryError = 0
		for i := 0; i < len(gepsa.queryCountInMinute); i++ {
			gepsa.queryCountInMinute[i] = 0
		}
		for i := 0; i < len(gepsa.latencyInMinute); i++ {
			gepsa.latencyInMinute[i] = 0
		}
	}
	if qi.addr != "" {
		gepsa.Addr = qi.addr
	}
	gepsa.QueryCount++
	gepsa.queryCountInMinute[gepsa.tick]++
	gepsa.latencyInMinute[gepsa.tick] += qi.elapsed
	if qi.hasError {
		gepsa.QueryError++
	}
}

// GetCacheStatus returns a GatewayTabletCacheStatus representing the current gateway status.
func (gepsa *GatewayTabletStatusAggregator) GetCacheStatus() *GatewayTabletCacheStatus {
	status := &GatewayTabletCacheStatus{
		Keyspace: gepsa.Keyspace,
		Shard:    gepsa.Shard,
		Name:     gepsa.Name,
	}
	gepsa.mu.RLock()
	defer gepsa.mu.RUnlock()
	status.TabletType = gepsa.TabletType
	status.Addr = gepsa.Addr
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
		status.AvgLatency = float64(totalLatency.Nanoseconds()) / float64(totalQuery) / 1000000
	}
	return status
}

// resetNextSlot resets the next tracking slot.
func (gepsa *GatewayTabletStatusAggregator) resetNextSlot() {
	gepsa.mu.Lock()
	defer gepsa.mu.Unlock()
	gepsa.tick = (gepsa.tick + 1) % 60
	gepsa.queryCountInMinute[gepsa.tick] = 0
	gepsa.latencyInMinute[gepsa.tick] = time.Duration(0)
}
