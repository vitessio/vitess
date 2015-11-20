// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/discovery"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

const (
	gatewayImplementationShard = "shardgateway"
)

func init() {
	RegisterGatewayCreator(gatewayImplementationShard, createShardGateway)
}

func createShardGateway(hc discovery.HealthCheck, topoServer topo.Server, serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, connTimeoutTotal, connTimeoutPerConn, connLife time.Duration, connTimings *stats.MultiTimings) Gateway {
	return &shardGateway{
		toposerv:           serv,
		cell:               cell,
		retryDelay:         retryDelay,
		retryCount:         retryCount,
		connTimeoutTotal:   connTimeoutTotal,
		connTimeoutPerConn: connTimeoutPerConn,
		connLife:           connLife,
		connTimings:        connTimings,
		shardConns:         make(map[string]*ShardConn),
	}
}

// A Gateway is the query processing module for each shard.
type shardGateway struct {
	toposerv           SrvTopoServer
	cell               string
	retryDelay         time.Duration
	retryCount         int
	connTimeoutTotal   time.Duration
	connTimeoutPerConn time.Duration
	connLife           time.Duration
	connTimings        *stats.MultiTimings

	mu         sync.Mutex
	shardConns map[string]*ShardConn
}

// InitializeConnections pre-initializes connections for all shards.
// It also populates topology cache by accessing it.
// It is not necessary to call this function before serving queries,
// but it would reduce connection overhead when serving.
func (sg *shardGateway) InitializeConnections(ctx context.Context) error {
	ksNames, err := sg.toposerv.GetSrvKeyspaceNames(ctx, sg.cell)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var errRecorder concurrency.AllErrorRecorder
	for _, ksName := range ksNames {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			// get SrvKeyspace for cell/keyspace
			ks, err := sg.toposerv.GetSrvKeyspace(ctx, sg.cell, keyspace)
			if err != nil {
				errRecorder.RecordError(err)
				return
			}
			// work on all shards of all serving tablet types
			for _, ksPartition := range ks.Partitions {
				tt := ksPartition.ServedType
				for _, shard := range ksPartition.ShardReferences {
					wg.Add(1)
					go func(shardName string, tabletType topodatapb.TabletType) {
						defer wg.Done()
						err = sg.getConnection(ctx, keyspace, shardName, tabletType).Dial(ctx)
						if err != nil {
							errRecorder.RecordError(err)
							return
						}
					}(shard.Name, tt)
				}
			}
		}(ksName)
	}
	wg.Wait()
	if errRecorder.HasErrors() {
		return errRecorder.AggrError(AggregateVtGateErrors)
	}
	return nil
}

// Execute executes the non-streaming query for the specified keyspace, shard, and tablet type.
func (sg *shardGateway) Execute(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	return sg.getConnection(ctx, keyspace, shard, tabletType).Execute(ctx, query, bindVars, transactionID)
}

// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
func (sg *shardGateway) ExecuteBatch(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	return sg.getConnection(ctx, keyspace, shard, tabletType).ExecuteBatch(ctx, queries, asTransaction, transactionID)
}

// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
func (sg *shardGateway) StreamExecute(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc) {
	return sg.getConnection(ctx, keyspace, shard, tabletType).StreamExecute(ctx, query, bindVars, transactionID)
}

// Begin starts a transaction for the specified keyspace, shard, and tablet type.
// It returns the transaction ID.
func (sg *shardGateway) Begin(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType) (int64, error) {
	return sg.getConnection(ctx, keyspace, shard, tabletType).Begin(ctx)
}

// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
func (sg *shardGateway) Commit(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType, transactionID int64) error {
	return sg.getConnection(ctx, keyspace, shard, tabletType).Commit(ctx, transactionID)
}

// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (sg *shardGateway) Rollback(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType, transactionID int64) error {
	return sg.getConnection(ctx, keyspace, shard, tabletType).Rollback(ctx, transactionID)
}

// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
func (sg *shardGateway) SplitQuery(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType, sql string, bindVars map[string]interface{}, splitColumn string, splitCount int) ([]tproto.QuerySplit, error) {
	return sg.getConnection(ctx, keyspace, shard, tabletType).SplitQuery(ctx, sql, bindVars, splitColumn, splitCount)
}

// Close shuts down the underlying connections.
func (sg *shardGateway) Close(ctx context.Context) error {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	for _, v := range sg.shardConns {
		v.Close()
	}
	sg.shardConns = make(map[string]*ShardConn)
	return nil
}

func (sg *shardGateway) getConnection(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType) *ShardConn {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", keyspace, shard, strings.ToLower(tabletType.String()))
	sdc, ok := sg.shardConns[key]
	if !ok {
		sdc = NewShardConn(ctx, sg.toposerv, sg.cell, keyspace, shard, tabletType, sg.retryDelay, sg.retryCount, sg.connTimeoutTotal, sg.connTimeoutPerConn, sg.connLife, sg.connTimings)
		sg.shardConns[key] = sdc
	}
	return sdc
}
