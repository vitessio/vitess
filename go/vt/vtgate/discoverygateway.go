// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"errors"
	"flag"
	"time"

	"golang.org/x/net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	pbq "github.com/youtube/vitess/go/vt/proto/query"
	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	cellsToWatch    = flag.String("cells-to-watch", "", "comma-separated list of cells for watching endpoints")
	refreshInterval = flag.Duration("endpoint-refresh-interval", 1*time.Minute, "endpoint refresh interval")
)

var errNotImplemented = errors.New("Not implemented")

const (
	gatewayImplementationDiscovery = "discoverygateway"
)

func init() {
	RegisterGatewayCreator(gatewayImplementationDiscovery, createDiscoveryGateway)
}

func createDiscoveryGateway(topoServer topo.Server, serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, connTimeoutTotal, connTimeoutPerConn, connLife time.Duration, connTimings *stats.MultiTimings) Gateway {
	return &discoveryGateway{
		topoServer:  topoServer,
		localCell:   cell,
		retryDelay:  retryDelay,
		connTimeout: connTimeoutTotal,
	}
}

type discoveryGateway struct {
	topoServer  topo.Server
	localCell   string
	retryDelay  time.Duration
	connTimeout time.Duration

	//epWatcher *discovery.EndPointWatcher
}

// InitializeConnections creates connections to VTTablets.
func (dg *discoveryGateway) InitializeConnections(ctx context.Context) error {
	//hc := discovery.NewHealthCheck(dg, dg.connTimeout, dg.retryDelay)
	//epWatcher := discovery.NewEndPointWatcher(dg.topoServer, hc, *cellsToWatch, *refreshInterval)
	//dg.epWatcher = epWatcher
	return nil
}

// Execute executes the non-streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Execute(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error) {
	return nil, errNotImplemented
}

// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) ExecuteBatch(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) (*tproto.QueryResultList, error) {
	return nil, errNotImplemented
}

// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) StreamExecute(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc) {
	return nil, func() error { return errNotImplemented }
}

// Begin starts a transaction for the specified keyspace, shard, and tablet type.
// It returns the transaction ID.
func (dg *discoveryGateway) Begin(ctx context.Context, keyspace string, shard string, tabletType pb.TabletType) (int64, error) {
	return 0, errNotImplemented
}

// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Commit(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, transactionID int64) error {
	return errNotImplemented
}

// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Rollback(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, transactionID int64) error {
	return errNotImplemented
}

// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) SplitQuery(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]tproto.QuerySplit, error) {
	return nil, errNotImplemented
}

// Close shuts down underlying connections.
func (dg *discoveryGateway) Close() error {
	//dg.epWatcher.Stop()
	return nil
}

func (dg *discoveryGateway) StatsUpdate(endPoint *pbt.EndPoint, cell string, target *pbq.Target, tabletExternallyReparentedTimestamp int64, stats *pbq.RealtimeStats) {
}
