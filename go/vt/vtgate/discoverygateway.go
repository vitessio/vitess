// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"errors"
	"flag"
	"strings"
	"time"

	"golang.org/x/net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/discovery"
	pbq "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	cellsToWatch        = flag.String("cells_to_watch", "", "comma-separated list of cells for watching endpoints")
	refreshInterval     = flag.Duration("endpoint_refresh_interval", 1*time.Minute, "endpoint refresh interval")
	topoReadConcurrency = flag.Int("topo_read_concurrency", 32, "concurrent topo reads")
)

var errNotImplemented = errors.New("Not implemented")

const (
	gatewayImplementationDiscovery = "discoverygateway"
)

func init() {
	RegisterGatewayCreator(gatewayImplementationDiscovery, createDiscoveryGateway)
}

func createDiscoveryGateway(hc discovery.HealthCheck, topoServer topo.Server, serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, connTimeoutTotal, connTimeoutPerConn, connLife time.Duration, connTimings *stats.MultiTimings) Gateway {
	return &discoveryGateway{
		hc:              hc,
		topoServer:      topoServer,
		localCell:       cell,
		tabletsWatchers: make([]*discovery.CellTabletsWatcher, 0, 1),
	}
}

type discoveryGateway struct {
	hc         discovery.HealthCheck
	topoServer topo.Server
	localCell  string

	tabletsWatchers []*discovery.CellTabletsWatcher
}

// InitializeConnections creates connections to VTTablets.
func (dg *discoveryGateway) InitializeConnections(ctx context.Context) error {
	dg.hc.SetListener(dg)
	for _, cell := range strings.Split(*cellsToWatch, ",") {
		ctw := discovery.NewCellTabletsWatcher(dg.topoServer, dg.hc, cell, *refreshInterval, *topoReadConcurrency)
		dg.tabletsWatchers = append(dg.tabletsWatchers, ctw)
	}
	return nil
}

// Execute executes the non-streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Execute(ctx context.Context, keyspace, shard string, tabletType pbt.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error) {
	return nil, errNotImplemented
}

// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) ExecuteBatch(ctx context.Context, keyspace, shard string, tabletType pbt.TabletType, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) (*tproto.QueryResultList, error) {
	return nil, errNotImplemented
}

// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) StreamExecute(ctx context.Context, keyspace, shard string, tabletType pbt.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc) {
	return nil, func() error { return errNotImplemented }
}

// Begin starts a transaction for the specified keyspace, shard, and tablet type.
// It returns the transaction ID.
func (dg *discoveryGateway) Begin(ctx context.Context, keyspace string, shard string, tabletType pbt.TabletType) (int64, error) {
	return 0, errNotImplemented
}

// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Commit(ctx context.Context, keyspace, shard string, tabletType pbt.TabletType, transactionID int64) error {
	return errNotImplemented
}

// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Rollback(ctx context.Context, keyspace, shard string, tabletType pbt.TabletType, transactionID int64) error {
	return errNotImplemented
}

// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) SplitQuery(ctx context.Context, keyspace, shard string, tabletType pbt.TabletType, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]tproto.QuerySplit, error) {
	return nil, errNotImplemented
}

// Close shuts down underlying connections.
func (dg *discoveryGateway) Close(ctx context.Context) error {
	for _, ctw := range dg.tabletsWatchers {
		ctw.Stop()
	}
	return nil
}

// StatsUpdate receives updates about target and realtime stats changes.
func (dg *discoveryGateway) StatsUpdate(endPoint *pbt.EndPoint, cell string, target *pbq.Target, tabletExternallyReparentedTimestamp int64, stats *pbq.RealtimeStats) {
}
