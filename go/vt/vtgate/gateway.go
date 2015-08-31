// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
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
	Execute(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error)

	// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
	ExecuteBatch(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) (*tproto.QueryResultList, error)

	// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
	StreamExecute(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc)

	// Begin starts a transaction for the specified keyspace, shard, and tablet type.
	// It returns the transaction ID.
	Begin(ctx context.Context, keyspace string, shard string, tabletType pb.TabletType) (int64, error)

	// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
	Commit(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, transactionID int64) error

	// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
	Rollback(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, transactionID int64) error

	// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
	SplitQuery(ctx context.Context, keyspace, shard string, tabletType pb.TabletType, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]tproto.QuerySplit, error)

	// Close shuts down underlying connections.
	Close() error
}

// GatewayCreator is the func which can create the actual gateway object.
type GatewayCreator func(serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, connTimeoutTotal, connTimeoutPerConn, connLife time.Duration, connTimings *stats.MultiTimings) Gateway

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
