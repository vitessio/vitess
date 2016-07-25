// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package l2vtgate provides the core functionnality of a second-layer vtgate
// to route queries from an original vtgate to a subset of tablets.
package l2vtgate

import (
	"fmt"
	"io"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	l2VTGate *L2VTGate
)

// L2VTGate implements queryservice.QueryService and forwards queries to
// the underlying gateway.
type L2VTGate struct {
	gateway gateway.Gateway
}

// RegisterL2VTGate defines the type of registration mechanism.
type RegisterL2VTGate func(queryservice.QueryService)

// RegisterL2VTGates stores register funcs for L2VTGate server.
var RegisterL2VTGates []RegisterL2VTGate

// Init creates the single L2VTGate with the provided parameters.
func Init(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, cell string, retryCount int, tabletTypesToWait []topodatapb.TabletType) *L2VTGate {
	if l2VTGate != nil {
		log.Fatalf("L2VTGate already initialized")
	}

	gateway := gateway.GetCreator()(hc, topoServer, serv, cell, retryCount, tabletTypesToWait)
	l2VTGate = &L2VTGate{
		gateway: gateway,
	}
	servenv.OnRun(func() {
		for _, f := range RegisterL2VTGates {
			f(l2VTGate)
		}
	})
	return l2VTGate
}

// Gateway returns this l2vtgate Gateway object (for tests mainly).
func (l *L2VTGate) Gateway() gateway.Gateway {
	return l.gateway
}

// Begin is part of the queryservice.QueryService interface
func (l *L2VTGate) Begin(ctx context.Context, target *querypb.Target) (int64, error) {
	return l.gateway.Begin(ctx, target.Keyspace, target.Shard, target.TabletType)
}

// Commit is part of the queryservice.QueryService interface
func (l *L2VTGate) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return l.gateway.Commit(ctx, target.Keyspace, target.Shard, target.TabletType, transactionID)
}

// Rollback is part of the queryservice.QueryService interface
func (l *L2VTGate) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return l.gateway.Rollback(ctx, target.Keyspace, target.Shard, target.TabletType, transactionID)
}

// Execute is part of the queryservice.QueryService interface
func (l *L2VTGate) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	return l.gateway.Execute(ctx, target.Keyspace, target.Shard, target.TabletType, sql, bindVariables, transactionID)
}

// StreamExecute is part of the queryservice.QueryService interface
func (l *L2VTGate) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sendReply func(*sqltypes.Result) error) error {
	stream, err := l.gateway.StreamExecute(ctx, target.Keyspace, target.Shard, target.TabletType, sql, bindVariables)
	if err != nil {
		return err
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := sendReply(r); err != nil {
			return err
		}
	}
}

// ExecuteBatch is part of the queryservice.QueryService interface
func (l *L2VTGate) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	return l.gateway.ExecuteBatch(ctx, target.Keyspace, target.Shard, target.TabletType, queries, asTransaction, transactionID)
}

// SplitQuery is part of the queryservice.QueryService interface
func (l *L2VTGate) SplitQuery(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	return l.gateway.SplitQuery(ctx, target.Keyspace, target.Shard, target.TabletType, sql, bindVariables, splitColumn, splitCount)
}

// SplitQueryV2 is part of the queryservice.QueryService interface
func (l *L2VTGate) SplitQueryV2(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, splitColumns []string, splitCount int64, numRowsPerQueryPart int64, algorithm querypb.SplitQueryRequest_Algorithm) ([]querytypes.QuerySplit, error) {
	return l.gateway.SplitQueryV2(ctx, target.Keyspace, target.Shard, target.TabletType, sql, bindVariables, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
}

// StreamHealthRegister is part of the queryservice.QueryService interface
func (l *L2VTGate) StreamHealthRegister(chan<- *querypb.StreamHealthResponse) (int, error) {
	return -1, fmt.Errorf("L2VTGate does not provide health status")
}

// StreamHealthUnregister is part of the queryservice.QueryService interface
func (l *L2VTGate) StreamHealthUnregister(int) error {
	return fmt.Errorf("L2VTGate does not provide health status")
}

// HandlePanic is part of the queryservice.QueryService interface
func (l *L2VTGate) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (l *L2VTGate) GetGatewayCacheStatus() gateway.TabletCacheStatusList {
	return l.gateway.CacheStatus()
}
