// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package l2vtgate provides the core functionnality of a second-layer vtgate
// to route queries from an original vtgate to a subset of tablets.
package l2vtgate

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	l2VTGate *L2VTGate
)

// L2VTGate implements queryservice.QueryService and forwards queries to
// the underlying gateway.
type L2VTGate struct {
	timings              *stats.MultiTimings
	tabletCallErrorCount *stats.MultiCounters
	gateway              gateway.Gateway
}

// RegisterL2VTGate defines the type of registration mechanism.
type RegisterL2VTGate func(queryservice.QueryService)

// RegisterL2VTGates stores register funcs for L2VTGate server.
var RegisterL2VTGates []RegisterL2VTGate

// Init creates the single L2VTGate with the provided parameters.
func Init(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, statsName, cell string, retryCount int, tabletTypesToWait []topodatapb.TabletType) *L2VTGate {
	if l2VTGate != nil {
		log.Fatalf("L2VTGate already initialized")
	}

	tabletCallErrorCountStatsName := ""
	if statsName != "" {
		tabletCallErrorCountStatsName = statsName + "ErrorCount"
	}

	gw := gateway.GetCreator()(hc, topoServer, serv, cell, retryCount)
	gateway.WaitForTablets(gw, tabletTypesToWait)
	l2VTGate = &L2VTGate{
		timings:              stats.NewMultiTimings(statsName, []string{"Operation", "Keyspace", "ShardName", "DbType"}),
		tabletCallErrorCount: stats.NewMultiCounters(tabletCallErrorCountStatsName, []string{"Operation", "Keyspace", "ShardName", "DbType"}),
		gateway:              gw,
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

func (l *L2VTGate) startAction(name string, target *querypb.Target) (time.Time, []string) {
	statsKey := []string{name, target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType)}
	startTime := time.Now()
	return startTime, statsKey
}

func (l *L2VTGate) endAction(startTime time.Time, statsKey []string, err *error) {
	if *err != nil {
		// Don't increment the error counter for duplicate
		// keys or bad queries, as those errors are caused by
		// client queries and are not VTGate's fault.
		ec := vterrors.RecoverVtErrorCode(*err)
		if ec != vtrpcpb.ErrorCode_INTEGRITY_ERROR && ec != vtrpcpb.ErrorCode_BAD_INPUT {
			l.tabletCallErrorCount.Add(statsKey, 1)
		}
	}
	l.timings.Record(statsKey, startTime)
}

// Begin is part of the queryservice.QueryService interface
func (l *L2VTGate) Begin(ctx context.Context, target *querypb.Target) (int64, error) {
	return l.gateway.Begin(ctx, target)
}

// Commit is part of the queryservice.QueryService interface
func (l *L2VTGate) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return l.gateway.Commit(ctx, target, transactionID)
}

// Rollback is part of the queryservice.QueryService interface
func (l *L2VTGate) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return l.gateway.Rollback(ctx, target, transactionID)
}

// Prepare is part of the queryservice.QueryService interface
func (l *L2VTGate) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return l.gateway.Prepare(ctx, target, transactionID, dtid)
}

// CommitPrepared is part of the queryservice.QueryService interface
func (l *L2VTGate) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return l.gateway.CommitPrepared(ctx, target, dtid)
}

// RollbackPrepared is part of the queryservice.QueryService interface
func (l *L2VTGate) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	return l.gateway.RollbackPrepared(ctx, target, dtid, originalID)
}

// CreateTransaction is part of the queryservice.QueryService interface
func (l *L2VTGate) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	return l.gateway.CreateTransaction(ctx, target, dtid, participants)
}

// StartCommit is part of the queryservice.QueryService interface
func (l *L2VTGate) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return l.gateway.StartCommit(ctx, target, transactionID, dtid)
}

// SetRollback is part of the queryservice.QueryService interface
func (l *L2VTGate) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	return l.gateway.SetRollback(ctx, target, dtid, transactionID)
}

// ConcludeTransaction is part of the queryservice.QueryService interface
func (l *L2VTGate) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return l.gateway.ConcludeTransaction(ctx, target, dtid)
}

// ReadTransaction is part of the queryservice.QueryService interface
func (l *L2VTGate) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	return l.gateway.ReadTransaction(ctx, target, dtid)
}

// Execute is part of the queryservice.QueryService interface
func (l *L2VTGate) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (result *sqltypes.Result, err error) {
	startTime, statsKey := l.startAction("Execute", target)
	defer l.endAction(startTime, statsKey, &err)

	return l.gateway.Execute(ctx, target, sql, bindVariables, transactionID, options)
}

// StreamExecute is part of the queryservice.QueryService interface
func (l *L2VTGate) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (err error) {
	startTime, statsKey := l.startAction("StreamExecute", target)
	defer l.endAction(startTime, statsKey, &err)

	return l.gateway.StreamExecute(ctx, target, sql, bindVariables, options, callback)
}

// ExecuteBatch is part of the queryservice.QueryService interface
func (l *L2VTGate) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) (results []sqltypes.Result, err error) {
	startTime, statsKey := l.startAction("ExecuteBatch", target)
	defer l.endAction(startTime, statsKey, &err)

	return l.gateway.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
}

// BeginExecute is part of the queryservice.QueryService interface
func (l *L2VTGate) BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions) (result *sqltypes.Result, transactionID int64, err error) {
	startTime, statsKey := l.startAction("Execute", target)
	defer l.endAction(startTime, statsKey, &err)

	return l.gateway.BeginExecute(ctx, target, sql, bindVariables, options)
}

// BeginExecuteBatch is part of the queryservice.QueryService interface
func (l *L2VTGate) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) (results []sqltypes.Result, transactionID int64, err error) {
	startTime, statsKey := l.startAction("ExecuteBatch", target)
	defer l.endAction(startTime, statsKey, &err)

	return l.gateway.BeginExecuteBatch(ctx, target, queries, asTransaction, options)
}

// MessageStream is part of the queryservice.QueryService interface
func (l *L2VTGate) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) (err error) {
	startTime, statsKey := l.startAction("ExecuteBatch", target)
	defer l.endAction(startTime, statsKey, &err)

	return l.gateway.MessageStream(ctx, target, name, callback)
}

// MessageAck is part of the queryservice.QueryService interface
func (l *L2VTGate) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	startTime, statsKey := l.startAction("ExecuteBatch", target)
	defer l.endAction(startTime, statsKey, &err)

	return l.gateway.MessageAck(ctx, target, name, ids)
}

// SplitQuery is part of the queryservice.QueryService interface
func (l *L2VTGate) SplitQuery(ctx context.Context, target *querypb.Target, query querytypes.BoundQuery, splitColumns []string, splitCount int64, numRowsPerQueryPart int64, algorithm querypb.SplitQueryRequest_Algorithm) (splits []querytypes.QuerySplit, err error) {
	startTime, statsKey := l.startAction("SplitQuery", target)
	defer l.endAction(startTime, statsKey, &err)

	return l.gateway.SplitQuery(ctx, target, query, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
}

// StreamHealth is part of the queryservice.QueryService interface
func (l *L2VTGate) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return fmt.Errorf("L2VTGate does not provide health status")
}

// UpdateStream is part of the queryservice.QueryService interface
func (l *L2VTGate) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error {
	err := l.gateway.UpdateStream(ctx, target, position, timestamp, callback)
	return err
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
