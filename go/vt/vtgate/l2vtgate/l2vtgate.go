/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package l2vtgate provides the core functionnality of a second-layer vtgate
// to route queries from an original vtgate to a subset of tablets.
package l2vtgate

import (
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"

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
	queryservice.QueryService
	timings              *stats.MultiTimings
	tabletCallErrorCount *stats.MultiCounters
	gateway              gateway.Gateway
}

// RegisterL2VTGate defines the type of registration mechanism.
type RegisterL2VTGate func(queryservice.QueryService)

// RegisterL2VTGates stores register funcs for L2VTGate server.
var RegisterL2VTGates []RegisterL2VTGate

// Init creates the single L2VTGate with the provided parameters.
func Init(hc discovery.HealthCheck, topoServer *topo.Server, serv topo.SrvTopoServer, statsName, cell string, retryCount int, tabletTypesToWait []topodatapb.TabletType) *L2VTGate {
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
	l2VTGate.QueryService = queryservice.Wrap(
		gw,
		func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, inTransaction bool, inner func(context.Context, *querypb.Target, queryservice.QueryService) (error, bool)) (err error) {
			if target != nil {
				startTime, statsKey := l2VTGate.startAction(name, target)
				defer l2VTGate.endAction(startTime, statsKey, &err)
			}
			err, _ = inner(ctx, target, conn)
			return err
		},
	)
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
		ec := vterrors.Code(*err)
		if ec != vtrpcpb.Code_ALREADY_EXISTS && ec != vtrpcpb.Code_INVALID_ARGUMENT {
			l.tabletCallErrorCount.Add(statsKey, 1)
		}
	}
	l.timings.Record(statsKey, startTime)
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (l *L2VTGate) GetGatewayCacheStatus() gateway.TabletCacheStatusList {
	return l.gateway.CacheStatus()
}
