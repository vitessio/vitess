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

// Package vtexplain analyzes a set of sql statements and returns the
// corresponding vtgate and vttablet query plans that will be executed
// on the given statements
package vtexplain

import (
	"fmt"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/gateway"

	"vitess.io/vitess/go/vt/vttablet/queryservice"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var (
	explainTopo    *ExplainTopo
	vtgateExecutor *vtgate.Executor
	healthCheck    *discovery.FakeHealthCheck

	vtgateSession = &vtgatepb.Session{
		TargetString: "",
		Autocommit:   true,
	}
)

func initVtgateExecutor(vSchemaStr string, opts *Options) error {
	explainTopo = &ExplainTopo{NumShards: opts.NumShards}
	healthCheck = discovery.NewFakeHealthCheck()

	resolver := newFakeResolver(opts, healthCheck, explainTopo, vtexplainCell)

	err := buildTopology(opts, vSchemaStr, opts.NumShards)
	if err != nil {
		return err
	}

	vtgateSession.TargetString = opts.Target

	streamSize := 10
	queryPlanCacheSize := int64(10)
	vtgateExecutor = vtgate.NewExecutor(context.Background(), explainTopo, vtexplainCell, "", resolver, opts.Normalize, streamSize, queryPlanCacheSize, false /* legacyAutocommit */)

	return nil
}

func newFakeResolver(opts *Options, hc discovery.HealthCheck, serv srvtopo.Server, cell string) *vtgate.Resolver {
	gw := gateway.GetCreator()(hc, serv, cell, 3)
	gw.WaitForTablets(context.Background(), []topodatapb.TabletType{topodatapb.TabletType_REPLICA})

	txMode := vtgatepb.TransactionMode_MULTI
	if opts.ExecutionMode == ModeTwoPC {
		txMode = vtgatepb.TransactionMode_TWOPC
	}
	tc := vtgate.NewTxConn(gw, txMode)
	sc := vtgate.NewScatterConn("", tc, gw, hc)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	return vtgate.NewResolver(srvResolver, serv, cell, sc)
}

func buildTopology(opts *Options, vschemaStr string, numShardsPerKeyspace int) error {
	explainTopo.Lock.Lock()
	defer explainTopo.Lock.Unlock()

	// We have to use proto's custom json loader so it can
	// handle string->enum conversion correctly.
	var srvVSchema vschemapb.SrvVSchema
	wrappedStr := fmt.Sprintf(`{"keyspaces": %s}`, vschemaStr)
	err := json2.Unmarshal([]byte(wrappedStr), &srvVSchema)
	if err != nil {
		return err
	}
	explainTopo.Keyspaces = srvVSchema.Keyspaces

	explainTopo.TabletConns = make(map[string]*explainTablet)
	for ks, vschema := range explainTopo.Keyspaces {
		numShards := 1
		if vschema.Sharded {
			numShards = numShardsPerKeyspace
		}
		for i := 0; i < numShards; i++ {
			kr, err := key.EvenShardsKeyRange(i, numShards)
			if err != nil {
				return err
			}
			shard := key.KeyRangeString(kr)
			hostname := fmt.Sprintf("%s/%s", ks, shard)
			log.Infof("registering test tablet %s for keyspace %s shard %s", hostname, ks, shard)

			tablet := healthCheck.AddFakeTablet(vtexplainCell, hostname, 1, ks, shard, topodatapb.TabletType_MASTER, true, 1, nil, func(t *topodatapb.Tablet) queryservice.QueryService {
				return newTablet(opts, t)
			})
			explainTopo.TabletConns[hostname] = tablet.(*explainTablet)
		}
	}

	return err
}

func vtgateExecute(sql string) ([]*engine.Plan, map[string]*TabletActions, error) {
	// use the plan cache to get the set of plans used for this query, then
	// clear afterwards for the next run
	planCache := vtgateExecutor.Plans()

	_, err := vtgateExecutor.Execute(context.Background(), "VtexplainExecute", vtgate.NewSafeSession(vtgateSession), sql, nil)
	if err != nil {
		for _, tc := range explainTopo.TabletConns {
			tc.tabletQueries = nil
			tc.mysqlQueries = nil
		}
		planCache.Clear()

		return nil, nil, fmt.Errorf("vtexplain execute error in '%s': %v", sql, err)
	}

	var plans []*engine.Plan
	for _, item := range planCache.Items() {
		plan := item.Value.(*engine.Plan)
		plan.ExecTime = 0
		plans = append(plans, plan)
	}
	planCache.Clear()

	tabletActions := make(map[string]*TabletActions)
	for shard, tc := range explainTopo.TabletConns {
		if len(tc.tabletQueries) == 0 {
			continue
		}

		tabletActions[shard] = &TabletActions{
			TabletQueries: tc.tabletQueries,
			MysqlQueries:  tc.mysqlQueries,
		}

		tc.tabletQueries = nil
		tc.mysqlQueries = nil
	}

	return plans, tabletActions, nil
}
