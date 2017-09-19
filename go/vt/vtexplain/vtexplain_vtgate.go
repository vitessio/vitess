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
	"encoding/json"
	"fmt"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var (
	explainTopo    *ExplainTopo
	vtgateExecutor *vtgate.Executor
	healthCheck    *discovery.FakeHealthCheck

	vtgateSession = &vtgatepb.Session{
		TargetString: "@master",
		Autocommit:   true,
	}
)

func initVtgateExecutor(vSchemaStr string, opts *Options) error {
	explainTopo = &ExplainTopo{NumShards: opts.NumShards}
	healthCheck = discovery.NewFakeHealthCheck()

	resolver := newFakeResolver(healthCheck, explainTopo, vtexplainCell)

	err := buildTopology(vSchemaStr, opts.NumShards)
	if err != nil {
		return err
	}

	streamSize := 10
	vtgateExecutor = vtgate.NewExecutor(context.Background(), explainTopo, vtexplainCell, "", resolver, opts.Normalize, streamSize)

	return nil
}

func newFakeResolver(hc discovery.HealthCheck, serv topo.SrvTopoServer, cell string) *vtgate.Resolver {
	gw := gateway.GetCreator()(hc, topo.Server{}, serv, cell, 3)
	gw.WaitForTablets(context.Background(), []topodatapb.TabletType{topodatapb.TabletType_REPLICA})
	tc := vtgate.NewTxConn(gw, vtgatepb.TransactionMode_MULTI)
	sc := vtgate.NewScatterConn("", tc, gw)
	return vtgate.NewResolver(serv, cell, sc)
}

func buildTopology(vschemaStr string, numShardsPerKeyspace int) error {
	explainTopo.Lock.Lock()
	defer explainTopo.Lock.Unlock()

	explainTopo.Keyspaces = make(map[string]*vschemapb.Keyspace)
	err := json.Unmarshal([]byte(vschemaStr), &explainTopo.Keyspaces)
	if err != nil {
		return err
	}

	explainTopo.TabletConns = make(map[string]*sandboxconn.SandboxConn)
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
			sc := healthCheck.AddTestTablet(vtexplainCell, hostname, 1, ks, shard, topodatapb.TabletType_MASTER, true, 1, nil)

			tablet := newFakeTablet()
			sc.Executor = tablet

			explainTopo.TabletConns[hostname] = sc
		}
	}

	return err
}

func vtgateExecute(sql string) ([]*engine.Plan, map[string]*TabletActions, error) {
	_, err := vtgateExecutor.Execute(context.Background(), vtgateSession, sql, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("vtexplain execute error: %v in %s", err, sql)
	}

	// use the plan cache to get the set of plans used for this query, then
	// clear afterwards for the next run
	planCache := vtgateExecutor.Plans()
	var plans []*engine.Plan
	for _, item := range planCache.Items() {
		plans = append(plans, item.Value.(*engine.Plan))
	}
	planCache.Clear()

	tabletActions := make(map[string]*TabletActions)
	for shard, tc := range explainTopo.TabletConns {
		if len(tc.Queries) == 0 {
			continue
		}

		tablet := tc.Executor.(*fakeTablet)

		tqs := make([]*TabletQuery, 0, len(tc.Queries))
		for _, bq := range tc.Queries {
			tq := &TabletQuery{
				SQL:      bq.Sql,
				BindVars: sqltypes.CopyBindVariables(bq.BindVariables),
			}
			tqs = append(tqs, tq)
		}

		tabletActions[shard] = &TabletActions{
			TabletQueries: tqs,
			MysqlQueries:  tablet.queries,
		}

		tc.Queries = nil
		tablet.queries = nil
	}

	return plans, tabletActions, nil
}
