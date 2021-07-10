/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/engine"
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

func initVtgateExecutor(vSchemaStr, ksShardMapStr string, opts *Options) error {
	explainTopo = &ExplainTopo{NumShards: opts.NumShards}
	explainTopo.TopoServer = memorytopo.NewServer(vtexplainCell)
	healthCheck = discovery.NewFakeHealthCheck()

	resolver := newFakeResolver(opts, explainTopo, vtexplainCell)

	err := buildTopology(opts, vSchemaStr, ksShardMapStr, opts.NumShards)
	if err != nil {
		return err
	}

	vtgateSession.TargetString = opts.Target

	streamSize := 10
	vtgateExecutor = vtgate.NewExecutor(context.Background(), explainTopo, vtexplainCell, resolver, opts.Normalize, false /*do not warn for sharded only*/, streamSize, cache.DefaultConfig)

	return nil
}

func newFakeResolver(opts *Options, serv srvtopo.Server, cell string) *vtgate.Resolver {
	ctx := context.Background()
	gw := vtgate.NewTabletGateway(ctx, healthCheck, serv, cell)
	_ = gw.WaitForTablets(ctx, []topodatapb.TabletType{topodatapb.TabletType_REPLICA})

	txMode := vtgatepb.TransactionMode_MULTI
	if opts.ExecutionMode == ModeTwoPC {
		txMode = vtgatepb.TransactionMode_TWOPC
	}
	tc := vtgate.NewTxConn(gw, txMode)
	sc := vtgate.NewScatterConn("", tc, gw)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	return vtgate.NewResolver(srvResolver, serv, cell, sc)
}

func buildTopology(opts *Options, vschemaStr string, ksShardMapStr string, numShardsPerKeyspace int) error {
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

	ksShardMap, err := getKeyspaceShardMap(ksShardMapStr)
	if err != nil {
		return err
	}

	explainTopo.TabletConns = make(map[string]*explainTablet)
	explainTopo.KeyspaceShards = make(map[string]map[string]*topodatapb.ShardReference)
	for ks, vschema := range explainTopo.Keyspaces {
		shards, err := getShardRanges(ks, vschema, ksShardMap, numShardsPerKeyspace)
		if err != nil {
			return err
		}

		explainTopo.KeyspaceShards[ks] = make(map[string]*topodatapb.ShardReference)

		for _, shard := range shards {
			hostname := fmt.Sprintf("%s/%s", ks, shard.Name)
			log.Infof("registering test tablet %s for keyspace %s shard %s", hostname, ks, shard.Name)

			tablet := healthCheck.AddFakeTablet(vtexplainCell, hostname, 1, ks, shard.Name, topodatapb.TabletType_MASTER, true, 1, nil, func(t *topodatapb.Tablet) queryservice.QueryService {
				return newTablet(opts, t)
			})
			explainTopo.TabletConns[hostname] = tablet.(*explainTablet)
			explainTopo.KeyspaceShards[ks][shard.Name] = shard
		}
	}

	return err
}

func getKeyspaceShardMap(ksShardMapStr string) (map[string]map[string]*topo.ShardInfo, error) {
	if ksShardMapStr == "" {
		return map[string]map[string]*topo.ShardInfo{}, nil
	}

	// keyspace-name -> shard-name -> ShardInfo
	var ksShardMap map[string]map[string]*topo.ShardInfo
	err := json2.Unmarshal([]byte(ksShardMapStr), &ksShardMap)

	return ksShardMap, err
}

func getShardRanges(ks string, vschema *vschemapb.Keyspace, ksShardMap map[string]map[string]*topo.ShardInfo, numShardsPerKeyspace int) ([]*topodatapb.ShardReference, error) {
	shardMap, ok := ksShardMap[ks]
	if ok {
		shards := make([]*topodatapb.ShardReference, 0, len(shardMap))
		for shard, info := range shardMap {
			ref := &topodatapb.ShardReference{
				Name:     shard,
				KeyRange: info.KeyRange,
			}

			shards = append(shards, ref)
		}
		return shards, nil

	}

	numShards := 1
	if vschema.Sharded {
		numShards = numShardsPerKeyspace
	}

	shards := make([]*topodatapb.ShardReference, numShards)

	for i := 0; i < numShards; i++ {
		kr, err := key.EvenShardsKeyRange(i, numShards)
		if err != nil {
			return nil, err
		}

		shards[i] = &topodatapb.ShardReference{
			Name:     key.KeyRangeString(kr),
			KeyRange: kr,
		}
	}

	return shards, nil
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

		return nil, nil, vterrors.Wrapf(err, "vtexplain execute error in '%s'", sql)
	}

	var plans []*engine.Plan
	planCache.ForEach(func(value interface{}) bool {
		plan := value.(*engine.Plan)
		plan.ExecTime = 0
		plans = append(plans, plan)
		return true
	})
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
