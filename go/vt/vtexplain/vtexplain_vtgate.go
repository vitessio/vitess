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
	"path"
	"sort"
	"strings"

	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func (vte *VTExplain) initVtgateExecutor(ctx context.Context, ts *topo.Server, vSchemaStr, ksShardMapStr string, opts *Options, srvTopoCounts *stats.CountersWithSingleLabel) error {
	vte.explainTopo = &ExplainTopo{NumShards: opts.NumShards}
	vte.explainTopo.TopoServer = ts
	vte.healthCheck = discovery.NewFakeHealthCheck(nil)

	resolver := vte.newFakeResolver(ctx, opts, vte.explainTopo, Cell)

	err := vte.buildTopology(ctx, ts, opts, vSchemaStr, ksShardMapStr, opts.NumShards, srvTopoCounts)
	if err != nil {
		return err
	}

	vte.vtgateSession.TargetString = opts.Target

	if opts.PlannerVersion != querypb.ExecuteOptions_DEFAULT_PLANNER {
		if vte.vtgateSession.Options == nil {
			vte.vtgateSession.Options = &querypb.ExecuteOptions{}
		}
		vte.vtgateSession.Options.PlannerVersion = opts.PlannerVersion
	}

	streamSize := 10
	var schemaTracker vtgate.SchemaInfo // no schema tracker for these tests
	queryLogBufferSize := 10
	plans := theine.NewStore[vtgate.PlanCacheKey, *engine.Plan](4*1024*1024, false)
	vte.vtgateExecutor = vtgate.NewExecutor(ctx, vte.env, vte.explainTopo, Cell, resolver, opts.Normalize, false, streamSize, plans, schemaTracker, false, opts.PlannerVersion, 0)
	vte.vtgateExecutor.SetQueryLogger(streamlog.New[*logstats.LogStats]("VTGate", queryLogBufferSize))

	return nil
}

func (vte *VTExplain) newFakeResolver(ctx context.Context, opts *Options, serv srvtopo.Server, cell string) *vtgate.Resolver {
	gw := vtgate.NewTabletGateway(ctx, vte.healthCheck, serv, cell)
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

func (vte *VTExplain) buildTopology(ctx context.Context, ts *topo.Server, opts *Options, vschemaStr string, ksShardMapStr string, numShardsPerKeyspace int, srvTopoCounts *stats.CountersWithSingleLabel) error {
	vte.explainTopo.Lock.Lock()
	defer vte.explainTopo.Lock.Unlock()

	// We have to use proto's custom json loader so it can
	// handle string->enum conversion correctly.
	var srvVSchema vschemapb.SrvVSchema
	wrappedStr := fmt.Sprintf(`{"keyspaces": %s}`, vschemaStr)
	err := json2.Unmarshal([]byte(wrappedStr), &srvVSchema)
	if err != nil {
		return err
	}
	schema := vindexes.BuildVSchema(&srvVSchema, vte.env.Parser())
	for ks, ksSchema := range schema.Keyspaces {
		if ksSchema.Error != nil {
			return vterrors.Wrapf(ksSchema.Error, "vschema failed to load on keyspace [%s]", ks)
		}
	}
	vte.explainTopo.Keyspaces = srvVSchema.Keyspaces

	ksShardMap, err := getKeyspaceShardMap(ksShardMapStr)
	if err != nil {
		return err
	}

	conn, err := ts.ConnForCell(ctx, Cell)
	if err != nil {
		return err
	}
	vte.explainTopo.TabletConns = make(map[string]*explainTablet)
	vte.explainTopo.KeyspaceShards = make(map[string]map[string]*topodatapb.ShardReference)
	for ks, vschema := range vte.explainTopo.Keyspaces {
		shards, err := getShardRanges(ks, vschema, ksShardMap, numShardsPerKeyspace)
		if err != nil {
			return err
		}

		vte.explainTopo.KeyspaceShards[ks] = make(map[string]*topodatapb.ShardReference)

		srvPath := path.Join(topo.KeyspacesPath, ks, topo.SrvKeyspaceFile)
		srvKeyspace := &topodatapb.SrvKeyspace{
			Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
				{
					ServedType:      topodatapb.TabletType_PRIMARY,
					ShardReferences: shards,
				},
				{
					ServedType:      topodatapb.TabletType_REPLICA,
					ShardReferences: shards,
				},
				{
					ServedType:      topodatapb.TabletType_RDONLY,
					ShardReferences: shards,
				},
			},
		}
		data, err := srvKeyspace.MarshalVT()
		if err != nil {
			return err
		}
		_, err = conn.Update(ctx, srvPath, data, nil)
		if err != nil {
			return err
		}

		for _, shard := range shards {
			// If the topology is in the middle of a reshard, there can be two shards covering the same key range (e.g.
			// both source shard 80- and target shard 80-c0 cover the keyrange 80-c0). For the purposes of explain, we
			// should only consider the one that is serving, hence we skip the ones not serving. Otherwise, vtexplain
			// gives inconsistent results - sometimes it will route the query being explained to the source shard, and
			// sometimes to the destination shard. See https://github.com/vitessio/vitess/issues/11632 .
			if shardInfo, ok := ksShardMap[ks][shard.Name]; ok && !shardInfo.IsPrimaryServing {
				continue
			}
			hostname := fmt.Sprintf("%s/%s", ks, shard.Name)
			log.Infof("registering test tablet %s for keyspace %s shard %s", hostname, ks, shard.Name)

			tablet := vte.healthCheck.AddFakeTablet(Cell, hostname, 1, ks, shard.Name, topodatapb.TabletType_PRIMARY, true, 1, nil, func(t *topodatapb.Tablet) queryservice.QueryService {
				return vte.newTablet(ctx, vte.env, opts, t, ts, srvTopoCounts)
			})
			vte.explainTopo.TabletConns[hostname] = tablet.(*explainTablet)
			vte.explainTopo.KeyspaceShards[ks][shard.Name] = shard
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

func (vte *VTExplain) vtgateExecute(sql string) ([]*engine.Plan, map[string]*TabletActions, error) {
	// This method will sort the shard session lexicographically.
	// This will ensure that the commit/rollback order is predictable.
	vte.sortShardSession()

	_, err := vte.vtgateExecutor.Execute(context.Background(), nil, "VtexplainExecute", vtgate.NewSafeSession(vte.vtgateSession), sql, nil)
	if err != nil {
		for _, tc := range vte.explainTopo.TabletConns {
			tc.tabletQueries = nil
			tc.mysqlQueries = nil
		}
		vte.vtgateExecutor.ClearPlans()
		return nil, nil, vterrors.Wrapf(err, "vtexplain execute error in '%s'", sql)
	}

	var plans []*engine.Plan

	// use the plan cache to get the set of plans used for this query, then
	// clear afterwards for the next run
	vte.vtgateExecutor.ForEachPlan(func(plan *engine.Plan) bool {
		plan.ExecTime = 0
		plans = append(plans, plan)
		return true
	})

	vte.vtgateExecutor.ClearPlans()

	tabletActions := make(map[string]*TabletActions)
	for shard, tc := range vte.explainTopo.TabletConns {
		if len(tc.tabletQueries) == 0 {
			continue
		}

		func() {
			tc.mu.Lock()
			defer tc.mu.Unlock()

			tabletActions[shard] = &TabletActions{
				TabletQueries: tc.tabletQueries,
				MysqlQueries:  tc.mysqlQueries,
			}

			tc.tabletQueries = nil
			tc.mysqlQueries = nil
		}()
	}

	return plans, tabletActions, nil
}

func (vte *VTExplain) sortShardSession() {
	ss := vte.vtgateSession.ShardSessions

	sort.Slice(ss, func(i, j int) bool {
		if ss[i].Target.Keyspace != ss[j].Target.Keyspace {
			return strings.Compare(ss[i].Target.Keyspace, ss[j].Target.Keyspace) <= 0
		}
		return strings.Compare(ss[i].Target.Shard, ss[j].Target.Shard) <= 0
	})
}
