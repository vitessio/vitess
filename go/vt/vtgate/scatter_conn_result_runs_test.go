/*
Copyright 2026 The Vitess Authors.

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

package vtgate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

type (
	blockingGateway struct {
		srvtopo.Gateway
		release <-chan struct{}
	}

	releaseObserver chan struct{}
)

func (g *blockingGateway) Execute(
	ctx context.Context,
	session queryservice.Session,
	target *querypb.Target,
	query string,
	bindVariables map[string]*querypb.BindVariable,
	transactionID int64,
	reservedID int64,
	options *querypb.ExecuteOptions,
) (*sqltypes.Result, error) {
	select {
	case <-g.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.Gateway.Execute(ctx, session, target, query, bindVariables, transactionID, reservedID, options)
}

func (o releaseObserver) Observe(*sqltypes.Result) {
	select {
	case <-o:
	default:
		close(o)
	}
}

// TestExecuteMultiShardWithResultRuns proves successful shard results stay
// aligned with shard order while the flat result keeps completion order.
func TestExecuteMultiShardWithResultRuns(t *testing.T) {
	ctx := utils.LeakCheckContextTimeout(t, 30*time.Second)
	const (
		cell = "aa"
		ks   = "TestExecuteMultiShardWithResultRuns"
	)

	createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{cell}), cell)
	sbc0 := hc.AddTestTablet(cell, "-80", 1, ks, "-80", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc1 := hc.AddTestTablet(cell, "80-", 1, ks, "80-", topodatapb.TabletType_PRIMARY, true, 1, nil)
	run0 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "3")
	run1 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "2", "4")
	sbc0.SetResults([]*sqltypes.Result{run0})
	sbc1.SetResults([]*sqltypes.Result{run1})

	rss := []*srvtopo.ResolvedShard{
		{Target: &querypb.Target{Keyspace: ks, Shard: "-80", TabletType: topodatapb.TabletType_PRIMARY}, Gateway: sbc0},
		{Target: &querypb.Target{Keyspace: ks, Shard: "80-", TabletType: topodatapb.TabletType_PRIMARY}, Gateway: sbc1},
	}
	queries := []*querypb.BoundQuery{{Sql: "select id from user"}, {Sql: "select id from user"}}

	releaseFirst := make(releaseObserver)
	rss[0].Gateway = &blockingGateway{
		Gateway: sbc0,
		release: releaseFirst,
	}

	result, runs, errs := sc.executeMultiShardWithResultRuns(ctx, nil, rss, queries, econtext.NewSafeSession(nil), false, false, releaseFirst, false)

	require.Empty(t, errs)
	require.Equal(t, []sqltypes.Row{run1.Rows[0], run1.Rows[1], run0.Rows[0], run0.Rows[1]}, result.Rows)
	require.Len(t, runs, len(rss))
	require.Same(t, run0, runs[0])
	require.Same(t, run1, runs[1])

	rss[0].Gateway = sbc0
	sbc0.SetResults([]*sqltypes.Result{run0})
	sbc1.MustFailCodes[vtrpcpb.Code_INTERNAL] = 1
	result, runs, errs = sc.executeMultiShardWithResultRuns(ctx, nil, rss, queries, econtext.NewSafeSession(nil), false, false, nullResultsObserver{}, false)
	require.Len(t, errs, 1)
	require.Len(t, result.Rows, 2)
	require.Len(t, runs, len(rss))
	require.Same(t, run0, runs[0])
	require.Nil(t, runs[1])
}

// TestExecuteMultiShardWithResultRunsRejectsMismatchedInput proves shard and
// query lists must stay positionally aligned.
func TestExecuteMultiShardWithResultRunsRejectsMismatchedInput(t *testing.T) {
	rss := []*srvtopo.ResolvedShard{{}}

	result, runs, errs := new(ScatterConn).executeMultiShardWithResultRuns(
		t.Context(), nil, rss, nil, nil, false, false, nil, false,
	)
	require.Nil(t, result)
	require.Nil(t, runs)
	require.Len(t, errs, 1)
	require.EqualError(t, errs[0], "[BUG] got mismatched number of queries and shards")
}
