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

package vtgate

import (
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

// This file uses the sandbox_test framework.

func TestExecuteFailOnAutocommit(t *testing.T) {

	createSandbox("TestExecuteFailOnAutocommit")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestExecuteFailOnAutocommit", "0", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestExecuteFailOnAutocommit", "1", topodatapb.TabletType_MASTER, true, 1, nil)

	rss := []*srvtopo.ResolvedShard{
		{
			Target: &querypb.Target{
				Keyspace:   "TestExecuteFailOnAutocommit",
				Shard:      "0",
				TabletType: topodatapb.TabletType_MASTER,
			},
			Gateway: sbc0,
		},
		{
			Target: &querypb.Target{
				Keyspace:   "TestExecuteFailOnAutocommit",
				Shard:      "1",
				TabletType: topodatapb.TabletType_MASTER,
			},
			Gateway: sbc1,
		},
	}
	queries := []*querypb.BoundQuery{
		{
			// This will fail to go to shard. It will be rejected at vtgate.
			Sql: "query1",
			BindVariables: map[string]*querypb.BindVariable{
				"bv0": sqltypes.Int64BindVariable(0),
			},
		},
		{
			// This will go to shard.
			Sql: "query2",
			BindVariables: map[string]*querypb.BindVariable{
				"bv1": sqltypes.Int64BindVariable(1),
			},
		},
	}
	// shard 0 - has transaction
	// shard 1 - does not have transaction.
	session := &vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{
			{
				Target:        &querypb.Target{Keyspace: "TestExecuteFailOnAutocommit", Shard: "0", TabletType: topodatapb.TabletType_MASTER, Cell: "aa"},
				TransactionId: 123,
				TabletAlias:   nil,
			},
		},
		Autocommit: false,
	}
	_, errs := sc.ExecuteMultiShard(ctx, rss, queries, NewSafeSession(session), true /*autocommit*/)
	err := vterrors.Aggregate(errs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "in autocommit mode, transactionID should be zero but was: 123")
	utils.MustMatch(t, 0, len(sbc0.Queries), "")
	utils.MustMatch(t, []*querypb.BoundQuery{queries[1]}, sbc1.Queries, "")
}
