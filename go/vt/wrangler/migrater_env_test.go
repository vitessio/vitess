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

package wrangler

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

const vreplQueryks = "select id, source from _vt.vreplication where workflow='test' and db_name='vt_ks'"
const vreplQueryks2 = "select id, source from _vt.vreplication where workflow='test' and db_name='vt_ks2'"

type testMigraterEnv struct {
	ts              *topo.Server
	wr              *Wrangler
	sourceMasters   []*fakeTablet
	targetMasters   []*fakeTablet
	dbSourceClients []*fakeDBClient
	dbTargetClients []*fakeDBClient
	allDBClients    []*fakeDBClient
	targetKeyspace  string
}

func newTestTableMigrater(ctx context.Context, t *testing.T) *testMigraterEnv {
	tme := &testMigraterEnv{}
	tme.ts = memorytopo.NewServer("cell1", "cell2")
	tme.wr = New(logutil.NewConsoleLogger(), tme.ts, tmclient.NewTabletManagerClient())

	sourceShards := []string{"-40", "40-"}
	targetShards := []string{"-80", "80-"}

	tabletID := 10
	for _, shard := range sourceShards {
		tme.sourceMasters = append(tme.sourceMasters, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks1", shard)))
		tabletID += 10
	}
	for _, shard := range targetShards {
		tme.targetMasters = append(tme.targetMasters, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks2", shard)))
		tabletID += 10
	}

	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
				}},
			},
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
				}},
			},
		},
	}
	if err := tme.ts.SaveVSchema(ctx, "ks1", vs); err != nil {
		t.Fatal(err)
	}
	if err := tme.ts.SaveVSchema(ctx, "ks2", vs); err != nil {
		t.Fatal(err)
	}
	if err := tme.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), tme.ts, "ks1", []string{"cell1"})
	if err != nil {
		t.Fatal(err)
	}
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), tme.ts, "ks2", []string{"cell1"})
	if err != nil {
		t.Fatal(err)
	}

	tme.startTablets(t)
	tme.createDBClients(ctx, t)
	tme.setMasterPositions()

	for i, targetShard := range targetShards {
		var rows []string
		for j, sourceShard := range sourceShards {
			bls := &binlogdatapb.BinlogSource{
				Keyspace: "ks1",
				Shard:    sourceShard,
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: fmt.Sprintf("select * from t1 where in_keyrange('%s')", targetShard),
					}, {
						Match:  "t2",
						Filter: fmt.Sprintf("select * from t2 where in_keyrange('%s')", targetShard),
					}},
				},
			}
			rows = append(rows, fmt.Sprintf("%d|%v", j+1, bls))
		}
		tme.dbTargetClients[i].addQuery(vreplQueryks2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source",
			"int64|varchar"),
			rows...),
			nil)
	}

	if err := tme.wr.saveRoutingRules(ctx, map[string][]string{
		"t1":     {"ks1.t1"},
		"ks2.t1": {"ks1.t1"},
		"t2":     {"ks1.t2"},
		"ks2.t2": {"ks1.t2"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tme.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}

	tme.targetKeyspace = "ks2"
	return tme
}

func newTestShardMigrater(ctx context.Context, t *testing.T) *testMigraterEnv {
	tme := &testMigraterEnv{}
	tme.ts = memorytopo.NewServer("cell1", "cell2")
	tme.wr = New(logutil.NewConsoleLogger(), tme.ts, tmclient.NewTabletManagerClient())

	sourceShards := []string{"-40", "40-"}
	targetShards := []string{"-80", "80-"}

	tabletID := 10
	for _, shard := range sourceShards {
		tme.sourceMasters = append(tme.sourceMasters, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", shard)))
		tabletID += 10
	}
	for _, shard := range targetShards {
		tme.targetMasters = append(tme.targetMasters, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", shard)))
		tabletID += 10
	}

	vs := &vschemapb.Keyspace{Sharded: true}
	if err := tme.ts.SaveVSchema(ctx, "ks", vs); err != nil {
		t.Fatal(err)
	}
	if err := tme.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), tme.ts, "ks", nil)
	if err != nil {
		t.Fatal(err)
	}

	tme.startTablets(t)
	tme.createDBClients(ctx, t)
	tme.setMasterPositions()

	for i, targetShard := range targetShards {
		_, targetKeyRange, err := topo.ValidateShardName(targetShard)
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		j := 1
		for _, sourceShard := range sourceShards {
			_, sourceKeyRange, err := topo.ValidateShardName(sourceShard)
			if err != nil {
				t.Fatal(err)
			}
			if !key.KeyRangesIntersect(targetKeyRange, sourceKeyRange) {
				continue
			}
			bls := &binlogdatapb.BinlogSource{
				Keyspace: "ks",
				Shard:    sourceShard,
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "/.*",
						Filter: targetShard,
					}},
				},
			}
			rows = append(rows, fmt.Sprintf("%d|%v", j, bls))
			j++
		}
		tme.dbTargetClients[i].addQuery(vreplQueryks, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source",
			"int64|varchar"),
			rows...),
			nil)
	}

	tme.targetKeyspace = "ks"
	for _, dbclient := range tme.dbSourceClients {
		dbclient.addQuery(vreplQueryks, &sqltypes.Result{}, nil)
	}
	return tme
}

func (tme *testMigraterEnv) startTablets(t *testing.T) {
	for _, master := range tme.sourceMasters {
		master.StartActionLoop(t, tme.wr)
	}
	for _, master := range tme.targetMasters {
		master.StartActionLoop(t, tme.wr)
	}
}

func (tme *testMigraterEnv) stopTablets(t *testing.T) {
	for _, master := range tme.sourceMasters {
		master.StopActionLoop(t)
	}
	for _, master := range tme.targetMasters {
		master.StopActionLoop(t)
	}
}

func (tme *testMigraterEnv) createDBClients(ctx context.Context, t *testing.T) {
	for _, master := range tme.sourceMasters {
		dbclient := newFakeDBClient()
		tme.dbSourceClients = append(tme.dbSourceClients, dbclient)
		dbClientFactory := func() binlogplayer.DBClient { return dbclient }
		master.Agent.VREngine = vreplication.NewEngine(tme.ts, "", master.FakeMysqlDaemon, dbClientFactory, dbclient.DBName())
		if err := master.Agent.VREngine.Open(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, master := range tme.targetMasters {
		dbclient := newFakeDBClient()
		tme.dbTargetClients = append(tme.dbTargetClients, dbclient)
		dbClientFactory := func() binlogplayer.DBClient { return dbclient }
		master.Agent.VREngine = vreplication.NewEngine(tme.ts, "", master.FakeMysqlDaemon, dbClientFactory, dbclient.DBName())
		if err := master.Agent.VREngine.Open(ctx); err != nil {
			t.Fatal(err)
		}
	}
	tme.allDBClients = append(tme.dbSourceClients, tme.dbTargetClients...)
}

func (tme *testMigraterEnv) setMasterPositions() {
	for _, master := range tme.sourceMasters {
		master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
			GTIDSet: mysql.MariadbGTIDSet{
				mysql.MariadbGTID{
					Domain:   5,
					Server:   456,
					Sequence: 892,
				},
			},
		}
	}
	for _, master := range tme.targetMasters {
		master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
			GTIDSet: mysql.MariadbGTIDSet{
				mysql.MariadbGTID{
					Domain:   5,
					Server:   456,
					Sequence: 893,
				},
			},
		}
	}
}
