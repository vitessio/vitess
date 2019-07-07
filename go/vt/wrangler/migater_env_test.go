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

type testMigraterEnv struct {
	ts                                           *topo.Server
	wr                                           *Wrangler
	source1Master, source1Replica, source1Rdonly *fakeTablet
	source2Master, source2Replica, source2Rdonly *fakeTablet
	dest1Master, dest1Replica, dest1Rdonly       *fakeTablet
	dest2Master, dest2Replica, dest2Rdonly       *fakeTablet
	dbSource1Client, dbSource2Client             *fakeDBClient
	dbDest1Client, dbDest2Client                 *fakeDBClient
	allDBClients                                 []*fakeDBClient
	streams                                      map[topo.KeyspaceShard][]uint32
}

func newTestTableMigrater(ctx context.Context, t *testing.T) *testMigraterEnv {
	tme := &testMigraterEnv{}
	tme.ts = memorytopo.NewServer("cell1", "cell2")
	tme.wr = New(logutil.NewConsoleLogger(), tme.ts, tmclient.NewTabletManagerClient())

	// Create cluster: ks1:-40,40- and ks2:-80,80-.
	tme.source1Master = newFakeTablet(t, tme.wr, "cell1", 10, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks1", "-40"))
	tme.source1Replica = newFakeTablet(t, tme.wr, "cell1", 11, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks1", "-40"))
	tme.source1Rdonly = newFakeTablet(t, tme.wr, "cell1", 12, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks1", "-40"))

	tme.source2Master = newFakeTablet(t, tme.wr, "cell1", 20, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks1", "40-"))
	tme.source2Replica = newFakeTablet(t, tme.wr, "cell1", 21, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks1", "40-"))
	tme.source2Rdonly = newFakeTablet(t, tme.wr, "cell1", 22, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks1", "40-"))

	tme.dest1Master = newFakeTablet(t, tme.wr, "cell1", 30, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks2", "-80"))
	tme.dest1Replica = newFakeTablet(t, tme.wr, "cell1", 31, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks2", "-80"))
	tme.dest1Rdonly = newFakeTablet(t, tme.wr, "cell1", 32, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks2", "-80"))

	tme.dest2Master = newFakeTablet(t, tme.wr, "cell1", 40, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks2", "80-"))
	tme.dest2Replica = newFakeTablet(t, tme.wr, "cell1", 41, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks2", "80-"))
	tme.dest2Rdonly = newFakeTablet(t, tme.wr, "cell1", 42, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks2", "80-"))

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

	// Emulate the following replication streams (many-to-many table migration):
	// -40 -> -80
	// 40- -> -80
	// 40- -> 80-
	// -40 will only have one target, and 80- will have only one source.
	bls1 := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "-40",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1 where in_keyrange('-80')",
			}, {
				Match:  "t2",
				Filter: "select * from t2 where in_keyrange('-80')",
			}},
		},
	}
	tme.dbDest1Client.addQuery("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"source",
		"varchar"),
		fmt.Sprintf("%v", bls1),
	), nil)
	bls2 := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "40-",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1 where in_keyrange('-80')",
			}, {
				Match:  "t2",
				Filter: "select * from t2 where in_keyrange('-80')",
			}},
		},
	}
	tme.dbDest1Client.addQuery("select source from _vt.vreplication where id = 2", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"source",
		"varchar"),
		fmt.Sprintf("%v", bls2),
	), nil)
	bls3 := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "40-",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1 where in_keyrange('80-')",
			}, {
				Match:  "t2",
				Filter: "select * from t2 where in_keyrange('80-')",
			}},
		},
	}
	tme.dbDest2Client.addQuery("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"source",
		"varchar"),
		fmt.Sprintf("%v", bls3),
	), nil)

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

	tme.streams = map[topo.KeyspaceShard][]uint32{
		{Keyspace: "ks2", Shard: "-80"}: {1, 2},
		{Keyspace: "ks2", Shard: "80-"}: {1},
	}
	return tme
}

func newTestShardMigrater(ctx context.Context, t *testing.T) *testMigraterEnv {
	tme := &testMigraterEnv{}
	tme.ts = memorytopo.NewServer("cell1", "cell2")
	tme.wr = New(logutil.NewConsoleLogger(), tme.ts, tmclient.NewTabletManagerClient())

	// Create cluster with "ks" as keyspace. -40,40- as serving, -80,80- as non-serving.
	tme.source1Master = newFakeTablet(t, tme.wr, "cell1", 10, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", "-40"))
	tme.source1Replica = newFakeTablet(t, tme.wr, "cell1", 11, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks", "-40"))
	tme.source1Rdonly = newFakeTablet(t, tme.wr, "cell1", 12, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks", "-40"))

	tme.source2Master = newFakeTablet(t, tme.wr, "cell1", 20, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", "40-"))
	tme.source2Replica = newFakeTablet(t, tme.wr, "cell1", 21, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks", "40-"))
	tme.source2Rdonly = newFakeTablet(t, tme.wr, "cell1", 22, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks", "40-"))

	tme.dest1Master = newFakeTablet(t, tme.wr, "cell1", 30, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", "-80"))
	tme.dest1Replica = newFakeTablet(t, tme.wr, "cell1", 31, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks", "-80"))
	tme.dest1Rdonly = newFakeTablet(t, tme.wr, "cell1", 32, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks", "-80"))

	tme.dest2Master = newFakeTablet(t, tme.wr, "cell1", 40, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", "80-"))
	tme.dest2Replica = newFakeTablet(t, tme.wr, "cell1", 41, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks", "80-"))
	tme.dest2Rdonly = newFakeTablet(t, tme.wr, "cell1", 42, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks", "80-"))

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

	// Emulate the following replication streams (simultaneous split and merge):
	// -40 -> -80
	// 40- -> -80
	// 40- -> 80-
	// -40 will only have one target, and 80- will have only one source.
	bls1 := &binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "-40",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "/.*",
				Filter: "-80",
			}},
		},
	}
	tme.dbDest1Client.addQuery("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"source",
		"varchar"),
		fmt.Sprintf("%v", bls1),
	), nil)
	bls2 := &binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "40-",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "/.*",
				Filter: "-80",
			}},
		},
	}
	tme.dbDest1Client.addQuery("select source from _vt.vreplication where id = 2", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"source",
		"varchar"),
		fmt.Sprintf("%v", bls2),
	), nil)
	bls3 := &binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "40-",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "/.*",
				Filter: "80-",
			}},
		},
	}
	tme.dbDest2Client.addQuery("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"source",
		"varchar"),
		fmt.Sprintf("%v", bls3),
	), nil)

	tme.streams = map[topo.KeyspaceShard][]uint32{
		{Keyspace: "ks", Shard: "-80"}: {1, 2},
		{Keyspace: "ks", Shard: "80-"}: {1},
	}
	return tme
}

func (tme *testMigraterEnv) startTablets(t *testing.T) {
	tme.source1Replica.StartActionLoop(t, tme.wr)
	tme.source1Rdonly.StartActionLoop(t, tme.wr)
	tme.source1Master.StartActionLoop(t, tme.wr)

	tme.source2Replica.StartActionLoop(t, tme.wr)
	tme.source2Rdonly.StartActionLoop(t, tme.wr)
	tme.source2Master.StartActionLoop(t, tme.wr)

	tme.dest1Replica.StartActionLoop(t, tme.wr)
	tme.dest1Rdonly.StartActionLoop(t, tme.wr)
	tme.dest1Master.StartActionLoop(t, tme.wr)

	tme.dest2Replica.StartActionLoop(t, tme.wr)
	tme.dest2Rdonly.StartActionLoop(t, tme.wr)
	tme.dest2Master.StartActionLoop(t, tme.wr)
}

func (tme *testMigraterEnv) stopTablets(t *testing.T) {
	tme.source1Replica.StopActionLoop(t)
	tme.source1Rdonly.StopActionLoop(t)
	tme.source1Master.StopActionLoop(t)

	tme.source2Replica.StopActionLoop(t)
	tme.source2Rdonly.StopActionLoop(t)
	tme.source2Master.StopActionLoop(t)

	tme.dest1Replica.StopActionLoop(t)
	tme.dest1Rdonly.StopActionLoop(t)
	tme.dest1Master.StopActionLoop(t)

	tme.dest2Replica.StopActionLoop(t)
	tme.dest2Rdonly.StopActionLoop(t)
	tme.dest2Master.StopActionLoop(t)
}

func (tme *testMigraterEnv) createDBClients(ctx context.Context, t *testing.T) {
	tme.dbDest1Client = newFakeDBClient()
	dbClientFactory1 := func() binlogplayer.DBClient { return tme.dbDest1Client }
	tme.dest1Master.Agent.VREngine = vreplication.NewEngine(tme.ts, "", tme.dest1Master.FakeMysqlDaemon, dbClientFactory1, tme.dbDest1Client.DBName())
	if err := tme.dest1Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	tme.dbDest2Client = newFakeDBClient()
	dbClientFactory2 := func() binlogplayer.DBClient { return tme.dbDest2Client }
	tme.dest2Master.Agent.VREngine = vreplication.NewEngine(tme.ts, "", tme.dest2Master.FakeMysqlDaemon, dbClientFactory2, tme.dbDest2Client.DBName())
	if err := tme.dest2Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	tme.dbSource1Client = newFakeDBClient()
	dbClientFactory3 := func() binlogplayer.DBClient { return tme.dbSource1Client }
	tme.source1Master.Agent.VREngine = vreplication.NewEngine(tme.ts, "", tme.source1Master.FakeMysqlDaemon, dbClientFactory3, tme.dbSource1Client.DBName())
	if err := tme.source1Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	tme.dbSource2Client = newFakeDBClient()
	dbClientFactory4 := func() binlogplayer.DBClient { return tme.dbSource2Client }
	tme.source2Master.Agent.VREngine = vreplication.NewEngine(tme.ts, "", tme.source2Master.FakeMysqlDaemon, dbClientFactory4, tme.dbSource2Client.DBName())
	if err := tme.source2Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	tme.allDBClients = []*fakeDBClient{tme.dbDest1Client, tme.dbDest2Client, tme.dbSource1Client, tme.dbSource2Client}
}

func (tme *testMigraterEnv) setMasterPositions() {
	tme.source1Master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 892,
			},
		},
	}
	tme.source2Master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 892,
			},
		},
	}
	tme.dest1Master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 893,
			},
		},
	}
	tme.dest2Master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 893,
			},
		},
	}
}
