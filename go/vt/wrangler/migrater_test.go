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
	"strings"
	"testing"
	"time"

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

func TestShardMigrateReads(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create cluster with "ks" as keyspace. -40,40- as serving, -80,80- as non-serving.
	source1Master := NewFakeTablet(t, wr, "cell1", 10, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", "-40"))
	source1Replica := NewFakeTablet(t, wr, "cell1", 11, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks", "-40"))
	source1Rdonly := NewFakeTablet(t, wr, "cell1", 12, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks", "-40"))

	source2Master := NewFakeTablet(t, wr, "cell1", 20, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", "40-"))
	source2Replica := NewFakeTablet(t, wr, "cell1", 21, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks", "40-"))
	source22Rdonly := NewFakeTablet(t, wr, "cell1", 22, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks", "40-"))

	dest1Master := NewFakeTablet(t, wr, "cell1", 30, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", "-80"))
	dest1Replica := NewFakeTablet(t, wr, "cell1", 31, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks", "-80"))
	dest1Rdonly := NewFakeTablet(t, wr, "cell1", 32, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks", "-80"))

	dest2Master := NewFakeTablet(t, wr, "cell1", 40, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks", "80-"))
	dest2Replica := NewFakeTablet(t, wr, "cell1", 41, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks", "80-"))
	dest2Rdonly := NewFakeTablet(t, wr, "cell1", 42, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks", "80-"))

	vs := &vschemapb.Keyspace{Sharded: true}
	if err := wr.ts.SaveVSchema(ctx, "ks", vs); err != nil {
		t.Fatal(err)
	}
	if err := wr.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}

	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, "ks", []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	checkShardServedTypes(t, ts, "-40", 3)
	checkShardServedTypes(t, ts, "40-", 3)
	checkShardServedTypes(t, ts, "-80", 0)
	checkShardServedTypes(t, ts, "80-", 0)

	source1Replica.StartActionLoop(t, wr)
	defer source1Replica.StopActionLoop(t)
	source1Rdonly.StartActionLoop(t, wr)
	defer source1Rdonly.StopActionLoop(t)
	source1Master.StartActionLoop(t, wr)
	defer source1Master.StopActionLoop(t)

	source2Replica.StartActionLoop(t, wr)
	defer source2Replica.StopActionLoop(t)
	source22Rdonly.StartActionLoop(t, wr)
	defer source22Rdonly.StopActionLoop(t)
	source2Master.StartActionLoop(t, wr)
	defer source2Master.StopActionLoop(t)

	dest1Replica.StartActionLoop(t, wr)
	defer dest1Replica.StopActionLoop(t)
	dest1Rdonly.StartActionLoop(t, wr)
	defer dest1Rdonly.StopActionLoop(t)
	dest1Master.StartActionLoop(t, wr)
	defer dest1Master.StopActionLoop(t)

	dest2Replica.StartActionLoop(t, wr)
	defer dest2Replica.StopActionLoop(t)
	dest2Rdonly.StartActionLoop(t, wr)
	defer dest2Rdonly.StopActionLoop(t)
	dest2Master.StartActionLoop(t, wr)
	defer dest2Master.StopActionLoop(t)

	// Override with a fake VREngine after Agent is initialized in action loop.
	dbDest1Client := newFakeDBClient()
	dbClientFactory1 := func() binlogplayer.DBClient { return dbDest1Client }
	dest1Master.Agent.VREngine = vreplication.NewEngine(ts, "", dest1Master.FakeMysqlDaemon, dbClientFactory1, dbDest1Client.DBName())
	dbDest1Client.setResult("use _vt", &sqltypes.Result{}, nil)
	dbDest1Client.setResult("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	if err := dest1Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	dbDest2Client := newFakeDBClient()
	dbClientFactory2 := func() binlogplayer.DBClient { return dbDest2Client }
	dest2Master.Agent.VREngine = vreplication.NewEngine(ts, "", dest2Master.FakeMysqlDaemon, dbClientFactory2, dbDest2Client.DBName())
	dbDest2Client.setResult("use _vt", &sqltypes.Result{}, nil)
	dbDest2Client.setResult("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	if err := dest2Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	dbSource1Client := newFakeDBClient()
	dbClientFactory3 := func() binlogplayer.DBClient { return dbSource1Client }
	source1Master.Agent.VREngine = vreplication.NewEngine(ts, "", source1Master.FakeMysqlDaemon, dbClientFactory3, dbSource1Client.DBName())
	dbSource1Client.setResult("use _vt", &sqltypes.Result{}, nil)
	dbSource1Client.setResult("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	if err := source1Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	dbSource2Client := newFakeDBClient()
	dbClientFactory4 := func() binlogplayer.DBClient { return dbSource2Client }
	source2Master.Agent.VREngine = vreplication.NewEngine(ts, "", source2Master.FakeMysqlDaemon, dbClientFactory4, dbSource2Client.DBName())
	dbSource2Client.setResult("use _vt", &sqltypes.Result{}, nil)
	dbSource2Client.setResult("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	if err := source2Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

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
	dbDest1Client.setResult("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"source",
			"varchar",
		),
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
	dbDest1Client.setResult("select source from _vt.vreplication where id = 2", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"source",
			"varchar",
		),
		fmt.Sprintf("%v", bls2),
	), nil)
	bls3 := &binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "-40",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "/.*",
				Filter: "80-",
			}},
		},
	}
	dbDest2Client.setResult("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"source",
			"varchar",
		),
		fmt.Sprintf("%v", bls3),
	), nil)
	bls4 := &binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "40-",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "/.*",
				Filter: "80-",
			}},
		},
	}
	dbDest2Client.setResult("select source from _vt.vreplication where id = 2", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"source",
			"varchar",
		),
		fmt.Sprintf("%v", bls4),
	), nil)

	streams := map[topo.KeyspaceShard][]uint32{
		{Keyspace: "ks", Shard: "-80"}: {1, 2},
		{Keyspace: "ks", Shard: "80-"}: {1, 2},
	}

	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkShardServedTypes(t, ts, "-40", 2)
	checkShardServedTypes(t, ts, "40-", 2)
	checkShardServedTypes(t, ts, "-80", 1)
	checkShardServedTypes(t, ts, "80-", 1)

	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkShardServedTypes(t, ts, "-40", 1)
	checkShardServedTypes(t, ts, "40-", 1)
	checkShardServedTypes(t, ts, "-80", 2)
	checkShardServedTypes(t, ts, "80-", 2)

	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_RDONLY, directionBackward)
	if err != nil {
		t.Fatal(err)
	}
	checkShardServedTypes(t, ts, "-40", 2)
	checkShardServedTypes(t, ts, "40-", 2)
	checkShardServedTypes(t, ts, "-80", 1)
	checkShardServedTypes(t, ts, "80-", 1)

	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_MASTER, directionForward)
	want := "tablet type must be REPLICA or RDONLY: MASTER"
	if err == nil || err.Error() != want {
		t.Errorf("MigrateReads(master) err: %v, want %v", err, want)
	}

	err = wr.MigrateWrites(ctx, MigrateShards, streams, 1*time.Second)
	want = "cannot migrate MASTER away"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites err: %v, want %v", err, want)
	}

	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}

	source1Master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 892,
			},
		},
	}
	source2Master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 892,
			},
		},
	}
	dest1Master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 893,
			},
		},
	}
	dest2Master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 893,
			},
		},
	}

	// Check for journals.
	dbSource1Client.setResult("select 1 from _vt.resharding_journal where id = 4076872238118445101", &sqltypes.Result{}, nil)
	dbSource2Client.setResult("select 1 from _vt.resharding_journal where id = 4076872238118445101", &sqltypes.Result{}, nil)

	// Wait for position: Reads current state, updates to Stopped, and re-reads.
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running|",
	)
	dbDest1Client.setResult("select pos, state, message from _vt.vreplication where id=1", state, nil)
	dbDest2Client.setResult("select pos, state, message from _vt.vreplication where id=1", state, nil)
	dbDest1Client.setResult("select pos, state, message from _vt.vreplication where id=2", state, nil)
	dbDest2Client.setResult("select pos, state, message from _vt.vreplication where id=2", state, nil)
	dbDest1Client.setResult("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	dbDest2Client.setResult("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	dbDest1Client.setResult("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 2", &sqltypes.Result{}, nil)
	dbDest2Client.setResult("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 2", &sqltypes.Result{}, nil)
	stopped := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|state",
		"int64|varchar"),
		"1|Stopped",
	)
	dbDest1Client.setResult("select * from _vt.vreplication where id = 1", stopped, nil)
	dbDest2Client.setResult("select * from _vt.vreplication where id = 1", stopped, nil)
	dbDest1Client.setResult("select * from _vt.vreplication where id = 2", stopped, nil)
	dbDest2Client.setResult("select * from _vt.vreplication where id = 2", stopped, nil)

	// Create journals.
	journal := "insert into _vt.resharding_journal.*4076872238118445101.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*shard_gtids.*80.*MariaDB/5-456-893.*participants.*40.*40"
	dbSource1Client.setResultRE(journal, &sqltypes.Result{}, nil)
	dbSource2Client.setResultRE(journal, &sqltypes.Result{}, nil)

	// Create reverse replicaions.
	dbSource1Client.setResultRE("insert into _vt.vreplication.*-80.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	dbSource1Client.setResultRE("insert into _vt.vreplication.*80-.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
	dbSource2Client.setResultRE("insert into _vt.vreplication.*-80.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	dbSource2Client.setResultRE("insert into _vt.vreplication.*80-.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
	dbSource1Client.setResult("select * from _vt.vreplication where id = 1", stopped, nil)
	dbSource2Client.setResult("select * from _vt.vreplication where id = 1", stopped, nil)
	dbSource1Client.setResult("select * from _vt.vreplication where id = 2", stopped, nil)
	dbSource2Client.setResult("select * from _vt.vreplication where id = 2", stopped, nil)

	// Delete the target replications.
	dbDest1Client.setResult("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	dbDest2Client.setResult("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	dbDest1Client.setResult("delete from _vt.vreplication where id = 2", &sqltypes.Result{}, nil)
	dbDest2Client.setResult("delete from _vt.vreplication where id = 2", &sqltypes.Result{}, nil)
	err = wr.MigrateWrites(ctx, MigrateShards, streams, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func checkShardServedTypes(t *testing.T, ts *topo.Server, shard string, expected int) {
	t.Helper()
	ctx := context.Background()
	si, err := ts.GetShard(ctx, "ks", shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	servedTypes, err := ts.GetShardServingTypes(ctx, si)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	if len(servedTypes) != expected {
		t.Errorf("shard %v has wrong served types: got: %v, expected: %v", shard, len(servedTypes), expected)
	}
}
