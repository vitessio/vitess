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
	"reflect"
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

// TestTableMigrate tests table mode migrations.
// This has to be kept in sync with TestShardMigrate.
func TestTableMigrate(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create cluster: ks1:-40,40- and ks2:-80,80-.
	source1Master := NewFakeTablet(t, wr, "cell1", 10, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks1", "-40"))
	source1Replica := NewFakeTablet(t, wr, "cell1", 11, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks1", "-40"))
	source1Rdonly := NewFakeTablet(t, wr, "cell1", 12, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks1", "-40"))

	source2Master := NewFakeTablet(t, wr, "cell1", 20, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks1", "40-"))
	source2Replica := NewFakeTablet(t, wr, "cell1", 21, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks1", "40-"))
	source22Rdonly := NewFakeTablet(t, wr, "cell1", 22, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks1", "40-"))

	dest1Master := NewFakeTablet(t, wr, "cell1", 30, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks2", "-80"))
	dest1Replica := NewFakeTablet(t, wr, "cell1", 31, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks2", "-80"))
	dest1Rdonly := NewFakeTablet(t, wr, "cell1", 32, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks2", "-80"))

	dest2Master := NewFakeTablet(t, wr, "cell1", 40, topodatapb.TabletType_MASTER, nil, TabletKeyspaceShard(t, "ks2", "80-"))
	dest2Replica := NewFakeTablet(t, wr, "cell1", 41, topodatapb.TabletType_REPLICA, nil, TabletKeyspaceShard(t, "ks2", "80-"))
	dest2Rdonly := NewFakeTablet(t, wr, "cell1", 42, topodatapb.TabletType_RDONLY, nil, TabletKeyspaceShard(t, "ks2", "80-"))

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
	if err := wr.ts.SaveVSchema(ctx, "ks1", vs); err != nil {
		t.Fatal(err)
	}
	if err := wr.ts.SaveVSchema(ctx, "ks2", vs); err != nil {
		t.Fatal(err)
	}
	if err := wr.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, "ks1", []string{"cell1"})
	if err != nil {
		t.Fatal(err)
	}
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, "ks2", []string{"cell1"})
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, ts, "ks1:-40", 3)
	checkServedTypes(t, ts, "ks1:40-", 3)
	checkServedTypes(t, ts, "ks2:-80", 3)
	checkServedTypes(t, ts, "ks2:80-", 3)

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
	if err := dest1Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	dbDest2Client := newFakeDBClient()
	dbClientFactory2 := func() binlogplayer.DBClient { return dbDest2Client }
	dest2Master.Agent.VREngine = vreplication.NewEngine(ts, "", dest2Master.FakeMysqlDaemon, dbClientFactory2, dbDest2Client.DBName())
	if err := dest2Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	dbSource1Client := newFakeDBClient()
	dbClientFactory3 := func() binlogplayer.DBClient { return dbSource1Client }
	source1Master.Agent.VREngine = vreplication.NewEngine(ts, "", source1Master.FakeMysqlDaemon, dbClientFactory3, dbSource1Client.DBName())
	if err := source1Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	dbSource2Client := newFakeDBClient()
	dbClientFactory4 := func() binlogplayer.DBClient { return dbSource2Client }
	source2Master.Agent.VREngine = vreplication.NewEngine(ts, "", source2Master.FakeMysqlDaemon, dbClientFactory4, dbSource2Client.DBName())
	if err := source2Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	allDBClients := []*fakeDBClient{dbDest1Client, dbDest2Client, dbSource1Client, dbSource2Client}

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
	dbDest1Client.addQuery("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
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
	dbDest1Client.addQuery("select source from _vt.vreplication where id = 2", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
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
	dbDest2Client.addQuery("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"source",
		"varchar"),
		fmt.Sprintf("%v", bls3),
	), nil)

	if err := wr.saveRoutingRules(ctx, map[string][]string{
		"t1":     {"ks1.t1"},
		"ks2.t1": {"ks1.t1"},
		"t2":     {"ks1.t2"},
		"ks2.t2": {"ks1.t2"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := wr.ts.RebuildSrvVSchema(ctx, nil); err != nil {
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

	streams := map[topo.KeyspaceShard][]uint32{
		{Keyspace: "ks2", Shard: "-80"}: {1, 2},
		{Keyspace: "ks2", Shard: "80-"}: {1},
	}

	//-------------------------------------------------------------------------------------------------------------------
	// Single cell RDONLY migration.
	err = wr.MigrateReads(ctx, MigrateTables, streams, []string{"cell1"}, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellRouting(t, wr, "cell1", map[string][]string{
		"t1":            {"ks1.t1"},
		"ks2.t1":        {"ks1.t1"},
		"t2":            {"ks1.t2"},
		"ks2.t2":        {"ks1.t2"},
		"t1@rdonly":     {"ks2.t1"},
		"ks2.t1@rdonly": {"ks2.t1"},
		"t2@rdonly":     {"ks2.t2"},
		"ks2.t2@rdonly": {"ks2.t2"},
	})
	checkCellRouting(t, wr, "cell2", map[string][]string{
		"t1":     {"ks1.t1"},
		"ks2.t1": {"ks1.t1"},
		"t2":     {"ks1.t2"},
		"ks2.t2": {"ks1.t2"},
	})
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Other cell REPLICA migration.
	// The global routing already contains redirections for rdonly.
	// So, adding routes for replica and deploying to cell2 will also cause
	// cell2 to migrat rdonly. This is a quirk that can be fixed later if necessary.
	err = wr.MigrateReads(ctx, MigrateTables, streams, []string{"cell2"}, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellRouting(t, wr, "cell1", map[string][]string{
		"t1":            {"ks1.t1"},
		"ks2.t1":        {"ks1.t1"},
		"t2":            {"ks1.t2"},
		"ks2.t2":        {"ks1.t2"},
		"t1@rdonly":     {"ks2.t1"},
		"ks2.t1@rdonly": {"ks2.t1"},
		"t2@rdonly":     {"ks2.t2"},
		"ks2.t2@rdonly": {"ks2.t2"},
	})
	checkCellRouting(t, wr, "cell2", map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
	})
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Single cell backward REPLICA migration.
	err = wr.MigrateReads(ctx, MigrateTables, streams, []string{"cell2"}, topodatapb.TabletType_REPLICA, directionBackward)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, wr, map[string][]string{
		"t1":            {"ks1.t1"},
		"ks2.t1":        {"ks1.t1"},
		"t2":            {"ks1.t2"},
		"ks2.t2":        {"ks1.t2"},
		"t1@rdonly":     {"ks2.t1"},
		"ks2.t1@rdonly": {"ks2.t1"},
		"t2@rdonly":     {"ks2.t2"},
		"ks2.t2@rdonly": {"ks2.t2"},
	})
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Migrate all REPLICA.
	err = wr.MigrateReads(ctx, MigrateTables, streams, nil, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
	})
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// All cells RDONLY backward migration.
	err = wr.MigrateReads(ctx, MigrateTables, streams, nil, topodatapb.TabletType_RDONLY, directionBackward)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
	})
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't migrate master with MigrateReads.
	err = wr.MigrateReads(ctx, MigrateTables, streams, nil, topodatapb.TabletType_MASTER, directionForward)
	want := "tablet type must be REPLICA or RDONLY: MASTER"
	if err == nil || err.Error() != want {
		t.Errorf("MigrateReads(master) err: %v, want %v", err, want)
	}
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't migrate writes if REPLICA and RDONLY have not fully migrated yet.
	err = wr.MigrateWrites(ctx, MigrateTables, streams, 1*time.Second)
	want = "missing tablet type specific routing, read-only traffic must be migrated before migrating writes"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites err: %v, want %v", err, want)
	}
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Test MigrateWrites cancelation on failure.

	// Migrate all the reads first.
	err = wr.MigrateReads(ctx, MigrateTables, streams, nil, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
	})

	// Check for journals.
	dbSource1Client.addQuery("select 1 from _vt.resharding_journal where id = 445516443381867838", &sqltypes.Result{}, nil)
	dbSource2Client.addQuery("select 1 from _vt.resharding_journal where id = 445516443381867838", &sqltypes.Result{}, nil)

	// Wait for position: Reads current state, updates to Stopped, and re-reads.
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running|",
	)
	dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	dbDest2Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
	dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	dbDest2Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 2", &sqltypes.Result{}, nil)
	stopped := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|state",
		"int64|varchar"),
		"1|Stopped",
	)
	dbDest1Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	dbDest2Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	dbDest1Client.addQuery("select * from _vt.vreplication where id = 2", stopped, nil)

	// Cancel Migration
	cancel1 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 1"
	cancel2 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 2"
	dbDest1Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	dbDest2Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	dbDest1Client.addQuery(cancel2, &sqltypes.Result{}, nil)

	err = wr.MigrateWrites(ctx, MigrateTables, streams, 0*time.Second)
	want = "DeadlineExceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites(0 timeout) err: %v, must contain %v", err, want)
	}
	checkRouting(t, wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
	})
	checkBlacklist(t, ts, "ks1:-40", nil)
	checkBlacklist(t, ts, "ks1:40-", nil)
	checkBlacklist(t, ts, "ks2:-80", nil)
	checkBlacklist(t, ts, "ks2:80-", nil)

	//-------------------------------------------------------------------------------------------------------------------
	// Test successful MigrateWrites.

	// Create journals.
	journal1 := "insert into _vt.resharding_journal.*445516443381867838.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*-80.*MariaDB/5-456-893.*participants.*40.*40"
	dbSource1Client.addQueryRE(journal1, &sqltypes.Result{}, nil)
	journal2 := "insert into _vt.resharding_journal.*445516443381867838.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*80.*participants.*40.*40"
	dbSource2Client.addQueryRE(journal2, &sqltypes.Result{}, nil)

	// Create backward replicaions.
	dbSource1Client.addQueryRE("insert into _vt.vreplication.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*-40.*t2.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	dbSource2Client.addQueryRE("insert into _vt.vreplication.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	dbSource2Client.addQueryRE("insert into _vt.vreplication.*ks2.*80-.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
	dbSource1Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	dbSource2Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	dbSource2Client.addQuery("select * from _vt.vreplication where id = 2", stopped, nil)

	// Delete the target replications.
	dbDest1Client.addQuery("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	dbDest2Client.addQuery("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	dbDest1Client.addQuery("delete from _vt.vreplication where id = 2", &sqltypes.Result{}, nil)

	err = wr.MigrateWrites(ctx, MigrateTables, streams, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	checkRouting(t, wr, map[string][]string{
		"t1": {"ks2.t1"},
		"t2": {"ks2.t2"},
	})
	checkBlacklist(t, ts, "ks1:-40", []string{"t1", "t2"})
	checkBlacklist(t, ts, "ks1:40-", []string{"t1", "t2"})
	checkBlacklist(t, ts, "ks2:-80", nil)
	checkBlacklist(t, ts, "ks2:80-", nil)

	verifyQueries(t, allDBClients)
}

// TestShardMigrate tests table mode migrations.
// This has to be kept in sync with TestTableMigrate.
func TestShardMigrate(t *testing.T) {
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
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, "ks", nil)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, ts, "ks:-40", 3)
	checkServedTypes(t, ts, "ks:40-", 3)
	checkServedTypes(t, ts, "ks:-80", 0)
	checkServedTypes(t, ts, "ks:80-", 0)

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
	if err := dest1Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	dbDest2Client := newFakeDBClient()
	dbClientFactory2 := func() binlogplayer.DBClient { return dbDest2Client }
	dest2Master.Agent.VREngine = vreplication.NewEngine(ts, "", dest2Master.FakeMysqlDaemon, dbClientFactory2, dbDest2Client.DBName())
	if err := dest2Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	dbSource1Client := newFakeDBClient()
	dbClientFactory3 := func() binlogplayer.DBClient { return dbSource1Client }
	source1Master.Agent.VREngine = vreplication.NewEngine(ts, "", source1Master.FakeMysqlDaemon, dbClientFactory3, dbSource1Client.DBName())
	if err := source1Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	dbSource2Client := newFakeDBClient()
	dbClientFactory4 := func() binlogplayer.DBClient { return dbSource2Client }
	source2Master.Agent.VREngine = vreplication.NewEngine(ts, "", source2Master.FakeMysqlDaemon, dbClientFactory4, dbSource2Client.DBName())
	if err := source2Master.Agent.VREngine.Open(ctx); err != nil {
		t.Fatal(err)
	}

	allDBClients := []*fakeDBClient{dbDest1Client, dbDest2Client, dbSource1Client, dbSource2Client}

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
	dbDest1Client.addQuery("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
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
	dbDest1Client.addQuery("select source from _vt.vreplication where id = 2", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
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
	dbDest2Client.addQuery("select source from _vt.vreplication where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"source",
		"varchar"),
		fmt.Sprintf("%v", bls3),
	), nil)

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

	streams := map[topo.KeyspaceShard][]uint32{
		{Keyspace: "ks", Shard: "-80"}: {1, 2},
		{Keyspace: "ks", Shard: "80-"}: {1},
	}

	//-------------------------------------------------------------------------------------------------------------------
	// Single cell RDONLY migration.
	err = wr.MigrateReads(ctx, MigrateShards, streams, []string{"cell1"}, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, ts, "ks:-40", "cell2", 3)
	checkCellServedTypes(t, ts, "ks:40-", "cell2", 3)
	checkCellServedTypes(t, ts, "ks:-80", "cell2", 0)
	checkCellServedTypes(t, ts, "ks:80-", "cell2", 0)
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Other cell REPLICA migration.
	err = wr.MigrateReads(ctx, MigrateShards, streams, []string{"cell2"}, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, ts, "ks:-40", "cell2", 2)
	checkCellServedTypes(t, ts, "ks:40-", "cell2", 2)
	checkCellServedTypes(t, ts, "ks:-80", "cell2", 1)
	checkCellServedTypes(t, ts, "ks:80-", "cell2", 1)
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Single cell backward REPLICA migration.
	err = wr.MigrateReads(ctx, MigrateShards, streams, []string{"cell2"}, topodatapb.TabletType_REPLICA, directionBackward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, ts, "ks:-40", "cell2", 3)
	checkCellServedTypes(t, ts, "ks:40-", "cell2", 3)
	checkCellServedTypes(t, ts, "ks:-80", "cell2", 0)
	checkCellServedTypes(t, ts, "ks:80-", "cell2", 0)
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Migrate all RDONLY.
	// This is an extra step that does not exist in the tables test.
	// The per-cell migration mechanism is different for tables. So, this
	// extra step is needed to bring things in sync.
	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, ts, "ks:-40", 2)
	checkServedTypes(t, ts, "ks:40-", 2)
	checkServedTypes(t, ts, "ks:-80", 1)
	checkServedTypes(t, ts, "ks:80-", 1)
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Migrate all REPLICA.
	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, ts, "ks:-40", 1)
	checkServedTypes(t, ts, "ks:40-", 1)
	checkServedTypes(t, ts, "ks:-80", 2)
	checkServedTypes(t, ts, "ks:80-", 2)
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// All cells RDONLY backward migration.
	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_RDONLY, directionBackward)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, ts, "ks:-40", 2)
	checkServedTypes(t, ts, "ks:40-", 2)
	checkServedTypes(t, ts, "ks:-80", 1)
	checkServedTypes(t, ts, "ks:80-", 1)
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't migrate master with MigrateReads.
	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_MASTER, directionForward)
	want := "tablet type must be REPLICA or RDONLY: MASTER"
	if err == nil || err.Error() != want {
		t.Errorf("MigrateReads(master) err: %v, want %v", err, want)
	}
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't migrate writes if REPLICA and RDONLY have not fully migrated yet.
	err = wr.MigrateWrites(ctx, MigrateShards, streams, 1*time.Second)
	want = "cannot migrate MASTER away"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites err: %v, want %v", err, want)
	}
	verifyQueries(t, allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Test MigrateWrites cancelation on failure.

	// Migrate all the reads first.
	err = wr.MigrateReads(ctx, MigrateShards, streams, nil, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, ts, "ks:-40", 1)
	checkServedTypes(t, ts, "ks:40-", 1)
	checkServedTypes(t, ts, "ks:-80", 2)
	checkServedTypes(t, ts, "ks:80-", 2)
	checkIsMasterServing(t, ts, "ks:-40", true)
	checkIsMasterServing(t, ts, "ks:40-", true)
	checkIsMasterServing(t, ts, "ks:-80", false)
	checkIsMasterServing(t, ts, "ks:80-", false)

	// Check for journals.
	dbSource1Client.addQuery("select 1 from _vt.resharding_journal where id = 8372031610433464572", &sqltypes.Result{}, nil)
	dbSource2Client.addQuery("select 1 from _vt.resharding_journal where id = 8372031610433464572", &sqltypes.Result{}, nil)

	// Wait for position: Reads current state, updates to Stopped, and re-reads.
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running|",
	)
	dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	dbDest2Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
	dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	dbDest2Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 2", &sqltypes.Result{}, nil)
	stopped := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|state",
		"int64|varchar"),
		"1|Stopped",
	)
	dbDest1Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	dbDest2Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	dbDest1Client.addQuery("select * from _vt.vreplication where id = 2", stopped, nil)

	// Cancel Migration
	cancel1 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 1"
	cancel2 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 2"
	dbDest1Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	dbDest2Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	dbDest1Client.addQuery(cancel2, &sqltypes.Result{}, nil)

	err = wr.MigrateWrites(ctx, MigrateShards, streams, 0*time.Second)
	want = "DeadlineExceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites(0 timeout) err: %v, must contain %v", err, want)
	}
	checkServedTypes(t, ts, "ks:-40", 1)
	checkServedTypes(t, ts, "ks:40-", 1)
	checkServedTypes(t, ts, "ks:-80", 2)
	checkServedTypes(t, ts, "ks:80-", 2)
	checkIsMasterServing(t, ts, "ks:-40", true)
	checkIsMasterServing(t, ts, "ks:40-", true)
	checkIsMasterServing(t, ts, "ks:-80", false)
	checkIsMasterServing(t, ts, "ks:80-", false)

	//-------------------------------------------------------------------------------------------------------------------
	// Test successful MigrateWrites.

	// Create journals.
	journal1 := "insert into _vt.resharding_journal.*8372031610433464572.*local_position.*MariaDB/5-456-892.*shard_gtids.*-80.*MariaDB/5-456-893.*participants.*40.*40"
	dbSource1Client.addQueryRE(journal1, &sqltypes.Result{}, nil)
	journal2 := "insert into _vt.resharding_journal.*8372031610433464572.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*shard_gtids.*80.*MariaDB/5-456-893.*participants.*40.*40"
	dbSource2Client.addQueryRE(journal2, &sqltypes.Result{}, nil)

	// Create backward replicaions.
	dbSource1Client.addQueryRE("insert into _vt.vreplication.*-80.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	dbSource2Client.addQueryRE("insert into _vt.vreplication.*-80.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	dbSource2Client.addQueryRE("insert into _vt.vreplication.*80-.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
	dbSource1Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	dbSource2Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	dbSource2Client.addQuery("select * from _vt.vreplication where id = 2", stopped, nil)

	// Delete the target replications.
	dbDest1Client.addQuery("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	dbDest2Client.addQuery("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	dbDest1Client.addQuery("delete from _vt.vreplication where id = 2", &sqltypes.Result{}, nil)

	err = wr.MigrateWrites(ctx, MigrateShards, streams, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	checkServedTypes(t, ts, "ks:-40", 0)
	checkServedTypes(t, ts, "ks:40-", 0)
	checkServedTypes(t, ts, "ks:-80", 3)
	checkServedTypes(t, ts, "ks:80-", 3)

	checkIsMasterServing(t, ts, "ks:-40", false)
	checkIsMasterServing(t, ts, "ks:40-", false)
	checkIsMasterServing(t, ts, "ks:-80", true)
	checkIsMasterServing(t, ts, "ks:80-", true)

	verifyQueries(t, allDBClients)
}

func checkRouting(t *testing.T, wr *Wrangler, want map[string][]string) {
	t.Helper()
	ctx := context.Background()
	got, err := wr.getRoutingRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rules:\n%v, want\n%v", got, want)
	}
	cells, err := wr.ts.GetCellInfoNames(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range cells {
		checkCellRouting(t, wr, cell, want)
	}
}

func checkCellRouting(t *testing.T, wr *Wrangler, cell string, want map[string][]string) {
	t.Helper()
	ctx := context.Background()
	svs, err := wr.ts.GetSrvVSchema(ctx, cell)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string][]string)
	for _, rr := range svs.RoutingRules.Rules {
		got[rr.FromTable] = append(got[rr.FromTable], rr.ToTables...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("srv rules for cell %s:\n%v, want\n%v", cell, got, want)
	}
}

func checkBlacklist(t *testing.T, ts *topo.Server, keyspaceShard string, want []string) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	si, err := ts.GetShard(ctx, splits[0], splits[1])
	if err != nil {
		t.Fatal(err)
	}
	tc := si.GetTabletControl(topodatapb.TabletType_MASTER)
	var got []string
	if tc != nil {
		got = tc.BlacklistedTables
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Blacklisted tables for %v: %v, want %v", keyspaceShard, got, want)
	}
}

func checkServedTypes(t *testing.T, ts *topo.Server, keyspaceShard string, want int) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	si, err := ts.GetShard(ctx, splits[0], splits[1])
	if err != nil {
		t.Fatal(err)
	}

	servedTypes, err := ts.GetShardServingTypes(ctx, si)
	if err != nil {
		t.Fatal(err)
	}

	if len(servedTypes) != want {
		t.Errorf("shard %v has wrong served types: got: %v, want: %v", keyspaceShard, len(servedTypes), want)
	}
}

func checkCellServedTypes(t *testing.T, ts *topo.Server, keyspaceShard, cell string, want int) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, splits[0])
	if err != nil {
		t.Fatal(err)
	}
	count := 0
outer:
	for _, partition := range srvKeyspace.GetPartitions() {
		for _, ref := range partition.ShardReferences {
			if ref.Name == splits[1] {
				count++
				continue outer
			}
		}
	}
	if count != want {
		t.Errorf("serving types for keyspaceShard %s, cell %s: %d, want %d", keyspaceShard, cell, count, want)
	}
}

func checkIsMasterServing(t *testing.T, ts *topo.Server, keyspaceShard string, want bool) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	si, err := ts.GetShard(ctx, splits[0], splits[1])
	if err != nil {
		t.Fatal(err)
	}
	if want != si.IsMasterServing {
		t.Errorf("IsMasterServing(%v): %v, want %v", keyspaceShard, si.IsMasterServing, want)
	}
}
