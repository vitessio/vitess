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

package tabletmanager

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/grpcclient"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletconn"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// The tests in this file test the BinlogPlayerMap object.
//
// The BinlogPlayerMap object is configured using the SourceShards of a Shard
// object. So we have to create the right topology entries for that.
//
// BinlogPlayerMap will create BinlogPlayerController objects
// to talk to the source remote tablets. They will use the topology to
// find valid tablets, so we have to update the Tablets.
//
// We fake the communication between the BinlogPlayerController objects and
// the remote tablets by registering our own binlogplayer.Client.
//
// BinlogPlayerController objects will then play the received events
// through a binlogplayer.VtClient. Again, we mock that one to record
// what is being sent to it and make sure it's correct.

// fakeBinlogClient implements binlogplayer.Client
type fakeBinlogClient struct {
	t               *testing.T
	expectedDialUID uint32

	expectedTables string
	tablesChannel  chan *binlogdatapb.BinlogTransaction

	expectedKeyRange string
	keyRangeChannel  chan *binlogdatapb.BinlogTransaction
}

func newFakeBinlogClient(t *testing.T, expectedDialUID uint32) *fakeBinlogClient {
	return &fakeBinlogClient{
		t:               t,
		expectedDialUID: expectedDialUID,

		tablesChannel:   make(chan *binlogdatapb.BinlogTransaction),
		keyRangeChannel: make(chan *binlogdatapb.BinlogTransaction),
	}
}

// Dial is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) Dial(tablet *topodatapb.Tablet) error {
	if fbc.expectedDialUID != tablet.Alias.Uid {
		fbc.t.Errorf("fakeBinlogClient.Dial expected uid %v got %v", fbc.expectedDialUID, tablet.Alias.Uid)
	}
	return nil
}

// Close is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) Close() {
}

type testStreamEventAdapter struct {
	c   chan *binlogdatapb.BinlogTransaction
	ctx context.Context
}

func (t *testStreamEventAdapter) Recv() (*binlogdatapb.BinlogTransaction, error) {
	select {
	case bt := <-t.c:
		return bt, nil
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	}
}

// StreamTables is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset) (binlogplayer.BinlogTransactionStream, error) {
	actualTables := strings.Join(tables, ",")
	if actualTables != fbc.expectedTables {
		return nil, fmt.Errorf("Got wrong tables %v, expected %v", actualTables, fbc.expectedTables)
	}
	return &testStreamEventAdapter{c: fbc.tablesChannel, ctx: ctx}, nil
}

// StreamKeyRange is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset) (binlogplayer.BinlogTransactionStream, error) {
	actualKeyRange := key.KeyRangeString(keyRange)
	if actualKeyRange != fbc.expectedKeyRange {
		return nil, fmt.Errorf("Got wrong keyrange %v, expected %v", actualKeyRange, fbc.expectedKeyRange)
	}
	return &testStreamEventAdapter{c: fbc.keyRangeChannel, ctx: ctx}, nil
}

// fakeTabletConn implement TabletConn interface. We only care about the
// health check part.
type fakeTabletConn struct {
	queryservice.QueryService
	tablet *topodatapb.Tablet
}

// StreamHealth is part of queryservice.QueryService.
func (ftc *fakeTabletConn) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	callback(&querypb.StreamHealthResponse{
		Serving: true,
		Target: &querypb.Target{
			Keyspace:   ftc.tablet.Keyspace,
			Shard:      ftc.tablet.Shard,
			TabletType: ftc.tablet.Type,
		},
		RealtimeStats: &querypb.RealtimeStats{},
	})
	return nil
}

// createSourceTablet is a helper method to create the source tablet
// in the given keyspace/shard.
func createSourceTablet(t *testing.T, name string, ts *topo.Server, keyspace, shard string) {
	vshard, kr, err := topo.ValidateShardName(shard)
	if err != nil {
		t.Fatalf("ValidateShardName(%v) failed: %v", shard, err)
	}

	ctx := context.Background()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Keyspace: keyspace,
		Shard:    vshard,
		Type:     topodatapb.TabletType_REPLICA,
		KeyRange: kr,
		PortMap: map[string]int32{
			"vt": 80,
		},
	}
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}

	// register a tablet conn dialer that will return the instance
	// we want
	tabletconn.RegisterDialer(name, func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		return &fakeTabletConn{
			QueryService: fakes.ErrorQueryService,
			tablet:       tablet,
		}, nil
	})
	flag.Set("tablet_protocol", name)
}

// checkBlpPositionList will ask the BinlogPlayerMap for its BlpPositionList,
// and check it contains one entry with the right data.
func checkBlpPositionList(t *testing.T, bpm *BinlogPlayerMap, vtClientSyncChannel chan *binlogplayer.VtClientMock) {
	// ask for BlpPositionList, make sure we got what we expect
	go func() {
		vtcm := binlogplayer.NewVtClientMock()
		vtcm.AddResult(&sqltypes.Result{
			Fields:       nil,
			RowsAffected: 1,
			InsertID:     0,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.NewVarBinary("MariaDB/0-1-1235"),
					sqltypes.NewVarBinary(""),
				},
			},
		})
		vtClientSyncChannel <- vtcm
	}()
	bpl, err := bpm.BlpPositionList()
	if err != nil {
		t.Errorf("BlpPositionList failed: %v", err)
		return
	}
	if len(bpl) != 1 ||
		bpl[0].Uid != 1 ||
		bpl[0].Position != "MariaDB/0-1-1235" {
		t.Errorf("unexpected BlpPositionList: %v", bpl)
	}
}

// mockedThrottlerSettings is the mocked out query result when filtered
// replication reads the throttler settings from the DB.
var mockedThrottlerSettings = &sqltypes.Result{
	Fields:       nil,
	RowsAffected: 1,
	InsertID:     0,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.NewVarBinary("9223372036854775807"), // max_tps
			sqltypes.NewVarBinary("9223372036854775807"), // max_replication_lag
		},
	},
}

func TestBinlogPlayerMapHorizontalSplit(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	ctx := context.Background()

	// create the keyspace, a full set of covering shards,
	// and a new split destination shard.
	if err := ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		ShardingColumnName: "sharding_key",
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	for _, shard := range []string{"-80", "80-", "40-60"} {
		if err := ts.CreateShard(ctx, "ks", shard); err != nil {
			t.Fatalf("CreateShard failed: %v", err)
		}
	}

	// create one replica remote tablet in source shard, we will
	// use it as a source for filtered replication.
	createSourceTablet(t, "test_horizontal", ts, "ks", "-80")

	// register a binlog player factory that will return the instances
	// we want
	clientSyncChannel := make(chan *fakeBinlogClient)
	binlogplayer.RegisterClientFactory("test_horizontal", func() binlogplayer.Client {
		return <-clientSyncChannel
	})
	flag.Set("binlog_player_protocol", "test_horizontal")

	// create the BinlogPlayerMap on the local tablet
	// (note that local tablet is never in the topology, we don't
	// need it there at all)
	mysqlDaemon := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}
	vtClientSyncChannel := make(chan *binlogplayer.VtClientMock)
	bpm := NewBinlogPlayerMap(ts, mysqlDaemon, func() binlogplayer.VtClient {
		return <-vtClientSyncChannel
	})

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  1,
		},
		KeyRange: &topodatapb.KeyRange{
			Start: []byte{0x40},
			End:   []byte{0x60},
		},
		Keyspace: "ks",
		Shard:    "40-60",
	}

	si, err := ts.GetShard(ctx, "ks", "40-60")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	// no source shard for the shard, not adding players
	bpm.RefreshMap(ctx, tablet, si)
	if bpm.isRunningFilteredReplication() {
		t.Errorf("isRunningFilteredReplication should be false")
	}
	if mysqlDaemon.BinlogPlayerEnabled {
		t.Errorf("mysqlDaemon.BinlogPlayerEnabled should be false")
	}

	// now add the source in shard
	si, err = ts.UpdateShardFields(ctx, si.Keyspace(), si.ShardName(), func(si *topo.ShardInfo) error {
		si.SourceShards = []*topodatapb.Shard_SourceShard{
			{
				Uid:      1,
				Keyspace: "ks",
				Shard:    "-80",
				KeyRange: &topodatapb.KeyRange{
					End: []byte{0x80},
				},
			},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// now we have a source, adding players
	bpm.RefreshMap(ctx, tablet, si)
	if !bpm.isRunningFilteredReplication() {
		t.Errorf("isRunningFilteredReplication should be true")
	}

	// write a mocked vtClientMock that will be used to read the
	// start position at first. Note this also synchronizes the player,
	// so we can then check mysqlDaemon.BinlogPlayerEnabled.
	vtClientMock := binlogplayer.NewVtClientMock()
	vtClientMock.AddResult(&sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1234"),
				sqltypes.NewVarBinary(""),
			},
		},
	})

	// Mock out the throttler settings result.
	vtClientMock.AddResult(mockedThrottlerSettings)

	vtClientSyncChannel <- vtClientMock
	if !mysqlDaemon.BinlogPlayerEnabled {
		t.Errorf("mysqlDaemon.BinlogPlayerEnabled should be true")
	}

	// the client will then try to connect to the remote tablet.
	// give it what it needs.
	fbc := newFakeBinlogClient(t, 100)
	fbc.expectedKeyRange = "40-60"
	clientSyncChannel <- fbc

	// now we can feed an event through the fake connection
	vtClientMock.CommitChannel = make(chan []string)
	fbc.keyRangeChannel <- &binlogdatapb.BinlogTransaction{
		Statements: []*binlogdatapb.BinlogTransaction_Statement{
			{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("INSERT INTO tablet VALUES(1)"),
			},
		},
		EventToken: &querypb.EventToken{
			Timestamp: 72,
			Position:  "MariaDB/0-1-1235",
		},
	}

	// and make sure it results in a committed statement
	sql := <-vtClientMock.CommitChannel
	if len(sql) != 6 ||
		sql[0] != "SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
		sql[1] != "SELECT max_tps, max_replication_lag FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
		sql[2] != "BEGIN" ||
		!strings.HasPrefix(sql[3], "UPDATE _vt.blp_checkpoint SET pos='MariaDB/0-1-1235', time_updated=") ||
		!strings.HasSuffix(sql[3], ", transaction_timestamp=72 WHERE source_shard_uid=1") ||
		sql[4] != "INSERT INTO tablet VALUES(1)" ||
		sql[5] != "COMMIT" {
		t.Errorf("Got wrong SQL: %#v", sql)
	}

	// ask for status, make sure we got what we expect
	s := bpm.Status()
	if s.State != "Running" ||
		len(s.Controllers) != 1 ||
		s.Controllers[0].Index != 1 ||
		s.Controllers[0].State != "Running" ||
		s.Controllers[0].SourceShard.Keyspace != "ks" ||
		s.Controllers[0].SourceShard.Shard != "-80" ||
		s.Controllers[0].LastError != "" {
		t.Errorf("unexpected state: %v", s)
	}

	// check BlpPositionList API from BinlogPlayerMap
	checkBlpPositionList(t, bpm, vtClientSyncChannel)

	// now stop the binlog player map, by removing the source shard.
	// this will stop the player, which will cancel its context,
	// and exit the fake streaming connection.
	si.SourceShards = nil
	bpm.RefreshMap(ctx, tablet, si)
	if bpm.isRunningFilteredReplication() {
		t.Errorf("isRunningFilteredReplication should be false")
	}
	s = bpm.Status()
	if s.State != "Running" ||
		len(s.Controllers) != 0 {
		t.Errorf("unexpected state: %v", s)
	}

	// now just stop the map
	bpm.Stop()
	s = bpm.Status()
	if s.State != "Stopped" ||
		len(s.Controllers) != 0 {
		t.Errorf("unexpected state: %v", s)
	}
}

func TestBinlogPlayerMapHorizontalSplitStopStartUntil(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	ctx := context.Background()

	// create the keyspace, a full set of covering shards,
	// and a new split destination shard.
	if err := ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		ShardingColumnName: "sharding_key",
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	for _, shard := range []string{"-80", "80-", "40-60"} {
		if err := ts.CreateShard(ctx, "ks", shard); err != nil {
			t.Fatalf("CreateShard failed: %v", err)
		}
	}

	// create one replica remote tablet in source shard, we will
	// use it as a source for filtered replication.
	createSourceTablet(t, "test_horizontal_until", ts, "ks", "-80")

	// register a binlog player factory that will return the instances
	// we want
	clientSyncChannel := make(chan *fakeBinlogClient)
	binlogplayer.RegisterClientFactory("test_horizontal_until", func() binlogplayer.Client {
		return <-clientSyncChannel
	})
	flag.Set("binlog_player_protocol", "test_horizontal_until")

	// create the BinlogPlayerMap on the local tablet
	// (note that local tablet is never in the topology, we don't
	// need it there at all)
	mysqlDaemon := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}
	vtClientSyncChannel := make(chan *binlogplayer.VtClientMock)
	bpm := NewBinlogPlayerMap(ts, mysqlDaemon, func() binlogplayer.VtClient {
		return <-vtClientSyncChannel
	})

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  1,
		},
		KeyRange: &topodatapb.KeyRange{
			Start: []byte{0x40},
			End:   []byte{0x60},
		},
		Keyspace: "ks",
		Shard:    "40-60",
	}

	si, err := ts.UpdateShardFields(ctx, "ks", "40-60", func(si *topo.ShardInfo) error {
		si.SourceShards = []*topodatapb.Shard_SourceShard{
			{
				Uid:      1,
				Keyspace: "ks",
				Shard:    "-80",
				KeyRange: &topodatapb.KeyRange{
					End: []byte{0x80},
				},
			},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// now we have a source, adding players
	bpm.RefreshMap(ctx, tablet, si)
	if !bpm.isRunningFilteredReplication() {
		t.Errorf("isRunningFilteredReplication should be true")
	}

	// write a mocked vtClientMock that will be used to read the
	// start position at first. Note this also synchronizes the player,
	// so we can then check mysqlDaemon.BinlogPlayerEnabled.
	startPos := &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1234"),
				sqltypes.NewVarBinary(""),
			},
		},
	}
	vtClientMock := binlogplayer.NewVtClientMock()
	vtClientMock.AddResult(startPos)

	// Mock out the throttler settings result.
	vtClientMock.AddResult(mockedThrottlerSettings)
	// Mock two more results since the BinlogPlayer will be stopped and then
	// restarted again with the RunUntil() call.
	vtClientMock.AddResult(startPos)
	vtClientMock.AddResult(mockedThrottlerSettings)

	vtClientSyncChannel <- vtClientMock
	if !mysqlDaemon.BinlogPlayerEnabled {
		t.Errorf("mysqlDaemon.BinlogPlayerEnabled should be true")
	}

	// the client will then try to connect to the remote tablet.
	// give it what it needs.
	fbc := newFakeBinlogClient(t, 100)
	fbc.expectedKeyRange = "40-60"
	clientSyncChannel <- fbc

	// now stop the map
	bpm.Stop()
	s := bpm.Status()
	if s.State != "Stopped" ||
		len(s.Controllers) != 1 ||
		s.Controllers[0].State != "Stopped" {
		t.Errorf("unexpected state: %v", s)
	}

	// in the background, start a function that will do what's needed
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		// the client will first try to read the current position again
		vtClientSyncChannel <- vtClientMock

		// the client will then try to connect to the remote tablet.
		// give it what it needs.
		fbc := newFakeBinlogClient(t, 100)
		fbc.expectedKeyRange = "40-60"
		clientSyncChannel <- fbc

		// feed an event through the fake connection
		vtClientMock.CommitChannel = make(chan []string)
		fbc.keyRangeChannel <- &binlogdatapb.BinlogTransaction{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
					Sql:      []byte("INSERT INTO tablet VALUES(1)"),
				},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 72,
				Position:  "MariaDB/0-1-1235",
			},
		}

		// and make sure it results in a committed statement
		sql := <-vtClientMock.CommitChannel
		if len(sql) != 8 ||
			sql[0] != "SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
			sql[1] != "SELECT max_tps, max_replication_lag FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
			sql[2] != "SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
			sql[3] != "SELECT max_tps, max_replication_lag FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
			sql[4] != "BEGIN" ||
			!strings.HasPrefix(sql[5], "UPDATE _vt.blp_checkpoint SET pos='MariaDB/0-1-1235', time_updated=") ||
			!strings.HasSuffix(sql[5], ", transaction_timestamp=72 WHERE source_shard_uid=1") ||
			sql[6] != "INSERT INTO tablet VALUES(1)" ||
			sql[7] != "COMMIT" {
			t.Errorf("Got wrong SQL: %#v", sql)
		}
		wg.Done()
	}()

	// now restart the map until we get the right BlpPosition
	mysqlDaemon.BinlogPlayerEnabled = false
	ctx1, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := bpm.RunUntil(ctx1, []*tabletmanagerdatapb.BlpPosition{
		{
			Uid:      1,
			Position: "MariaDB/0-1-1235",
		},
	}, 5*time.Second); err != nil {
		t.Fatalf("RunUntil failed: %v", err)
	}

	// make sure the background function is done
	wg.Wait()

	// ask for status, make sure we got what we expect
	s = bpm.Status()
	if s.State != "Stopped" ||
		len(s.Controllers) != 1 ||
		s.Controllers[0].Index != 1 ||
		s.Controllers[0].State != "Stopped" ||
		s.Controllers[0].LastError != "" {
		t.Errorf("unexpected state: %v", s)
	}

	// check BlpPositionList API from BinlogPlayerMap
	checkBlpPositionList(t, bpm, vtClientSyncChannel)
}

func TestBinlogPlayerMapVerticalSplit(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	ctx := context.Background()

	// create the keyspaces, with one shard each
	if err := ts.CreateKeyspace(ctx, "source", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	if err := ts.CreateKeyspace(ctx, "destination", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	for _, keyspace := range []string{"source", "destination"} {
		if err := ts.CreateShard(ctx, keyspace, "0"); err != nil {
			t.Fatalf("CreateShard failed: %v", err)
		}
	}

	// create one replica remote tablet in source keyspace, we will
	// use it as a source for filtered replication.
	createSourceTablet(t, "test_vertical", ts, "source", "0")

	// register a binlog player factory that will return the instances
	// we want
	clientSyncChannel := make(chan *fakeBinlogClient)
	binlogplayer.RegisterClientFactory("test_vertical", func() binlogplayer.Client {
		return <-clientSyncChannel
	})
	flag.Set("binlog_player_protocol", "test_vertical")

	// create the BinlogPlayerMap on the local tablet
	// (note that local tablet is never in the topology, we don't
	// need it there at all)
	// The schema will be used to resolve the table wildcards.
	mysqlDaemon := &fakemysqldaemon.FakeMysqlDaemon{
		MysqlPort: 3306,
		Schema: &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:              "table1",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name:              "funtables_one",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name:              "excluded_table",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
			},
		},
	}
	vtClientSyncChannel := make(chan *binlogplayer.VtClientMock)
	bpm := NewBinlogPlayerMap(ts, mysqlDaemon, func() binlogplayer.VtClient {
		return <-vtClientSyncChannel
	})

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  1,
		},
		Keyspace: "destination",
		Shard:    "0",
	}

	si, err := ts.UpdateShardFields(ctx, "destination", "0", func(si *topo.ShardInfo) error {
		si.SourceShards = []*topodatapb.Shard_SourceShard{
			{
				Uid:      1,
				Keyspace: "source",
				Shard:    "0",
				Tables: []string{
					"table1",
					"/funtables_/",
				},
			},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// now we have a source, adding players
	bpm.RefreshMap(ctx, tablet, si)
	if !bpm.isRunningFilteredReplication() {
		t.Errorf("isRunningFilteredReplication should be true")
	}

	// write a mocked vtClientMock that will be used to read the
	// start position at first. Note this also synchronizes the player,
	// so we can then check mysqlDaemon.BinlogPlayerEnabled.
	vtClientMock := binlogplayer.NewVtClientMock()
	vtClientMock.AddResult(&sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1234"),
				sqltypes.NewVarBinary(""),
			},
		},
	})

	// Mock out the throttler settings result.
	vtClientMock.AddResult(mockedThrottlerSettings)

	vtClientSyncChannel <- vtClientMock
	if !mysqlDaemon.BinlogPlayerEnabled {
		t.Errorf("mysqlDaemon.BinlogPlayerEnabled should be true")
	}
	// the client will then try to connect to the remote tablet.
	// give it what it needs.
	fbc := newFakeBinlogClient(t, 100)
	fbc.expectedTables = "table1,funtables_one"
	clientSyncChannel <- fbc

	// now we can feed an event through the fake connection
	vtClientMock.CommitChannel = make(chan []string)
	fbc.tablesChannel <- &binlogdatapb.BinlogTransaction{
		Statements: []*binlogdatapb.BinlogTransaction_Statement{
			{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				Sql:      []byte("INSERT INTO tablet VALUES(1)"),
			},
		},
		EventToken: &querypb.EventToken{
			Timestamp: 72,
			Position:  "MariaDB/0-1-1235",
		},
	}

	// and make sure it results in a committed statement
	sql := <-vtClientMock.CommitChannel
	if len(sql) != 6 ||
		sql[0] != "SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
		sql[1] != "SELECT max_tps, max_replication_lag FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
		sql[2] != "BEGIN" ||
		!strings.HasPrefix(sql[3], "UPDATE _vt.blp_checkpoint SET pos='MariaDB/0-1-1235', time_updated=") ||
		!strings.HasSuffix(sql[3], ", transaction_timestamp=72 WHERE source_shard_uid=1") ||
		sql[4] != "INSERT INTO tablet VALUES(1)" ||
		sql[5] != "COMMIT" {
		t.Errorf("Got wrong SQL: %#v", sql)
	}

	// ask for status, make sure we got what we expect
	s := bpm.Status()
	if s.State != "Running" ||
		len(s.Controllers) != 1 ||
		s.Controllers[0].Index != 1 ||
		s.Controllers[0].State != "Running" ||
		s.Controllers[0].SourceShard.Keyspace != "source" ||
		s.Controllers[0].SourceShard.Shard != "0" ||
		s.Controllers[0].LastError != "" {
		t.Errorf("unexpected state: %v %v", s, s.Controllers[0])
	}

	// check BlpPositionList API from BinlogPlayerMap
	checkBlpPositionList(t, bpm, vtClientSyncChannel)

	// now stop the binlog player map.
	// this will stop the player, which will cancel its context,
	// and exit the fake streaming connection.
	bpm.Stop()
	s = bpm.Status()
	if s.State != "Stopped" ||
		len(s.Controllers) != 1 ||
		s.Controllers[0].State != "Stopped" {
		t.Errorf("unexpected state: %v", s)
	}
}
