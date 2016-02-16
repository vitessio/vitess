// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

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
// find valid endpoints, so we have to update the EndPoints.
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
func (fbc *fakeBinlogClient) Dial(endPoint *topodatapb.EndPoint, connTimeout time.Duration) error {
	if fbc.expectedDialUID != endPoint.Uid {
		fbc.t.Errorf("fakeBinlogClient.Dial expected uid %v got %v", fbc.expectedDialUID, endPoint.Uid)
	}
	return nil
}

// Close is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) Close() {
}

// ServeUpdateStream is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) ServeUpdateStream(ctx context.Context, position string) (chan *binlogdatapb.StreamEvent, binlogplayer.ErrFunc, error) {
	return nil, nil, fmt.Errorf("Should never be called")
}

// StreamTables is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset) (chan *binlogdatapb.BinlogTransaction, binlogplayer.ErrFunc, error) {
	actualTables := strings.Join(tables, ",")
	if actualTables != fbc.expectedTables {
		return nil, nil, fmt.Errorf("Got wrong tables %v, expected %v", actualTables, fbc.expectedTables)
	}

	c := make(chan *binlogdatapb.BinlogTransaction)
	var finalErr error
	go func() {
		for {
			select {
			case bt := <-fbc.tablesChannel:
				c <- bt
			case <-ctx.Done():
				finalErr = ctx.Err()
				close(c)
				return
			}
		}
	}()
	return c, func() error {
		return finalErr
	}, nil
}

// StreamKeyRange is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset) (chan *binlogdatapb.BinlogTransaction, binlogplayer.ErrFunc, error) {
	actualKeyRange := key.KeyRangeString(keyRange)
	if actualKeyRange != fbc.expectedKeyRange {
		return nil, nil, fmt.Errorf("Got wrong keyrange %v, expected %v", actualKeyRange, fbc.expectedKeyRange)
	}

	c := make(chan *binlogdatapb.BinlogTransaction)
	var finalErr error
	go func() {
		for {
			select {
			case bt := <-fbc.keyRangeChannel:
				c <- bt
			case <-ctx.Done():
				finalErr = ctx.Err()
				close(c)
				return
			}
		}
	}()
	return c, func() error {
		return finalErr
	}, nil
}

// fakeTabletConn implement TabletConn interface. We only care about the
// health check part.
type fakeTabletConn struct {
	endPoint   *topodatapb.EndPoint
	keyspace   string
	shard      string
	tabletType topodatapb.TabletType
}

// Execute is part of the TabletConn interface
func (ftc *fakeTabletConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("not implemented in this test")
}

// Execute is part of the TabletConn interface
func (ftc *fakeTabletConn) ExecuteBatch(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	return nil, fmt.Errorf("not implemented in this test")
}

// StreamExecute is part of the TabletConn interface
func (ftc *fakeTabletConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	return nil, nil, fmt.Errorf("not implemented in this test")
}

// Begin is part of the TabletConn interface
func (ftc *fakeTabletConn) Begin(ctx context.Context) (transactionID int64, err error) {
	return 0, fmt.Errorf("not implemented in this test")
}

// Commit is part of the TabletConn interface
func (ftc *fakeTabletConn) Commit(ctx context.Context, transactionID int64) error {
	return fmt.Errorf("not implemented in this test")
}

// Rollback is part of the TabletConn interface
func (ftc *fakeTabletConn) Rollback(ctx context.Context, transactionID int64) error {
	return fmt.Errorf("not implemented in this test")
}

// Execute2 is part of the TabletConn interface
func (ftc *fakeTabletConn) Execute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("not implemented in this test")
}

// ExecuteBatch2 is part of the TabletConn interface
func (ftc *fakeTabletConn) ExecuteBatch2(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	return nil, fmt.Errorf("not implemented in this test")
}

// Begin2 is part of the TabletConn interface
func (ftc *fakeTabletConn) Begin2(ctx context.Context) (transactionID int64, err error) {
	return 0, fmt.Errorf("not implemented in this test")
}

// Commit2 is part of the TabletConn interface
func (ftc *fakeTabletConn) Commit2(ctx context.Context, transactionID int64) error {
	return fmt.Errorf("not implemented in this test")
}

// Rollback2 is part of the TabletConn interface
func (ftc *fakeTabletConn) Rollback2(ctx context.Context, transactionID int64) error {
	return fmt.Errorf("not implemented in this test")
}

// StreamExecute2 is part of the TabletConn interface
func (ftc *fakeTabletConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	return nil, nil, fmt.Errorf("not implemented in this test")
}

// Close is part of the TabletConn interface
func (ftc *fakeTabletConn) Close() {
}

// SetTarget is part of the TabletConn interface
func (ftc *fakeTabletConn) SetTarget(keyspace, shard string, tabletType topodatapb.TabletType) error {
	return fmt.Errorf("not implemented in this test")
}

// EndPoint is part of the TabletConn interface
func (ftc *fakeTabletConn) EndPoint() *topodatapb.EndPoint {
	return ftc.endPoint
}

// SplitQuery is part of the TabletConn interface
func (ftc *fakeTabletConn) SplitQuery(ctx context.Context, query querytypes.BoundQuery, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	return nil, fmt.Errorf("not implemented in this test")
}

// StreamHealth is part of the TabletConn interface
func (ftc *fakeTabletConn) StreamHealth(ctx context.Context) (<-chan *querypb.StreamHealthResponse, tabletconn.ErrFunc, error) {
	c := make(chan *querypb.StreamHealthResponse)
	var finalErr error
	go func() {
		c <- &querypb.StreamHealthResponse{
			Serving: true,
			Target: &querypb.Target{
				Keyspace:   ftc.keyspace,
				Shard:      ftc.shard,
				TabletType: ftc.tabletType,
			},
			RealtimeStats: &querypb.RealtimeStats{},
		}
		select {
		case <-ctx.Done():
			finalErr = ctx.Err()
			close(c)
		}
	}()
	return c, func() error {
		return finalErr
	}, nil
}

// createSourceTablet is a helper method to create the source tablet
// in the given keyspace/shard.
func createSourceTablet(t *testing.T, name string, ts topo.Server, keyspace, shard string) {
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
		Type:     topodatapb.TabletType_REPLICA,
		KeyRange: kr,
		Keyspace: keyspace,
		Shard:    vshard,
		PortMap: map[string]int32{
			"vt": 80,
		},
	}
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}
	if err := topotools.UpdateTabletEndpoints(ctx, ts, tablet); err != nil {
		t.Fatalf("topotools.UpdateTabletEndpoints failed: %v", err)
	}

	// register a tablet conn dialer that will return the instance
	// we want
	tabletconn.RegisterDialer(name, func(ctx context.Context, endPoint *topodatapb.EndPoint, k, s string, tabletType topodatapb.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
		return &fakeTabletConn{
			endPoint:   endPoint,
			keyspace:   keyspace,
			shard:      vshard,
			tabletType: topodatapb.TabletType_REPLICA,
		}, nil
	})
	flag.Lookup("tablet_protocol").Value.Set(name)
}

// checkBlpPositionList will ask the BinlogPlayerMap for its BlpPositionList,
// and check it contains one entry with the right data.
func checkBlpPositionList(t *testing.T, bpm *BinlogPlayerMap, vtClientSyncChannel chan *binlogplayer.VtClientMock) {
	// ask for BlpPositionList, make sure we got what we expect
	go func() {
		vtcm := binlogplayer.NewVtClientMock()
		vtcm.Result = &sqltypes.Result{
			Fields:       nil,
			RowsAffected: 1,
			InsertID:     0,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("MariaDB/0-1-1235")),
					sqltypes.MakeString([]byte("")),
				},
			},
		}
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

func TestBinlogPlayerMapHorizontalSplit(t *testing.T) {
	ts := zktestserver.New(t, []string{"cell1"})
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
	flag.Lookup("binlog_player_protocol").Value.Set("test_horizontal")

	// create the BinlogPlayerMap on the local tablet
	// (note that local tablet is never in the topology, we don't
	// need it there at all)
	mysqlDaemon := &mysqlctl.FakeMysqlDaemon{MysqlPort: 3306}
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
	if err := ts.UpdateShard(ctx, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
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
	vtClientMock.Result = &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeString([]byte("MariaDB/0-1-1234")),
				sqltypes.MakeString([]byte("")),
			},
		},
	}
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
				Category: binlogdatapb.BinlogTransaction_Statement_BL_DML,
				Sql:      "INSERT INTO tablet VALUES(1)",
			},
		},
		Timestamp:     72,
		TransactionId: "MariaDB/0-1-1235",
	}

	// and make sure it results in a committed statement
	sql := <-vtClientMock.CommitChannel
	if len(sql) != 5 ||
		sql[0] != "SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
		sql[1] != "BEGIN" ||
		!strings.HasPrefix(sql[2], "UPDATE _vt.blp_checkpoint SET pos='MariaDB/0-1-1235', time_updated=") ||
		!strings.HasSuffix(sql[2], ", transaction_timestamp=72 WHERE source_shard_uid=1") ||
		sql[3] != "INSERT INTO tablet VALUES(1)" ||
		sql[4] != "COMMIT" {
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
	ts := zktestserver.New(t, []string{"cell1"})
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
	flag.Lookup("binlog_player_protocol").Value.Set("test_horizontal_until")

	// create the BinlogPlayerMap on the local tablet
	// (note that local tablet is never in the topology, we don't
	// need it there at all)
	mysqlDaemon := &mysqlctl.FakeMysqlDaemon{MysqlPort: 3306}
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
	if err := ts.UpdateShard(ctx, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
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
	vtClientMock.Result = &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeString([]byte("MariaDB/0-1-1234")),
				sqltypes.MakeString([]byte("")),
			},
		},
	}
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
					Category: binlogdatapb.BinlogTransaction_Statement_BL_DML,
					Sql:      "INSERT INTO tablet VALUES(1)",
				},
			},
			Timestamp:     72,
			TransactionId: "MariaDB/0-1-1235",
		}

		// and make sure it results in a committed statement
		sql := <-vtClientMock.CommitChannel
		if len(sql) != 6 ||
			sql[0] != "SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
			sql[1] != "SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
			sql[2] != "BEGIN" ||
			!strings.HasPrefix(sql[3], "UPDATE _vt.blp_checkpoint SET pos='MariaDB/0-1-1235', time_updated=") ||
			!strings.HasSuffix(sql[3], ", transaction_timestamp=72 WHERE source_shard_uid=1") ||
			sql[4] != "INSERT INTO tablet VALUES(1)" ||
			sql[5] != "COMMIT" {
			t.Errorf("Got wrong SQL: %#v", sql)
		}
		wg.Done()
	}()

	// now restart the map until we get the right BlpPosition
	mysqlDaemon.BinlogPlayerEnabled = false
	ctx1, _ := context.WithTimeout(ctx, 5*time.Second)
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
	ts := zktestserver.New(t, []string{"cell1"})
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
	flag.Lookup("binlog_player_protocol").Value.Set("test_vertical")

	// create the BinlogPlayerMap on the local tablet
	// (note that local tablet is never in the topology, we don't
	// need it there at all)
	// The schema will be used to resolve the table wildcards.
	mysqlDaemon := &mysqlctl.FakeMysqlDaemon{
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

	si, err := ts.GetShard(ctx, "destination", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.SourceShards = []*topodatapb.Shard_SourceShard{
		{
			Uid:      1,
			Keyspace: "source",
			Shard:    "0",
			Tables: []string{
				"table1",
				"funtables_*",
			},
		},
	}
	if err := ts.UpdateShard(ctx, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
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
	vtClientMock.Result = &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeString([]byte("MariaDB/0-1-1234")),
				sqltypes.MakeString([]byte("")),
			},
		},
	}
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
				Category: binlogdatapb.BinlogTransaction_Statement_BL_DML,
				Sql:      "INSERT INTO tablet VALUES(1)",
			},
		},
		Timestamp:     72,
		TransactionId: "MariaDB/0-1-1235",
	}

	// and make sure it results in a committed statement
	sql := <-vtClientMock.CommitChannel
	if len(sql) != 5 ||
		sql[0] != "SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=1" ||
		sql[1] != "BEGIN" ||
		!strings.HasPrefix(sql[2], "UPDATE _vt.blp_checkpoint SET pos='MariaDB/0-1-1235', time_updated=") ||
		!strings.HasSuffix(sql[2], ", transaction_timestamp=72 WHERE source_shard_uid=1") ||
		sql[3] != "INSERT INTO tablet VALUES(1)" ||
		sql[4] != "COMMIT" {
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
