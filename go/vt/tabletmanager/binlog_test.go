package tabletmanager

import (
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/zktopo"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	tablesChannel  chan *proto.BinlogTransaction

	expectedKeyRange string
	keyRangeChannel  chan *proto.BinlogTransaction
}

func newFakeBinlogClient(t *testing.T, expectedDialUID uint32) *fakeBinlogClient {
	return &fakeBinlogClient{
		t:               t,
		expectedDialUID: expectedDialUID,

		tablesChannel:   make(chan *proto.BinlogTransaction),
		keyRangeChannel: make(chan *proto.BinlogTransaction),
	}
}

// Dial is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) Dial(endPoint *pb.EndPoint, connTimeout time.Duration) error {
	if fbc.expectedDialUID != endPoint.Uid {
		fbc.t.Errorf("fakeBinlogClient.Dial expected uid %v got %v", fbc.expectedDialUID, endPoint.Uid)
	}
	return nil
}

// Close is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) Close() {
}

// ServeUpdateStream is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) ServeUpdateStream(ctx context.Context, position string) (chan *proto.StreamEvent, binlogplayer.ErrFunc, error) {
	return nil, nil, fmt.Errorf("Should never be called")
}

// StreamTables is part of the binlogplayer.Client interface
func (fbc *fakeBinlogClient) StreamTables(ctx context.Context, position string, tables []string, charset *mproto.Charset) (chan *proto.BinlogTransaction, binlogplayer.ErrFunc, error) {
	actualTables := strings.Join(tables, ",")
	if actualTables != fbc.expectedTables {
		return nil, nil, fmt.Errorf("Got wrong tables %v, expected %v", actualTables, fbc.expectedTables)
	}

	c := make(chan *proto.BinlogTransaction)
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
func (fbc *fakeBinlogClient) StreamKeyRange(ctx context.Context, position string, keyspaceIDType key.KeyspaceIdType, keyRange *pb.KeyRange, charset *mproto.Charset) (chan *proto.BinlogTransaction, binlogplayer.ErrFunc, error) {
	actualKeyRange := key.KeyRangeString(keyRange)
	if actualKeyRange != fbc.expectedKeyRange {
		return nil, nil, fmt.Errorf("Got wrong keyrange %v, expected %v", actualKeyRange, fbc.expectedKeyRange)
	}

	c := make(chan *proto.BinlogTransaction)
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

// checkBlpPositionList will ask the BinlogPlayerMap for its BlpPositionList,
// and check it contains one entry with the right data.
func checkBlpPositionList(t *testing.T, bpm *BinlogPlayerMap, vtClientSyncChannel chan *binlogplayer.VtClientMock) {
	// ask for BlpPositionList, make sure we got what we expect
	go func() {
		vtcm := binlogplayer.NewVtClientMock()
		vtcm.Result = &mproto.QueryResult{
			Fields:       nil,
			RowsAffected: 1,
			InsertId:     0,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
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
	if len(bpl.Entries) != 1 ||
		bpl.Entries[0].Uid != 1 ||
		bpl.Entries[0].Position.GTIDSet.(myproto.MariadbGTID).Domain != 0 ||
		bpl.Entries[0].Position.GTIDSet.(myproto.MariadbGTID).Server != 1 ||
		bpl.Entries[0].Position.GTIDSet.(myproto.MariadbGTID).Sequence != 1235 {
		t.Errorf("unexpected BlpPositionList: %v", bpl)
	}
}

func TestBinlogPlayerMapHorizontalSplit(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1"})
	ctx := context.Background()

	// create the keyspace, a full set of covering shards,
	// and a new split destination shard.
	keyspace := "ks"
	if err := ts.CreateKeyspace(ctx, keyspace, &pb.Keyspace{
		ShardingColumnType: pb.KeyspaceIdType_UINT64,
		ShardingColumnName: "sharding_key",
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	for _, shard := range []string{"-80", "80-", "40-60"} {
		if err := ts.CreateShard(ctx, keyspace, shard); err != nil {
			t.Fatalf("CreateShard failed: %v", err)
		}
	}

	// create one replica remote tablet in source shard, we will
	// use it as a source for filtered replication.
	tablet := &pb.Tablet{
		Alias: &pb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: pb.TabletType_REPLICA,
		KeyRange: &pb.KeyRange{
			End: []byte{0x80},
		},
		Keyspace: keyspace,
		Shard:    "-80",
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

	tablet = &pb.Tablet{
		Alias: &pb.TabletAlias{
			Cell: "cell1",
			Uid:  1,
		},
		KeyRange: &pb.KeyRange{
			Start: []byte{0x40},
			End:   []byte{0x60},
		},
		Keyspace: keyspace,
		Shard:    "40-60",
	}

	ki, err := ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		t.Fatalf("GetKeyspace failed: %v", err)
	}
	si, err := ts.GetShard(ctx, keyspace, "40-60")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	// no source shard for the shard, not adding players
	bpm.RefreshMap(ctx, tablet, ki, si)
	if bpm.isRunningFilteredReplication() {
		t.Errorf("isRunningFilteredReplication should be false")
	}
	if mysqlDaemon.BinlogPlayerEnabled {
		t.Errorf("mysqlDaemon.BinlogPlayerEnabled should be false")
	}

	// now add the source in shard
	si.SourceShards = []*pb.Shard_SourceShard{
		&pb.Shard_SourceShard{
			Uid:      1,
			Keyspace: keyspace,
			Shard:    "-80",
			KeyRange: &pb.KeyRange{
				End: []byte{0x80},
			},
		},
	}
	if err := ts.UpdateShard(ctx, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
	}

	// now we have a source, adding players
	bpm.RefreshMap(ctx, tablet, ki, si)
	if !bpm.isRunningFilteredReplication() {
		t.Errorf("isRunningFilteredReplication should be true")
	}

	// write a mocked vtClientMock that will be used to read the
	// start position at first. Note this also synchronizes the player,
	// so we can then check mysqlDaemon.BinlogPlayerEnabled.
	vtClientMock := binlogplayer.NewVtClientMock()
	vtClientMock.CommitChannel = make(chan []string)
	vtClientMock.Result = &mproto.QueryResult{
		Fields:       nil,
		RowsAffected: 1,
		InsertId:     0,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
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
	fbc.keyRangeChannel <- &proto.BinlogTransaction{
		Statements: []proto.Statement{
			proto.Statement{
				Category: proto.BL_DML,
				Sql:      "INSERT INTO tablet VALUES(1)",
			},
		},
		Timestamp:     72,
		TransactionID: "MariaDB/0-1-1235",
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
		s.Controllers[0].SourceShard.Keyspace != keyspace ||
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
	bpm.RefreshMap(ctx, tablet, ki, si)
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

func TestBinlogPlayerMapVerticalSplit(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1"})
	ctx := context.Background()

	// create the keyspaces, with one shard each
	if err := ts.CreateKeyspace(ctx, "source", &pb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	if err := ts.CreateKeyspace(ctx, "destination", &pb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	for _, keyspace := range []string{"source", "destination"} {
		if err := ts.CreateShard(ctx, keyspace, "0"); err != nil {
			t.Fatalf("CreateShard failed: %v", err)
		}
	}

	// create one replica remote tablet in source keyspace, we will
	// use it as a source for filtered replication.
	tablet := &pb.Tablet{
		Alias: &pb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type:     pb.TabletType_REPLICA,
		Keyspace: "source",
		Shard:    "0",
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
		Schema: &myproto.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*myproto.TableDefinition{
				&myproto.TableDefinition{
					Name:              "table1",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              myproto.TableBaseTable,
				},
				&myproto.TableDefinition{
					Name:              "funtables_one",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              myproto.TableBaseTable,
				},
				&myproto.TableDefinition{
					Name:              "excluded_table",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              myproto.TableBaseTable,
				},
			},
		},
	}
	vtClientSyncChannel := make(chan *binlogplayer.VtClientMock)
	bpm := NewBinlogPlayerMap(ts, mysqlDaemon, func() binlogplayer.VtClient {
		return <-vtClientSyncChannel
	})

	tablet = &pb.Tablet{
		Alias: &pb.TabletAlias{
			Cell: "cell1",
			Uid:  1,
		},
		KeyRange: &pb.KeyRange{
			Start: []byte{0x40},
			End:   []byte{0x60},
		},
		Keyspace: "destination",
		Shard:    "0",
	}

	ki, err := ts.GetKeyspace(ctx, "destination")
	if err != nil {
		t.Fatalf("GetKeyspace failed: %v", err)
	}
	si, err := ts.GetShard(ctx, "destination", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.SourceShards = []*pb.Shard_SourceShard{
		&pb.Shard_SourceShard{
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
	bpm.RefreshMap(ctx, tablet, ki, si)
	if !bpm.isRunningFilteredReplication() {
		t.Errorf("isRunningFilteredReplication should be true")
	}

	// write a mocked vtClientMock that will be used to read the
	// start position at first. Note this also synchronizes the player,
	// so we can then check mysqlDaemon.BinlogPlayerEnabled.
	vtClientMock := binlogplayer.NewVtClientMock()
	vtClientMock.CommitChannel = make(chan []string)
	vtClientMock.Result = &mproto.QueryResult{
		Fields:       nil,
		RowsAffected: 1,
		InsertId:     0,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
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
	fbc.tablesChannel <- &proto.BinlogTransaction{
		Statements: []proto.Statement{
			proto.Statement{
				Category: proto.BL_DML,
				Sql:      "INSERT INTO tablet VALUES(1)",
			},
		},
		Timestamp:     72,
		TransactionID: "MariaDB/0-1-1235",
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
