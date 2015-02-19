// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	_ "github.com/youtube/vitess/go/vt/tabletserver/gorpctabletconn"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"
)

// This is a local SqlQuery RCP implementation to support the tests
type SqlQuery struct {
	t *testing.T
}

func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return nil
}

func (sq *SqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) error {
	// Custom parsing of the query we expect
	min := 100
	max := 200
	var err error
	parts := strings.Split(query.Sql, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "id>=") {
			min, err = strconv.Atoi(part[4:])
			if err != nil {
				return err
			}
		} else if strings.HasPrefix(part, "id<") {
			max, err = strconv.Atoi(part[3:])
		}
	}
	sq.t.Logf("SqlQuery: got query: %v with min %v max %v", *query, min, max)

	// Send the headers
	if err := sendReply(&mproto.QueryResult{
		Fields: []mproto.Field{
			mproto.Field{
				Name: "id",
				Type: mproto.VT_LONGLONG,
			},
			mproto.Field{
				Name: "msg",
				Type: mproto.VT_VARCHAR,
			},
			mproto.Field{
				Name: "keyspace_id",
				Type: mproto.VT_LONGLONG,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	ksids := []uint64{0x2000000000000000, 0x6000000000000000}
	for i := min; i < max; i++ {
		if err := sendReply(&mproto.QueryResult{
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("Text for %v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", ksids[i%2]))),
				},
			},
		}); err != nil {
			return err
		}
	}
	// SELECT id, msg, keyspace_id FROM table1 WHERE id>=180 AND id<190 ORDER BY id
	return nil
}

type ExpectedExecuteFetch struct {
	Query       string
	MaxRows     int
	WantFields  bool
	QueryResult *mproto.QueryResult
	Error       error
}

// FakePoolConnection implements dbconnpool.PoolConnection
type FakePoolConnection struct {
	t      *testing.T
	Closed bool

	ExpectedExecuteFetch      []ExpectedExecuteFetch
	ExpectedExecuteFetchIndex int
}

func NewFakePoolConnectionQuery(t *testing.T, query string, err error) *FakePoolConnection {
	return &FakePoolConnection{
		t: t,
		ExpectedExecuteFetch: []ExpectedExecuteFetch{
			ExpectedExecuteFetch{
				Query:       query,
				QueryResult: &mproto.QueryResult{},
				Error:       err,
			},
		},
	}
}

func (fpc *FakePoolConnection) ExecuteFetch(query string, maxrows int, wantfields bool) (*mproto.QueryResult, error) {
	if fpc.ExpectedExecuteFetchIndex >= len(fpc.ExpectedExecuteFetch) {
		fpc.t.Errorf("got unexpected out of bound fetch: %v >= %v", fpc.ExpectedExecuteFetchIndex, len(fpc.ExpectedExecuteFetch))
		return nil, fmt.Errorf("unexpected out of bound fetch")
	}
	expected := fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].Query
	if strings.HasSuffix(expected, "*") {
		if !strings.HasPrefix(query, expected[0:len(expected)-1]) {
			fpc.t.Errorf("got unexpected query start: %v != %v", query, expected)
			return nil, fmt.Errorf("unexpected query")
		}
	} else {
		if query != expected {
			fpc.t.Errorf("got unexpected query: %v != %v", query, expected)
			return nil, fmt.Errorf("unexpected query")
		}
	}
	fpc.t.Logf("ExecuteFetch: %v", query)
	defer func() {
		fpc.ExpectedExecuteFetchIndex++
	}()
	if fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].Error != nil {
		return nil, fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].Error
	}
	return fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].QueryResult, nil
}

func (fpc *FakePoolConnection) ExecuteStreamFetch(query string, callback func(*mproto.QueryResult) error, streamBufferSize int) error {
	return nil
}

func (fpc *FakePoolConnection) ID() int64 {
	return 1
}

func (fpc *FakePoolConnection) Close() {
	fpc.Closed = true
}

func (fpc *FakePoolConnection) IsClosed() bool {
	return fpc.Closed
}

func (fpc *FakePoolConnection) Recycle() {
}

func (fpc *FakePoolConnection) Reconnect() error {
	return nil
}

// on the source rdonly guy, should only have one query to find min & max
func SourceRdonlyFactory(t *testing.T) func() (dbconnpool.PoolConnection, error) {
	return func() (dbconnpool.PoolConnection, error) {
		return &FakePoolConnection{
			t: t,
			ExpectedExecuteFetch: []ExpectedExecuteFetch{
				ExpectedExecuteFetch{
					Query: "SELECT MIN(id), MAX(id) FROM vt_ks.table1",
					QueryResult: &mproto.QueryResult{
						Fields: []mproto.Field{
							mproto.Field{
								Name: "min",
								Type: mproto.VT_LONGLONG,
							},
							mproto.Field{
								Name: "max",
								Type: mproto.VT_LONGLONG,
							},
						},
						Rows: [][]sqltypes.Value{
							[]sqltypes.Value{
								sqltypes.MakeString([]byte("100")),
								sqltypes.MakeString([]byte("200")),
							},
						},
					},
				},
			},
		}, nil
	}
}

// on the destinations
func DestinationsFactory(t *testing.T, insertCount int64) func() (dbconnpool.PoolConnection, error) {
	var queryIndex int64 = -1

	return func() (dbconnpool.PoolConnection, error) {
		qi := atomic.AddInt64(&queryIndex, 1)
		switch {
		// Return an error on the first query, to make sure that it's retried successfully
		case qi == 0:
			return NewFakePoolConnectionQuery(t,
				"INSERT INTO `vt_ks`.table1(id, msg, keyspace_id) VALUES (*",
				fmt.Errorf("The MariaDB server is running with the --read-only option so it cannot execute this statement (errno 1290) during query:"),
			), nil
		case qi <= insertCount:
			return NewFakePoolConnectionQuery(t, "INSERT INTO `vt_ks`.table1(id, msg, keyspace_id) VALUES (*", nil), nil
		case qi == insertCount+1:
			return NewFakePoolConnectionQuery(t, "CREATE DATABASE IF NOT EXISTS _vt", nil), nil
		case qi == insertCount+2:
			return NewFakePoolConnectionQuery(t, "CREATE TABLE IF NOT EXISTS _vt.blp_checkpoint (\n"+
				"  source_shard_uid INT(10) UNSIGNED NOT NULL,\n"+
				"  pos VARCHAR(250) DEFAULT NULL,\n"+
				"  time_updated BIGINT UNSIGNED NOT NULL,\n"+
				"  transaction_timestamp BIGINT UNSIGNED NOT NULL,\n"+
				"  flags VARCHAR(250) DEFAULT NULL,\n"+
				"  PRIMARY KEY (source_shard_uid)) ENGINE=InnoDB", nil), nil
		case qi == insertCount+3:
			return NewFakePoolConnectionQuery(t, "INSERT INTO _vt.blp_checkpoint (source_shard_uid, pos, time_updated, transaction_timestamp, flags) VALUES (0, 'MariaDB/12-34-5678', *", nil), nil
		}

		return nil, fmt.Errorf("Unexpected connection")
	}
}

func TestSplitClonePopulateBlpCheckpoint(t *testing.T) {
	testSplitClone(t, "-populate_blp_checkpoint")
}

func testSplitClone(t *testing.T, strategy string) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

	sourceMaster := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wr, "cell1", 1,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "-80"),
		testlib.TabletParent(sourceMaster.Tablet.Alias))
	sourceRdonly2 := testlib.NewFakeTablet(t, wr, "cell1", 2,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "-80"),
		testlib.TabletParent(sourceMaster.Tablet.Alias))

	leftMaster := testlib.NewFakeTablet(t, wr, "cell1", 10,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "-40"))
	leftRdonly := testlib.NewFakeTablet(t, wr, "cell1", 11,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "-40"),
		testlib.TabletParent(leftMaster.Tablet.Alias))

	rightMaster := testlib.NewFakeTablet(t, wr, "cell1", 20,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "40-80"))
	rightRdonly := testlib.NewFakeTablet(t, wr, "cell1", 21,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "40-80"),
		testlib.TabletParent(rightMaster.Tablet.Alias))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, leftMaster, leftRdonly, rightMaster, rightRdonly} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	// add the topo and schema data we'll need
	ctx := context.Background()
	if err := topo.CreateShard(ts, "ks", "80-"); err != nil {
		t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	if err := wr.SetKeyspaceShardingInfo(ctx, "ks", "keyspace_id", key.KIT_UINT64, 4, false); err != nil {
		t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	if err := wr.RebuildKeyspaceGraph(ctx, "ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	gwrk, err := NewSplitCloneWorker(wr, "cell1", "ks", "-80", nil, strategy, 10 /*sourceReaderCount*/, 4 /*destinationPackCount*/, 1 /*minTableSizeForSplit*/, 10 /*destinationWriterCount*/)
	if err != nil {
		t.Errorf("Worker creation failed: %v", err)
	}
	wrk := gwrk.(*SplitCloneWorker)

	for _, sourceRdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2} {
		sourceRdonly.FakeMysqlDaemon.Schema = &myproto.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*myproto.TableDefinition{
				&myproto.TableDefinition{
					Name:              "table1",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              myproto.TABLE_BASE_TABLE,
					// This informs how many rows we can pack into a single insert
					DataLength: 2048,
				},
			},
		}
		sourceRdonly.FakeMysqlDaemon.DbAppConnectionFactory = SourceRdonlyFactory(t)
		sourceRdonly.FakeMysqlDaemon.CurrentSlaveStatus = &myproto.ReplicationStatus{
			Position: myproto.ReplicationPosition{
				GTIDSet: myproto.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678},
			},
		}
		sourceRdonly.RPCServer.Register(&SqlQuery{t: t})
	}

	// We read 100 source rows. sourceReaderCount is set to 10, so
	// we'll have 100/10=10 rows per table chunk.
	// destinationPackCount is set to 4, so we take 4 source rows
	// at once. So we'll process 4 + 4 + 2 rows to get to 10.
	// That means 3 insert statements on each target (each
	// containing half of the rows, i.e. 2 + 2 + 1 rows). So 3 * 10
	// = 30 insert statements on each destination.
	leftMaster.FakeMysqlDaemon.DbAppConnectionFactory = DestinationsFactory(t, 30)
	leftRdonly.FakeMysqlDaemon.DbAppConnectionFactory = DestinationsFactory(t, 30)
	rightMaster.FakeMysqlDaemon.DbAppConnectionFactory = DestinationsFactory(t, 30)
	rightRdonly.FakeMysqlDaemon.DbAppConnectionFactory = DestinationsFactory(t, 30)

	// Only wait 1 ms between retries, so that the test passes faster
	executeFetchRetryTime = (1 * time.Millisecond)

	wrk.Run()
	status := wrk.StatusAsText()
	t.Logf("Got status: %v", status)
	if wrk.err != nil || wrk.state != stateSCDone {
		t.Errorf("Worker run failed")
	}
}
