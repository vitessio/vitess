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
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

// verticalSqlQuery is a local QueryService implementation to support the tests
type verticalSqlQuery struct {
	queryservice.ErrorQueryService
	t *testing.T
}

func (sq *verticalSqlQuery) StreamExecute(ctx context.Context, target *pb.Target, query *proto.Query, sendReply func(reply *mproto.QueryResult) error) error {
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
	sq.t.Logf("verticalSqlQuery: got query: %v with min %v max %v", *query, min, max)

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
		},
	}); err != nil {
		return err
	}

	// Send the values
	for i := min; i < max; i++ {
		if err := sendReply(&mproto.QueryResult{
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("Text for %v", i))),
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

// VerticalFakePoolConnection implements dbconnpool.PoolConnection
type VerticalFakePoolConnection struct {
	t      *testing.T
	Closed bool

	ExpectedExecuteFetch      []ExpectedExecuteFetch
	ExpectedExecuteFetchIndex int
}

func NewVerticalFakePoolConnectionQuery(t *testing.T, query string, err error) *VerticalFakePoolConnection {
	return &VerticalFakePoolConnection{
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

func (fpc *VerticalFakePoolConnection) ExecuteFetch(query string, maxrows int, wantfields bool) (*mproto.QueryResult, error) {
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

func (fpc *VerticalFakePoolConnection) ExecuteStreamFetch(query string, callback func(*mproto.QueryResult) error, streamBufferSize int) error {
	return nil
}

func (fpc *VerticalFakePoolConnection) ID() int64 {
	return 1
}

func (fpc *VerticalFakePoolConnection) Close() {
	fpc.Closed = true
}

func (fpc *VerticalFakePoolConnection) IsClosed() bool {
	return fpc.Closed
}

func (fpc *VerticalFakePoolConnection) Recycle() {
}

func (fpc *VerticalFakePoolConnection) Reconnect() error {
	return nil
}

// on the source rdonly guy, should only have one query to find min & max
func VerticalSourceRdonlyFactory(t *testing.T) func() (dbconnpool.PoolConnection, error) {
	return func() (dbconnpool.PoolConnection, error) {
		return &VerticalFakePoolConnection{
			t: t,
			ExpectedExecuteFetch: []ExpectedExecuteFetch{
				ExpectedExecuteFetch{
					Query: "SELECT MIN(id), MAX(id) FROM vt_source_ks.moving1",
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
func VerticalDestinationsFactory(t *testing.T, insertCount int64) func() (dbconnpool.PoolConnection, error) {
	var queryIndex int64 = -1

	return func() (dbconnpool.PoolConnection, error) {
		qi := atomic.AddInt64(&queryIndex, 1)
		switch {
		// Return an error on the first query, to make sure that it's retried successfully
		case qi == 0:
			return NewFakePoolConnectionQuery(t,
				"INSERT INTO `vt_destination_ks`.moving1(id, msg) VALUES (*",
				fmt.Errorf("The MariaDB server is running with the --read-only option so it cannot execute this statement (errno 1290) during query:"),
			), nil
		case qi <= insertCount:
			return NewVerticalFakePoolConnectionQuery(t, "INSERT INTO `vt_destination_ks`.moving1(id, msg) VALUES (*", nil), nil
		case qi == insertCount+1:
			return NewVerticalFakePoolConnectionQuery(t, "CREATE DATABASE IF NOT EXISTS _vt", nil), nil
		case qi == insertCount+2:
			return NewVerticalFakePoolConnectionQuery(t, "CREATE TABLE IF NOT EXISTS _vt.blp_checkpoint (\n"+
				"  source_shard_uid INT(10) UNSIGNED NOT NULL,\n"+
				"  pos VARCHAR(250) DEFAULT NULL,\n"+
				"  time_updated BIGINT UNSIGNED NOT NULL,\n"+
				"  transaction_timestamp BIGINT UNSIGNED NOT NULL,\n"+
				"  flags VARCHAR(250) DEFAULT NULL,\n"+
				"  PRIMARY KEY (source_shard_uid)) ENGINE=InnoDB", nil), nil
		case qi == insertCount+3:
			return NewVerticalFakePoolConnectionQuery(t, "INSERT INTO _vt.blp_checkpoint (source_shard_uid, pos, time_updated, transaction_timestamp, flags) VALUES (0, 'MariaDB/12-34-5678', *", nil), nil
		}

		return nil, fmt.Errorf("Unexpected connection")
	}
}

func TestVerticalSplitClonePopulateBlpCheckpoint(t *testing.T) {
	testVerticalSplitClone(t, "-populate_blp_checkpoint")
}

func testVerticalSplitClone(t *testing.T, strategy string) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

	sourceMaster := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wr, "cell1", 1,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly2 := testlib.NewFakeTablet(t, wr, "cell1", 2,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "source_ks", "0"))

	// Create the destination keyspace with the appropriate ServedFromMap
	ki := &pbt.Keyspace{
		ServedFroms: []*pbt.Keyspace_ServedFrom{
			&pbt.Keyspace_ServedFrom{
				TabletType: pbt.TabletType_MASTER,
				Keyspace:   "source_ks",
			},
			&pbt.Keyspace_ServedFrom{
				TabletType: pbt.TabletType_REPLICA,
				Keyspace:   "source_ks",
			},
			&pbt.Keyspace_ServedFrom{
				TabletType: pbt.TabletType_RDONLY,
				Keyspace:   "source_ks",
			},
		},
	}
	ctx := context.Background()
	wr.TopoServer().CreateKeyspace(ctx, "destination_ks", ki)

	destMaster := testlib.NewFakeTablet(t, wr, "cell1", 10,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	destRdonly := testlib.NewFakeTablet(t, wr, "cell1", 11,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, destMaster, destRdonly} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	// add the topo and schema data we'll need
	if err := wr.RebuildKeyspaceGraph(ctx, "source_ks", nil, true); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}
	if err := wr.RebuildKeyspaceGraph(ctx, "destination_ks", nil, true); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	gwrk, err := NewVerticalSplitCloneWorker(wr, "cell1", "destination_ks", "0", []string{"moving.*", "view1"}, strategy, 10 /*sourceReaderCount*/, 4 /*destinationPackCount*/, 1 /*minTableSizeForSplit*/, 10 /*destinationWriterCount*/)
	if err != nil {
		t.Errorf("Worker creation failed: %v", err)
	}
	wrk := gwrk.(*VerticalSplitCloneWorker)

	for _, sourceRdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2} {
		sourceRdonly.FakeMysqlDaemon.Schema = &myproto.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*myproto.TableDefinition{
				&myproto.TableDefinition{
					Name:              "moving1",
					Columns:           []string{"id", "msg"},
					PrimaryKeyColumns: []string{"id"},
					Type:              myproto.TableBaseTable,
					// This informs how many rows we can pack into a single insert
					DataLength: 2048,
				},
				&myproto.TableDefinition{
					Name: "view1",
					Type: myproto.TableView,
				},
			},
		}
		sourceRdonly.FakeMysqlDaemon.DbAppConnectionFactory = VerticalSourceRdonlyFactory(t)
		sourceRdonly.FakeMysqlDaemon.CurrentMasterPosition = myproto.ReplicationPosition{
			GTIDSet: myproto.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678},
		}
		sourceRdonly.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			"STOP SLAVE",
			"START SLAVE",
		}
		grpcqueryservice.RegisterForTest(sourceRdonly.RPCServer, &verticalSqlQuery{t: t})
	}

	// We read 100 source rows. sourceReaderCount is set to 10, so
	// we'll have 100/10=10 rows per table chunk.
	// destinationPackCount is set to 4, so we take 4 source rows
	// at once. So we'll process 4 + 4 + 2 rows to get to 10.
	// That means 3 insert statements on the target. So 3 * 10
	// = 30 insert statements on the destination.
	destMaster.FakeMysqlDaemon.DbAppConnectionFactory = VerticalDestinationsFactory(t, 30)
	destRdonly.FakeMysqlDaemon.DbAppConnectionFactory = VerticalDestinationsFactory(t, 30)

	// Only wait 1 ms between retries, so that the test passes faster
	*executeFetchRetryTime = (1 * time.Millisecond)

	err = wrk.Run(ctx)
	status := wrk.StatusAsText()
	t.Logf("Got status: %v", status)
	if err != nil || wrk.State != WorkerStateDone {
		t.Errorf("Worker run failed")
	}

	if statsDestinationAttemptedResolves.String() != "2" {
		t.Errorf("Wrong statsDestinationAttemptedResolves: wanted %v, got %v", "2", statsDestinationAttemptedResolves.String())
	}
	if statsDestinationActualResolves.String() != "1" {
		t.Errorf("Wrong statsDestinationActualResolves: wanted %v, got %v", "1", statsDestinationActualResolves.String())
	}
	if statsRetryCounters.String() != "{\"ReadOnly\": 1}" {
		t.Errorf("Wrong statsRetryCounters: wanted %v, got %v", "{\"ReadOnly\": 1}", statsRetryCounters.String())
	}
}
