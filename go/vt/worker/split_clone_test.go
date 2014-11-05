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
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo"
)

// This is a local SqlQuery RCP implementation to support the tests
type SqlQuery struct {
	t *testing.T
}

func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return nil
}

func (sq *SqlQuery) StreamExecute(ctx *rpcproto.Context, query *proto.Query, sendReply func(reply interface{}) error) error {
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

func NewFakePoolConnectionQueryBinlogOff(t *testing.T, query string) *FakePoolConnection {
	return &FakePoolConnection{
		t: t,
		ExpectedExecuteFetch: []ExpectedExecuteFetch{
			ExpectedExecuteFetch{
				Query:       "SET sql_log_bin = OFF",
				QueryResult: &mproto.QueryResult{},
			},
			ExpectedExecuteFetch{
				Query:       query,
				QueryResult: &mproto.QueryResult{},
			},
			ExpectedExecuteFetch{
				Query:       "SET sql_log_bin = ON",
				QueryResult: &mproto.QueryResult{},
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
	return fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].QueryResult, nil
}

func (fpc *FakePoolConnection) ExecuteStreamFetch(query string, callback func(*mproto.QueryResult) error, streamBufferSize int) error {
	return nil
}

func (fpc *FakePoolConnection) Id() int64 {
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
func DestinationsFactory(t *testing.T, rowCount int64) func() (dbconnpool.PoolConnection, error) {
	var queryIndex int64 = -1
	return func() (dbconnpool.PoolConnection, error) {
		qi := atomic.AddInt64(&queryIndex, 1)
		switch {
		case qi == 0:
			return NewFakePoolConnectionQueryBinlogOff(t, "CREATE DATABASE `vt_ks` /*!40100 DEFAULT CHARACTER SET utf8 */"), nil
		case qi == 1:
			return NewFakePoolConnectionQueryBinlogOff(t, "CREATE TABLE `vt_ks`.`resharding1` (\n"+
				"  `id` bigint(20) NOT NULL,\n"+
				"  `msg` varchar(64) DEFAULT NULL,\n"+
				"  `keyspace_id` bigint(20) unsigned NOT NULL,\n"+
				"  PRIMARY KEY (`id`),\n"+
				"  KEY `by_msg` (`msg`)\n"+
				") ENGINE=InnoDB DEFAULT CHARSET=utf8"), nil
		case qi >= 2 && qi < rowCount+2:
			return NewFakePoolConnectionQueryBinlogOff(t, "INSERT INTO `vt_ks`.table1(id, msg, keyspace_id) VALUES (*"), nil
		case qi == rowCount+2:
			return NewFakePoolConnectionQueryBinlogOff(t, "ALTER TABLE `vt_ks`.`table1` MODIFY   `id` bigint(20) NOT NULL AUTO_INCREMENT"), nil
		case qi == rowCount+3:
			return NewFakePoolConnectionQueryBinlogOff(t, "CREATE DATABASE IF NOT EXISTS _vt"), nil
		case qi == rowCount+4:
			return NewFakePoolConnectionQueryBinlogOff(t, "CREATE TABLE IF NOT EXISTS _vt.blp_checkpoint (\n"+
				"  source_shard_uid INT(10) UNSIGNED NOT NULL,\n"+
				"  pos VARCHAR(250) DEFAULT NULL,\n"+
				"  time_updated BIGINT UNSIGNED NOT NULL,\n"+
				"  transaction_timestamp BIGINT UNSIGNED NOT NULL,\n"+
				"  flags VARCHAR(250) DEFAULT NULL,\n"+
				"  PRIMARY KEY (source_shard_uid)) ENGINE=InnoDB"), nil
		case qi == rowCount+5:
			return NewFakePoolConnectionQueryBinlogOff(t, "INSERT INTO _vt.blp_checkpoint (source_shard_uid, pos, time_updated, transaction_timestamp, flags) VALUES (0, 'MariaDB/12-34-5678', *"), nil
		}

		return nil, fmt.Errorf("Unexpected connection")
	}
}

func TestSplitClone(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, time.Minute, time.Second)

	sourceMaster := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly := testlib.NewFakeTablet(t, wr, "cell1", 1,
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

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly, leftMaster, leftRdonly, rightMaster, rightRdonly} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	// add the topo and schema data we'll need
	if err := topo.CreateShard(ts, "ks", "80-"); err != nil {
		t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	if err := wr.SetKeyspaceShardingInfo("ks", "keyspace_id", key.KIT_UINT64, 4, false); err != nil {
		t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	if err := wr.RebuildKeyspaceGraph("ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}
	sourceRdonly.FakeMysqlDaemon.Schema = &myproto.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*myproto.TableDefinition{
			&myproto.TableDefinition{
				Name:              "table1",
				Schema:            "CREATE TABLE `resharding1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Columns:           []string{"id", "msg", "keyspace_id"},
				PrimaryKeyColumns: []string{"id"},
				Type:              myproto.TABLE_BASE_TABLE,
				DataLength:        2048,
				RowCount:          100,
			},
		},
		Version: "unused",
	}
	sourceRdonly.FakeMysqlDaemon.DbaConnectionFactory = SourceRdonlyFactory(t)
	sourceRdonly.FakeMysqlDaemon.CurrentSlaveStatus = &myproto.ReplicationStatus{
		Position: myproto.ReplicationPosition{
			GTIDSet: myproto.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678},
		},
	}
	sourceRdonly.RpcServer.Register(&SqlQuery{t: t})
	leftMaster.FakeMysqlDaemon.DbaConnectionFactory = DestinationsFactory(t, 50)
	leftRdonly.FakeMysqlDaemon.DbaConnectionFactory = DestinationsFactory(t, 50)
	rightMaster.FakeMysqlDaemon.DbaConnectionFactory = DestinationsFactory(t, 50)
	rightRdonly.FakeMysqlDaemon.DbaConnectionFactory = DestinationsFactory(t, 50)

	wrk := NewSplitCloneWorker(wr, "cell1", "ks", "-80", nil, "populateBlpCheckpoint", 10, 1, 10).(*SplitCloneWorker)
	wrk.Run()
	status := wrk.StatusAsText()
	t.Logf("Got status: %v", status)
	if wrk.err != nil || wrk.state != stateSCDone {
		t.Errorf("Worker run failed")
	}
}
