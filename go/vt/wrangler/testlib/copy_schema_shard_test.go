// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

type ExpectedExecuteFetch struct {
	Query       string
	MaxRows     int
	WantFields  bool
	QueryResult *sqltypes.Result
	Error       error
}

// FakePoolConnection implements dbconnpool.PoolConnection
type FakePoolConnection struct {
	t      *testing.T
	Closed bool

	ExpectedExecuteFetch      []ExpectedExecuteFetch
	ExpectedExecuteFetchIndex int
}

func NewFakePoolConnectionQuery(t *testing.T, query string) *FakePoolConnection {
	return &FakePoolConnection{
		t: t,
		ExpectedExecuteFetch: []ExpectedExecuteFetch{
			ExpectedExecuteFetch{
				Query:       query,
				QueryResult: &sqltypes.Result{},
			},
		},
	}
}

func (fpc *FakePoolConnection) ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if fpc.ExpectedExecuteFetchIndex >= len(fpc.ExpectedExecuteFetch) {
		fpc.t.Errorf("got unexpected out of bound fetch: %v >= %v", fpc.ExpectedExecuteFetchIndex, len(fpc.ExpectedExecuteFetch))
		return nil, fmt.Errorf("unexpected out of bound fetch")
	}
	expected := fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].Query
	if query != expected {
		fpc.t.Errorf("got unexpected query: %v != %v", query, expected)
		return nil, fmt.Errorf("unexpected query")
	}
	fpc.t.Logf("ExecuteFetch: %v", query)
	defer func() {
		fpc.ExpectedExecuteFetchIndex++
	}()
	return fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].QueryResult, nil
}

func (fpc *FakePoolConnection) ExecuteStreamFetch(query string, callback func(*sqltypes.Result) error, streamBufferSize int) error {
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

func TestCopySchemaShard_UseTabletAsSource(t *testing.T) {
	copySchema(t, false /* useShardAsSource */)
}

func TestCopySchemaShard_UseShardAsSource(t *testing.T) {
	copySchema(t, true /* useShardAsSource */)
}

func copySchema(t *testing.T, useShardAsSource bool) {
	db := fakesqldb.Register()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	if err := ts.CreateKeyspace(context.Background(), "ks", &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	sourceMaster := NewFakeTablet(t, wr, "cell1", 0,
		topodatapb.TabletType_MASTER, db, TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly := NewFakeTablet(t, wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, db, TabletKeyspaceShard(t, "ks", "-80"))

	destinationMaster := NewFakeTablet(t, wr, "cell1", 10,
		topodatapb.TabletType_MASTER, db, TabletKeyspaceShard(t, "ks", "-40"))

	for _, ft := range []*FakeTablet{sourceMaster, sourceRdonly, destinationMaster} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	schema := &tabletmanagerdatapb.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:   "table1",
				Schema: "CREATE TABLE `resharding1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Type:   tmutils.TableBaseTable,
			},
			{
				Name:   "view1",
				Schema: "CREATE TABLE `view1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Type:   tmutils.TableView,
			},
		},
	}
	sourceMaster.FakeMysqlDaemon.Schema = schema
	sourceRdonly.FakeMysqlDaemon.Schema = schema

	createDb := "CREATE DATABASE `vt_ks` /*!40100 DEFAULT CHARACTER SET utf8 */"
	createTable := "CREATE TABLE `vt_ks`.`resharding1` (\n" +
		"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
		"  `msg` varchar(64) DEFAULT NULL,\n" +
		"  `keyspace_id` bigint(20) unsigned NOT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `by_msg` (`msg`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
	createTableView := "CREATE TABLE `view1` (\n" +
		"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
		"  `msg` varchar(64) DEFAULT NULL,\n" +
		"  `keyspace_id` bigint(20) unsigned NOT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `by_msg` (`msg`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
	db.AddQuery("USE vt_ks", &sqltypes.Result{})
	db.AddQuery(createDb, &sqltypes.Result{})
	db.AddQuery(createTable, &sqltypes.Result{})
	db.AddQuery(createTableView, &sqltypes.Result{})

	source := topoproto.TabletAliasString(sourceRdonly.Tablet.Alias)
	if useShardAsSource {
		source = "ks/-80"
	}
	if err := vp.Run([]string{"CopySchemaShard", "-include-views", source, "ks/-40"}); err != nil {
		t.Fatalf("CopySchemaShard failed: %v", err)
	}
	if count := db.GetQueryCalledNum(createDb); count != 1 {
		t.Fatalf("CopySchemaShard did not create the db exactly once. Query count: %v", count)
	}
	if count := db.GetQueryCalledNum(createTable); count != 1 {
		t.Fatalf("CopySchemaShard did not create the table exactly once. Query count: %v", count)
	}
	if count := db.GetQueryCalledNum(createTableView); count != 1 {
		t.Fatalf("CopySchemaShard did not create the table view exactly once. Query count: %v", count)
	}
}
