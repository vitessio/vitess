// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	_ "github.com/youtube/vitess/go/vt/tabletserver/gorpctabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
)

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

func NewFakePoolConnectionQuery(t *testing.T, query string) *FakePoolConnection {
	return &FakePoolConnection{
		t: t,
		ExpectedExecuteFetch: []ExpectedExecuteFetch{
			ExpectedExecuteFetch{
				Query:       query,
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

// on the destinations
func DestinationsFactory(t *testing.T) func() (dbconnpool.PoolConnection, error) {
	var queryIndex int64 = -1

	return func() (dbconnpool.PoolConnection, error) {
		qi := atomic.AddInt64(&queryIndex, 1)
		switch {
		case qi == 0:
			return NewFakePoolConnectionQuery(t, "CREATE DATABASE `vt_ks` /*!40100 DEFAULT CHARACTER SET utf8 */"), nil
		case qi == 1:
			return NewFakePoolConnectionQuery(t, "CREATE TABLE `vt_ks`.`resharding1` (\n"+
				"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"+
				"  `msg` varchar(64) DEFAULT NULL,\n"+
				"  `keyspace_id` bigint(20) unsigned NOT NULL,\n"+
				"  PRIMARY KEY (`id`),\n"+
				"  KEY `by_msg` (`msg`)\n"+
				") ENGINE=InnoDB DEFAULT CHARSET=utf8"), nil
		case qi == 2:
			return NewFakePoolConnectionQuery(t, "CREATE TABLE `view1` (\n"+
				"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"+
				"  `msg` varchar(64) DEFAULT NULL,\n"+
				"  `keyspace_id` bigint(20) unsigned NOT NULL,\n"+
				"  PRIMARY KEY (`id`),\n"+
				"  KEY `by_msg` (`msg`)\n"+
				") ENGINE=InnoDB DEFAULT CHARSET=utf8"), nil
		}

		return nil, fmt.Errorf("Unexpected connection")
	}
}

func TestCopySchemaShard(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, time.Minute, time.Second)

	sourceMaster := NewFakeTablet(t, wr, "cell1", 0,
		topo.TYPE_MASTER, TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly := NewFakeTablet(t, wr, "cell1", 1,
		topo.TYPE_RDONLY, TabletKeyspaceShard(t, "ks", "-80"),
		TabletParent(sourceMaster.Tablet.Alias))

	destinationMaster := NewFakeTablet(t, wr, "cell1", 10,
		topo.TYPE_MASTER, TabletKeyspaceShard(t, "ks", "-40"))
	// one destination RdOnly, so we know that schema copies propogate from masters
	destinationRdonly := NewFakeTablet(t, wr, "cell1", 11,
		topo.TYPE_RDONLY, TabletKeyspaceShard(t, "ks", "-40"),
		TabletParent(destinationMaster.Tablet.Alias))

	for _, ft := range []*FakeTablet{sourceMaster, sourceRdonly, destinationMaster, destinationRdonly} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	sourceRdonly.FakeMysqlDaemon.Schema = &myproto.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*myproto.TableDefinition{
			&myproto.TableDefinition{
				Name:   "table1",
				Schema: "CREATE TABLE `resharding1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Type:   myproto.TABLE_BASE_TABLE,
			},
			&myproto.TableDefinition{
				Name:   "view1",
				Schema: "CREATE TABLE `view1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Type:   myproto.TABLE_VIEW,
			},
		},
	}

	destinationMaster.FakeMysqlDaemon.DbaConnectionFactory = DestinationsFactory(t)
	destinationRdonly.FakeMysqlDaemon.DbaConnectionFactory = DestinationsFactory(t)

	if err := wr.CopySchemaShard(sourceRdonly.Tablet.Alias, nil, nil, true, "ks", "-40"); err != nil {
		t.Fatalf("CopySchemaShard failed: %v", err)
	}

}
