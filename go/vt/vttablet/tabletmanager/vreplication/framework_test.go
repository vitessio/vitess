/*
Copyright 2018 The Vitess Authors.

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

package vreplication

import (
	"flag"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	testCell     = "cell"
	testKeyspace = "ks"
	testShard    = "0"
)

// This file provides support functions for tests.
// It's capable of creating a single unsharded keyspace
// and allows you to add various tablet types.

//--------------------------------------
// Topos and tablets

func createTopo() *topo.Server {
	ts := memorytopo.NewServer(testCell)
	ctx := context.Background()
	if err := ts.CreateKeyspace(ctx, testKeyspace, &topodatapb.Keyspace{}); err != nil {
		panic(err)
	}
	if err := ts.CreateShard(ctx, testKeyspace, testShard); err != nil {
		panic(err)
	}
	return ts
}

func addTablet(ts *topo.Server, id int, shard string, tabletType topodatapb.TabletType, serving, healthy bool) *topodatapb.Tablet {
	t := newTablet(id, shard, tabletType, serving, healthy)
	if err := ts.CreateTablet(context.Background(), t); err != nil {
		panic(err)
	}
	return t
}

func newTablet(id int, shard string, tabletType topodatapb.TabletType, serving, healthy bool) *topodatapb.Tablet {
	stag := "not_serving"
	if serving {
		stag = "serving"
	}
	htag := "not_healthy"
	if healthy {
		htag = "healthy"
	}
	_, kr, err := topo.ValidateShardName(shard)
	if err != nil {
		panic(err)
	}
	return &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: testCell,
			Uid:  uint32(id),
		},
		Keyspace: testKeyspace,
		Shard:    testShard,
		KeyRange: kr,
		Type:     tabletType,
		Tags: map[string]string{
			"serving": stag,
			"healthy": htag,
		},
		PortMap: map[string]int32{
			"test": int32(id),
		},
	}
}

// fakeTabletConn implement TabletConn interface. We only care about the
// health check part. The state reported by the tablet will depend
// on the Tag values "serving" and "healthy".
type fakeTabletConn struct {
	queryservice.QueryService
	tablet *topodatapb.Tablet
}

// StreamHealth is part of queryservice.QueryService.
func (ftc *fakeTabletConn) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	serving := true
	if s, ok := ftc.tablet.Tags["serving"]; ok {
		serving = (s == "serving")
	}
	var herr string
	if s, ok := ftc.tablet.Tags["healthy"]; ok && s != "healthy" {
		herr = "err"
	}
	callback(&querypb.StreamHealthResponse{
		Serving: serving,
		Target: &querypb.Target{
			Keyspace:   ftc.tablet.Keyspace,
			Shard:      ftc.tablet.Shard,
			TabletType: ftc.tablet.Type,
		},
		RealtimeStats: &querypb.RealtimeStats{HealthError: herr},
	})
	return nil
}

//--------------------------------------
// Binlog Client to TabletManager

// fakeBinlogClient satisfies binlogplayer.Client.
// Not to be used concurrently.
type fakeBinlogClient struct {
	lastTablet   *topodatapb.Tablet
	lastPos      string
	lastTables   []string
	lastKeyRange *topodatapb.KeyRange
	lastCharset  *binlogdatapb.Charset
}

func newFakeBinlogClient() *fakeBinlogClient {
	globalFBC = &fakeBinlogClient{}
	return globalFBC
}

func (fbc *fakeBinlogClient) Dial(tablet *topodatapb.Tablet) error {
	fbc.lastTablet = tablet
	return nil
}

func (fbc *fakeBinlogClient) Close() {
}

func (fbc *fakeBinlogClient) StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset) (binlogplayer.BinlogTransactionStream, error) {
	fbc.lastPos = position
	fbc.lastTables = tables
	fbc.lastCharset = charset
	return &btStream{ctx: ctx}, nil
}

func (fbc *fakeBinlogClient) StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset) (binlogplayer.BinlogTransactionStream, error) {
	fbc.lastPos = position
	fbc.lastKeyRange = keyRange
	fbc.lastCharset = charset
	return &btStream{ctx: ctx}, nil
}

// btStream satisfies binlogplayer.BinlogTransactionStream
type btStream struct {
	ctx  context.Context
	sent bool
}

func (t *btStream) Recv() (*binlogdatapb.BinlogTransaction, error) {
	if !t.sent {
		t.sent = true
		return &binlogdatapb.BinlogTransaction{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
					Sql:      []byte("insert into t values(1)"),
				},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 72,
				Position:  "MariaDB/0-1-1235",
			},
		}, nil
	}
	<-t.ctx.Done()
	return nil, t.ctx.Err()
}

func expectFBCRequest(t *testing.T, fbc *fakeBinlogClient, tablet *topodatapb.Tablet, pos string, tables []string, kr *topodatapb.KeyRange) {
	t.Helper()
	if !proto.Equal(tablet, fbc.lastTablet) {
		t.Errorf("Request tablet: %v, want %v", fbc.lastTablet, tablet)
	}
	if pos != fbc.lastPos {
		t.Errorf("Request pos: %v, want %v", fbc.lastPos, pos)
	}
	if !reflect.DeepEqual(tables, fbc.lastTables) {
		t.Errorf("Request tables: %v, want %v", fbc.lastTables, tables)
	}
	if !proto.Equal(kr, fbc.lastKeyRange) {
		t.Errorf("Request KeyRange: %v, want %v", fbc.lastKeyRange, kr)
	}
}

//--------------------------------------
// init

// globalFBC is set by newFakeBinlogClient, which is then returned by the client factory below.
var globalFBC *fakeBinlogClient

func init() {
	tabletconn.RegisterDialer("test", func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		return &fakeTabletConn{
			QueryService: fakes.ErrorQueryService,
			tablet:       tablet,
		}, nil
	})
	flag.Set("tablet_protocol", "test")

	binlogplayer.RegisterClientFactory("test", func() binlogplayer.Client { return globalFBC })
	flag.Set("binlog_player_protocol", "test")
}
