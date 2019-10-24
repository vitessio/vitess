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

package binlogplayer

import (
	"flag"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This partially duplicates code from vreplication/framework_test.go.
// TODO(sougou): merge the functionality when we move this package to vreplication.

// fakeBinlogClient satisfies Client.
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

func (fbc *fakeBinlogClient) StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset) (BinlogTransactionStream, error) {
	fbc.lastPos = position
	fbc.lastTables = tables
	fbc.lastCharset = charset
	return &btStream{ctx: ctx}, nil
}

func (fbc *fakeBinlogClient) StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset) (BinlogTransactionStream, error) {
	fbc.lastPos = position
	fbc.lastKeyRange = keyRange
	fbc.lastCharset = charset
	return &btStream{ctx: ctx}, nil
}

// btStream satisfies BinlogTransactionStream
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

// globalFBC is set by newFakeBinlogClient, which is then returned by the client factory below.
var globalFBC *fakeBinlogClient

func init() {
	RegisterClientFactory("test", func() Client { return globalFBC })
	flag.Set("binlog_player_protocol", "test")
}
