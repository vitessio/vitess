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

package binlogplayertest

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/key"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// keyRangeRequest is used to make a request for StreamKeyRange.
type keyRangeRequest struct {
	Position string
	KeyRange *topodatapb.KeyRange
	Charset  *binlogdatapb.Charset
}

// tablesRequest is used to make a request for StreamTables.
type tablesRequest struct {
	Position string
	Tables   []string
	Charset  *binlogdatapb.Charset
}

// FakeBinlogStreamer is our implementation of UpdateStream
type FakeBinlogStreamer struct {
	t      *testing.T
	panics bool
}

// NewFakeBinlogStreamer returns the test instance for UpdateStream
func NewFakeBinlogStreamer(t *testing.T) *FakeBinlogStreamer {
	return &FakeBinlogStreamer{
		t:      t,
		panics: false,
	}
}

//
// StreamKeyRange tests
//

var testKeyRangeRequest = &keyRangeRequest{
	Position: "KeyRange starting position",
	KeyRange: &topodatapb.KeyRange{
		Start: key.Uint64Key(0x7000000000000000).Bytes(),
		End:   key.Uint64Key(0x9000000000000000).Bytes(),
	},
	Charset: &binlogdatapb.Charset{
		Client: 12,
		Conn:   13,
		Server: 14,
	},
}

var testBinlogTransaction = &binlogdatapb.BinlogTransaction{
	Statements: []*binlogdatapb.BinlogTransaction_Statement{
		{
			Category: binlogdatapb.BinlogTransaction_Statement_BL_ROLLBACK,
			Charset: &binlogdatapb.Charset{
				Client: 120,
				Conn:   130,
				Server: 140,
			},
			Sql: []byte("my statement"),
		},
	},
	EventToken: &querypb.EventToken{
		Timestamp: 78,
		Position:  "BinlogTransaction returned position",
	},
}

// StreamKeyRange is part of the UpdateStream interface
func (fake *FakeBinlogStreamer) StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset, callback func(reply *binlogdatapb.BinlogTransaction) error) error {
	if fake.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	req := &keyRangeRequest{
		Position: position,
		KeyRange: keyRange,
		Charset:  charset,
	}
	if position != testKeyRangeRequest.Position ||
		!proto.Equal(keyRange, testKeyRangeRequest.KeyRange) ||
		!proto.Equal(charset, testKeyRangeRequest.Charset) {
		fake.t.Errorf("wrong StreamKeyRange parameter, got %+v want %+v", req, testKeyRangeRequest)
	}
	callback(testBinlogTransaction)
	return nil
}

func testStreamKeyRange(t *testing.T, bpc binlogplayer.Client) {
	ctx := context.Background()
	stream, err := bpc.StreamKeyRange(ctx, testKeyRangeRequest.Position, testKeyRangeRequest.KeyRange, testKeyRangeRequest.Charset)
	if err != nil {
		t.Fatalf("got error: %v", err)
	}
	if se, err := stream.Recv(); err != nil {
		t.Fatalf("got error: %v", err)
	} else {
		if !proto.Equal(se, testBinlogTransaction) {
			t.Errorf("got wrong result, got %v expected %v", *se, *testBinlogTransaction)
		}
	}
	if se, err := stream.Recv(); err == nil {
		t.Fatalf("got a response when error expected: %v", se)
	}
}

func testStreamKeyRangePanics(t *testing.T, bpc binlogplayer.Client) {
	ctx := context.Background()
	stream, err := bpc.StreamKeyRange(ctx, testKeyRangeRequest.Position, testKeyRangeRequest.KeyRange, testKeyRangeRequest.Charset)
	if err != nil {
		t.Fatalf("got error: %v", err)
	}
	if se, err := stream.Recv(); err == nil {
		t.Fatalf("got a response when error expected: %v", se)
	} else {
		if !strings.Contains(err.Error(), "test-triggered panic") {
			t.Errorf("wrong error from panic: %v", err)
		}
	}
}

//
// StreamTables test
//

var testTablesRequest = &tablesRequest{
	Position: "Tables starting position",
	Tables:   []string{"table1", "table2"},
	Charset: &binlogdatapb.Charset{
		Client: 12,
		Conn:   13,
		Server: 14,
	},
}

// StreamTables is part of the UpdateStream interface
func (fake *FakeBinlogStreamer) StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset, callback func(reply *binlogdatapb.BinlogTransaction) error) error {
	if fake.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	req := &tablesRequest{
		Position: position,
		Tables:   tables,
		Charset:  charset,
	}
	if position != testTablesRequest.Position ||
		!reflect.DeepEqual(tables, testTablesRequest.Tables) ||
		!proto.Equal(charset, testTablesRequest.Charset) {
		fake.t.Errorf("wrong StreamTables parameter, got %+v want %+v", req, testTablesRequest)
	}
	callback(testBinlogTransaction)
	return nil
}

func testStreamTables(t *testing.T, bpc binlogplayer.Client) {
	ctx := context.Background()
	stream, err := bpc.StreamTables(ctx, testTablesRequest.Position, testTablesRequest.Tables, testTablesRequest.Charset)
	if err != nil {
		t.Fatalf("got error: %v", err)
	}
	if se, err := stream.Recv(); err != nil {
		t.Fatalf("got error: %v", err)
	} else {
		if !proto.Equal(se, testBinlogTransaction) {
			t.Errorf("got wrong result, got %v expected %v", *se, *testBinlogTransaction)
		}
	}
	if se, err := stream.Recv(); err == nil {
		t.Fatalf("got a response when error expected: %v", se)
	}
}

func testStreamTablesPanics(t *testing.T, bpc binlogplayer.Client) {
	ctx := context.Background()
	stream, err := bpc.StreamTables(ctx, testTablesRequest.Position, testTablesRequest.Tables, testTablesRequest.Charset)
	if err != nil {
		t.Fatalf("got error: %v", err)
	}
	if se, err := stream.Recv(); err == nil {
		t.Fatalf("got a response when error expected: %v", se)
	} else {
		if !strings.Contains(err.Error(), "test-triggered panic") {
			t.Errorf("wrong error from panic: %v", err)
		}
	}
}

// HandlePanic is part of the UpdateStream interface
func (fake *FakeBinlogStreamer) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("caught panic: %v", x)
	}
}

// Run runs the test suite
func Run(t *testing.T, bpc binlogplayer.Client, tablet *topodatapb.Tablet, fake *FakeBinlogStreamer) {
	if err := bpc.Dial(tablet); err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	// no panic
	testStreamKeyRange(t, bpc)
	testStreamTables(t, bpc)

	// panic now, and test
	fake.panics = true
	testStreamKeyRangePanics(t, bpc)
	testStreamTablesPanics(t, bpc)
	fake.panics = false
}
