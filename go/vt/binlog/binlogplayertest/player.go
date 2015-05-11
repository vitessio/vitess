// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlogplayertest

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

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

// ServeUpdateStream is part of the the UpdateStream interface
func (fake *FakeBinlogStreamer) ServeUpdateStream(req *proto.UpdateStreamRequest, sendReply func(reply *proto.StreamEvent) error) error {
	if fake.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return nil
}

//
// StreamKeyRange tests
//

var testKeyRangeRequest = &proto.KeyRangeRequest{
	Position: myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   1,
			Server:   3456,
			Sequence: 7890,
		},
	},
	KeyspaceIdType: key.KIT_UINT64,
	KeyRange: key.KeyRange{
		Start: key.Uint64Key(0x7000000000000000).KeyspaceId(),
		End:   key.Uint64Key(0x9000000000000000).KeyspaceId(),
	},
	Charset: &mproto.Charset{
		Client: 12,
		Conn:   13,
		Server: 14,
	},
}

var testBinlogTransaction = &proto.BinlogTransaction{
	Statements: []proto.Statement{
		proto.Statement{
			Category: proto.BL_ROLLBACK,
			Charset: &mproto.Charset{
				Client: 120,
				Conn:   130,
				Server: 140,
			},
			Sql: []byte("my statement"),
		},
	},
	Timestamp: 78,
	GTIDField: myproto.GTIDField{
		Value: myproto.MariadbGTID{
			Domain:   1,
			Server:   345,
			Sequence: 789,
		},
	},
}

// StreamKeyRange is part of the the UpdateStream interface
func (fake *FakeBinlogStreamer) StreamKeyRange(req *proto.KeyRangeRequest, sendReply func(reply *proto.BinlogTransaction) error) error {
	if fake.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	sendReply(testBinlogTransaction)
	return nil
}

func testStreamKeyRange(t *testing.T, bpc binlogplayer.BinlogPlayerClient) {
	c := make(chan *proto.BinlogTransaction)
	bpr := bpc.StreamKeyRange(testKeyRangeRequest, c)
	if se, ok := <-c; !ok {
		t.Fatalf("got no response")
	} else {
		if !reflect.DeepEqual(*se, *testBinlogTransaction) {
			t.Errorf("got wrong result, got %v expected %v", *se, *testBinlogTransaction)
		}
	}
	if se, ok := <-c; ok {
		t.Fatalf("got a response when error expected: %v", se)
	}
	if err := bpr.Error(); err != nil {
		t.Errorf("got unexpected error: %v", err)
	}
}

func testStreamKeyRangePanics(t *testing.T, bpc binlogplayer.BinlogPlayerClient) {
	c := make(chan *proto.BinlogTransaction)
	bpr := bpc.StreamKeyRange(testKeyRangeRequest, c)
	if se, ok := <-c; ok {
		t.Fatalf("got a response when error expected: %v", se)
	}
	err := bpr.Error()
	if err == nil || !strings.Contains(err.Error(), "test-triggered panic") {
		t.Errorf("wrong error from panic: %v", err)
	}
}

//
// StreamTables test
//

var testTablesRequest = &proto.TablesRequest{
	Position: myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   1,
			Server:   345,
			Sequence: 789,
		},
	},
	Tables: []string{"table1", "table2"},
	Charset: &mproto.Charset{
		Client: 12,
		Conn:   13,
		Server: 14,
	},
}

// StreamTables is part of the the UpdateStream interface
func (fake *FakeBinlogStreamer) StreamTables(req *proto.TablesRequest, sendReply func(reply *proto.BinlogTransaction) error) error {
	if fake.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	sendReply(testBinlogTransaction)
	return nil
}

func testStreamTables(t *testing.T, bpc binlogplayer.BinlogPlayerClient) {
	c := make(chan *proto.BinlogTransaction)
	bpr := bpc.StreamTables(testTablesRequest, c)
	if se, ok := <-c; !ok {
		t.Fatalf("got no response")
	} else {
		if !reflect.DeepEqual(*se, *testBinlogTransaction) {
			t.Errorf("got wrong result, got %v expected %v", *se, *testBinlogTransaction)
		}
	}
	if se, ok := <-c; ok {
		t.Fatalf("got a response when error expected: %v", se)
	}
	if err := bpr.Error(); err != nil {
		t.Errorf("got unexpected error: %v", err)
	}
}

func testStreamTablesPanics(t *testing.T, bpc binlogplayer.BinlogPlayerClient) {
	c := make(chan *proto.BinlogTransaction)
	bpr := bpc.StreamTables(testTablesRequest, c)
	if se, ok := <-c; ok {
		t.Fatalf("got a response when error expected: %v", se)
	}
	err := bpr.Error()
	if err == nil || !strings.Contains(err.Error(), "test-triggered panic") {
		t.Errorf("wrong error from panic: %v", err)
	}
}

// HandlePanic is part of the the UpdateStream interface
func (fake *FakeBinlogStreamer) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("Caught panic: %v", x)
	}
}

// Run runs the test suite
func Run(t *testing.T, bpc binlogplayer.BinlogPlayerClient, addr string, fake *FakeBinlogStreamer) {
	if err := bpc.Dial(addr, 30*time.Second); err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	// no panic
	testStreamKeyRange(t, bpc)
	testStreamTables(t, bpc)

	// panic now, and test
	fake.panics = true
	testStreamKeyRangePanics(t, bpc)
	testStreamTablesPanics(t, bpc)
}
