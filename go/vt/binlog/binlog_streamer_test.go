// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// fakeEvent implements proto.BinlogEvent.
type fakeEvent struct{}

func (fakeEvent) IsValid() bool                   { return true }
func (fakeEvent) IsFormatDescription() bool       { return false }
func (fakeEvent) IsQuery() bool                   { return false }
func (fakeEvent) IsXID() bool                     { return false }
func (fakeEvent) IsGTID() bool                    { return false }
func (fakeEvent) IsRotate() bool                  { return false }
func (fakeEvent) IsIntVar() bool                  { return false }
func (fakeEvent) IsRand() bool                    { return false }
func (fakeEvent) HasGTID(proto.BinlogFormat) bool { return true }
func (fakeEvent) Timestamp() uint32               { return 1407805592 }
func (fakeEvent) Format() (proto.BinlogFormat, error) {
	return proto.BinlogFormat{}, errors.New("not a format")
}
func (fakeEvent) GTID(proto.BinlogFormat) (myproto.GTID, error) {
	return myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0xd}, nil
}
func (fakeEvent) IsBeginGTID(proto.BinlogFormat) bool { return false }
func (fakeEvent) Query(proto.BinlogFormat) (proto.Query, error) {
	return proto.Query{}, errors.New("not a query")
}
func (fakeEvent) IntVar(proto.BinlogFormat) (string, uint64, error) {
	return "", 0, errors.New("not an intvar")
}
func (fakeEvent) Rand(proto.BinlogFormat) (uint64, uint64, error) {
	return 0, 0, errors.New("not a rand")
}
func (ev fakeEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type invalidEvent struct{ fakeEvent }

func (invalidEvent) IsValid() bool { return false }
func (ev invalidEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type rotateEvent struct{ fakeEvent }

func (rotateEvent) IsRotate() bool { return true }
func (ev rotateEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type formatEvent struct{ fakeEvent }

func (formatEvent) IsFormatDescription() bool { return true }
func (formatEvent) Format() (proto.BinlogFormat, error) {
	return proto.BinlogFormat{FormatVersion: 1}, nil
}
func (ev formatEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type invalidFormatEvent struct{ formatEvent }

func (invalidFormatEvent) Format() (proto.BinlogFormat, error) {
	return proto.BinlogFormat{}, errors.New("invalid format event")
}
func (ev invalidFormatEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type queryEvent struct {
	fakeEvent
	query proto.Query
}

func (queryEvent) IsQuery() bool { return true }
func (ev queryEvent) Query(proto.BinlogFormat) (proto.Query, error) {
	return ev.query, nil
}
func (ev queryEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type invalidQueryEvent struct{ queryEvent }

func (invalidQueryEvent) Query(proto.BinlogFormat) (proto.Query, error) {
	return proto.Query{}, errors.New("invalid query event")
}
func (ev invalidQueryEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type xidEvent struct{ fakeEvent }

func (xidEvent) IsXID() bool { return true }
func (ev xidEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type intVarEvent struct {
	fakeEvent
	name  string
	value uint64
}

func (intVarEvent) IsIntVar() bool { return true }
func (ev intVarEvent) IntVar(proto.BinlogFormat) (string, uint64, error) {
	return ev.name, ev.value, nil
}
func (ev intVarEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type invalidIntVarEvent struct{ intVarEvent }

func (invalidIntVarEvent) IntVar(proto.BinlogFormat) (string, uint64, error) {
	return "", 0, errors.New("invalid intvar event")
}
func (ev invalidIntVarEvent) StripChecksum(proto.BinlogFormat) (proto.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

// sample MariaDB event data
var (
	mariadbRotateEvent         = mysqlctl.NewMariadbBinlogEvent([]byte{0x0, 0x0, 0x0, 0x0, 0x4, 0x88, 0xf3, 0x0, 0x0, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x20, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x76, 0x74, 0x2d, 0x30, 0x30, 0x30, 0x30, 0x30, 0x36, 0x32, 0x33, 0x34, 0x34, 0x2d, 0x62, 0x69, 0x6e, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x31})
	mariadbFormatEvent         = mysqlctl.NewMariadbBinlogEvent([]byte{0x87, 0x41, 0x9, 0x54, 0xf, 0x88, 0xf3, 0x0, 0x0, 0xf4, 0x0, 0x0, 0x0, 0xf8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x31, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x33, 0x2d, 0x4d, 0x61, 0x72, 0x69, 0x61, 0x44, 0x42, 0x2d, 0x31, 0x7e, 0x70, 0x72, 0x65, 0x63, 0x69, 0x73, 0x65, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x87, 0x41, 0x9, 0x54, 0x13, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0xdc, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2, 0x0, 0x0, 0x0, 0xa, 0xa, 0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x13, 0x4, 0x0, 0x6e, 0xe0, 0xfd, 0x41})
	mariadbStandaloneGTIDEvent = mysqlctl.NewMariadbBinlogEvent([]byte{0x88, 0x41, 0x9, 0x54, 0xa2, 0x88, 0xf3, 0x0, 0x0, 0x26, 0x0, 0x0, 0x0, 0xcf, 0x8, 0x0, 0x0, 0x8, 0x0, 0x9, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0})
	mariadbBeginGTIDEvent      = mysqlctl.NewMariadbBinlogEvent([]byte{0x88, 0x41, 0x9, 0x54, 0xa2, 0x88, 0xf3, 0x0, 0x0, 0x26, 0x0, 0x0, 0x0, 0xb5, 0x9, 0x0, 0x0, 0x8, 0x0, 0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0})
	mariadbCreateEvent         = mysqlctl.NewMariadbBinlogEvent([]byte{0x88, 0x41, 0x9, 0x54, 0x2, 0x88, 0xf3, 0x0, 0x0, 0xc2, 0x0, 0x0, 0x0, 0xf2, 0x6, 0x0, 0x0, 0x0, 0x0, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x10, 0x0, 0x0, 0x1a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x3, 0x73, 0x74, 0x64, 0x4, 0x8, 0x0, 0x8, 0x0, 0x21, 0x0, 0x76, 0x74, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x0, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x69, 0x66, 0x20, 0x6e, 0x6f, 0x74, 0x20, 0x65, 0x78, 0x69, 0x73, 0x74, 0x73, 0x20, 0x76, 0x74, 0x5f, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x20, 0x28, 0xa, 0x69, 0x64, 0x20, 0x62, 0x69, 0x67, 0x69, 0x6e, 0x74, 0x20, 0x61, 0x75, 0x74, 0x6f, 0x5f, 0x69, 0x6e, 0x63, 0x72, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x2c, 0xa, 0x6d, 0x73, 0x67, 0x20, 0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72, 0x28, 0x36, 0x34, 0x29, 0x2c, 0xa, 0x70, 0x72, 0x69, 0x6d, 0x61, 0x72, 0x79, 0x20, 0x6b, 0x65, 0x79, 0x20, 0x28, 0x69, 0x64, 0x29, 0xa, 0x29, 0x20, 0x45, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x3d, 0x49, 0x6e, 0x6e, 0x6f, 0x44, 0x42})
	mariadbInsertEvent         = mysqlctl.NewMariadbBinlogEvent([]byte{0x88, 0x41, 0x9, 0x54, 0x2, 0x88, 0xf3, 0x0, 0x0, 0xa8, 0x0, 0x0, 0x0, 0x79, 0xa, 0x0, 0x0, 0x0, 0x0, 0x27, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x10, 0x0, 0x0, 0x1a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x3, 0x73, 0x74, 0x64, 0x4, 0x21, 0x0, 0x21, 0x0, 0x21, 0x0, 0x76, 0x74, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x0, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x76, 0x74, 0x5f, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x28, 0x6d, 0x73, 0x67, 0x29, 0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x27, 0x74, 0x65, 0x73, 0x74, 0x20, 0x30, 0x27, 0x29, 0x20, 0x2f, 0x2a, 0x20, 0x5f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x20, 0x76, 0x74, 0x5f, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x20, 0x28, 0x69, 0x64, 0x20, 0x29, 0x20, 0x28, 0x6e, 0x75, 0x6c, 0x6c, 0x20, 0x29, 0x3b, 0x20, 0x2a, 0x2f})
	mariadbXidEvent            = mysqlctl.NewMariadbBinlogEvent([]byte{0x88, 0x41, 0x9, 0x54, 0x10, 0x88, 0xf3, 0x0, 0x0, 0x1b, 0x0, 0x0, 0x0, 0xe0, 0xc, 0x0, 0x0, 0x0, 0x0, 0x85, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0})

	charset = &mproto.Charset{Client: 33, Conn: 33, Server: 33}
)

func sendTestEvents(channel chan<- proto.BinlogEvent, events []proto.BinlogEvent) {
	for _, ev := range events {
		channel <- ev
	}
	close(channel)
}

func TestBinlogStreamerParseEventsXID(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				proto.Statement{Category: proto.BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			Timestamp: 1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestBinlogStreamerParseEventsCommit(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("COMMIT")}},
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				proto.Statement{Category: proto.BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			Timestamp: 1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestBinlogStreamerStop(t *testing.T) {
	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	// Start parseEvents(), but don't send it anything, so it just waits.
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		svm.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Errorf("timed out waiting for binlogConnStreamer.Stop()")
	}
}

func TestBinlogStreamerParseEventsClientEOF(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}
	want := ClientEOF

	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return io.EOF
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	err := svm.Join()
	if err == nil {
		t.Errorf("expected error, got none")
	}
	if err != want {
		t.Errorf("wrong error, got %#v, want %#v", err, want)
	}
}

func TestBinlogStreamerParseEventsServerEOF(t *testing.T) {
	want := ServerEOF

	events := make(chan proto.BinlogEvent)
	close(events)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	err := svm.Join()
	if err == nil {
		t.Errorf("expected error, got none")
	}
	if err != want {
		t.Errorf("wrong error, got %#v, want %#v", err, want)
	}
}

func TestBinlogStreamerParseEventsSendErrorXID(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}
	want := "send reply error: foobar"

	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return fmt.Errorf("foobar")
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	err := svm.Join()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogStreamerParseEventsSendErrorCommit(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("COMMIT")}},
	}
	want := "send reply error: foobar"

	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return fmt.Errorf("foobar")
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	err := svm.Join()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogStreamerParseEventsInvalid(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		invalidEvent{},
		xidEvent{},
	}
	want := "can't parse binlog event, invalid data:"

	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	err := svm.Join()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogStreamerParseEventsInvalidFormat(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		invalidFormatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}
	want := "can't parse FORMAT_DESCRIPTION_EVENT:"

	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	err := svm.Join()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogStreamerParseEventsNoFormat(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		//formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}
	want := "got a real event before FORMAT_DESCRIPTION_EVENT:"

	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	err := svm.Join()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogStreamerParseEventsInvalidQuery(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		invalidQueryEvent{},
		xidEvent{},
	}
	want := "can't get query from binlog event:"

	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	err := svm.Join()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogStreamerParseEventsRollback(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("ROLLBACK")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: nil,
			Timestamp:  1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				proto.Statement{Category: proto.BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			Timestamp: 1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestBinlogStreamerParseEventsDMLWithoutBegin(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				proto.Statement{Category: proto.BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			Timestamp: 1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
		proto.BinlogTransaction{
			Statements: nil,
			Timestamp:  1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestBinlogStreamerParseEventsBeginWithoutCommit(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		xidEvent{},
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				proto.Statement{Category: proto.BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			Timestamp: 1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
		proto.BinlogTransaction{
			Statements: []proto.Statement{},
			Timestamp:  1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestBinlogStreamerParseEventsSetInsertID(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		intVarEvent{name: "INSERT_ID", value: 101},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Sql: []byte("SET INSERT_ID=101")},
				proto.Statement{Category: proto.BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				proto.Statement{Category: proto.BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			Timestamp: 1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestBinlogStreamerParseEventsInvalidIntVar(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		invalidIntVarEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}
	want := "can't parse INTVAR_EVENT:"

	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	err := svm.Join()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogStreamerParseEventsOtherDB(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "other", Sql: []byte("INSERT INTO test values (3, 4)")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				proto.Statement{Category: proto.BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			Timestamp: 1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestBinlogStreamerParseEventsOtherDBBegin(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "other", Sql: []byte("BEGIN")}}, // Check that this doesn't get filtered out.
		queryEvent{query: proto.Query{Database: "other", Sql: []byte("INSERT INTO test values (3, 4)")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		xidEvent{},
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				proto.Statement{Category: proto.BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			Timestamp: 1407805592,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0x0d}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestBinlogStreamerParseEventsBeginAgain(t *testing.T) {
	input := []proto.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")}},
		queryEvent{query: proto.Query{Database: "vt_test_keyspace", Sql: []byte("BEGIN")}},
	}

	events := make(chan proto.BinlogEvent)

	sendTransaction := func(trans *proto.BinlogTransaction) error {
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)
	before := binlogStreamerErrors.Counts()["ParseEvents"]

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}
	after := binlogStreamerErrors.Counts()["ParseEvents"]
	if got := after - before; got != 1 {
		t.Errorf("error count change = %v, want 1", got)
	}
}

func TestBinlogStreamerParseEventsMariadbBeginGTID(t *testing.T) {
	input := []proto.BinlogEvent{
		mariadbRotateEvent,
		mariadbFormatEvent,
		mariadbBeginGTIDEvent,
		mariadbInsertEvent,
		mariadbXidEvent,
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Charset: charset, Sql: []byte("SET TIMESTAMP=1409892744")},
				proto.Statement{Category: proto.BL_DML, Charset: charset, Sql: []byte("insert into vt_insert_test(msg) values ('test 0') /* _stream vt_insert_test (id ) (null ); */")},
			},
			Timestamp: 1409892744,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 10}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestBinlogStreamerParseEventsMariadbStandaloneGTID(t *testing.T) {
	input := []proto.BinlogEvent{
		mariadbRotateEvent,
		mariadbFormatEvent,
		mariadbStandaloneGTIDEvent,
		mariadbCreateEvent,
	}

	events := make(chan proto.BinlogEvent)

	want := []proto.BinlogTransaction{
		proto.BinlogTransaction{
			Statements: []proto.Statement{
				proto.Statement{Category: proto.BL_SET, Charset: &mproto.Charset{Client: 8, Conn: 8, Server: 33}, Sql: []byte("SET TIMESTAMP=1409892744")},
				proto.Statement{Category: proto.BL_DDL, Charset: &mproto.Charset{Client: 8, Conn: 8, Server: 33}, Sql: []byte("create table if not exists vt_insert_test (\nid bigint auto_increment,\nmsg varchar(64),\nprimary key (id)\n) Engine=InnoDB")},
			},
			Timestamp: 1409892744,
			GTIDField: myproto.GTIDField{
				Value: myproto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 9}},
		},
	}
	var got []proto.BinlogTransaction
	sendTransaction := func(trans *proto.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewBinlogStreamer("vt_test_keyspace", nil, nil, myproto.ReplicationPosition{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestGetStatementCategory(t *testing.T) {
	table := map[string]int{
		"":  proto.BL_UNRECOGNIZED,
		" ": proto.BL_UNRECOGNIZED,
		" UPDATE we don't try to fix leading spaces": proto.BL_UNRECOGNIZED,
		"FOOBAR unknown query prefix":                proto.BL_UNRECOGNIZED,

		"BEGIN":    proto.BL_BEGIN,
		"COMMIT":   proto.BL_COMMIT,
		"ROLLBACK": proto.BL_ROLLBACK,
		"INSERT something (something, something)": proto.BL_DML,
		"UPDATE something SET something=nothing":  proto.BL_DML,
		"DELETE something":                        proto.BL_DML,
		"CREATE something":                        proto.BL_DDL,
		"ALTER something":                         proto.BL_DDL,
		"DROP something":                          proto.BL_DDL,
		"TRUNCATE something":                      proto.BL_DDL,
		"RENAME something":                        proto.BL_DDL,
		"SET something=nothing":                   proto.BL_SET,
	}

	for input, want := range table {
		if got := getStatementCategory([]byte(input)); got != want {
			t.Errorf("getStatementCategory(%v) = %v, want %v", input, got, want)
		}
	}
}
