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

	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// fakeEvent implements replication.BinlogEvent.
type fakeEvent struct{}

func (fakeEvent) IsValid() bool                         { return true }
func (fakeEvent) IsFormatDescription() bool             { return false }
func (fakeEvent) IsQuery() bool                         { return false }
func (fakeEvent) IsXID() bool                           { return false }
func (fakeEvent) IsGTID() bool                          { return false }
func (fakeEvent) IsRotate() bool                        { return false }
func (fakeEvent) IsIntVar() bool                        { return false }
func (fakeEvent) IsRand() bool                          { return false }
func (fakeEvent) HasGTID(replication.BinlogFormat) bool { return true }
func (fakeEvent) Timestamp() uint32                     { return 1407805592 }
func (fakeEvent) Format() (replication.BinlogFormat, error) {
	return replication.BinlogFormat{}, errors.New("not a format")
}
func (fakeEvent) GTID(replication.BinlogFormat) (replication.GTID, error) {
	return replication.MariadbGTID{Domain: 0, Server: 62344, Sequence: 0xd}, nil
}
func (fakeEvent) IsBeginGTID(replication.BinlogFormat) bool { return false }
func (fakeEvent) Query(replication.BinlogFormat) (replication.Query, error) {
	return replication.Query{}, errors.New("not a query")
}
func (fakeEvent) IntVar(replication.BinlogFormat) (string, uint64, error) {
	return "", 0, errors.New("not an intvar")
}
func (fakeEvent) Rand(replication.BinlogFormat) (uint64, uint64, error) {
	return 0, 0, errors.New("not a rand")
}
func (ev fakeEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type invalidEvent struct{ fakeEvent }

func (invalidEvent) IsValid() bool { return false }
func (ev invalidEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type rotateEvent struct{ fakeEvent }

func (rotateEvent) IsRotate() bool { return true }
func (ev rotateEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type formatEvent struct{ fakeEvent }

func (formatEvent) IsFormatDescription() bool { return true }
func (formatEvent) Format() (replication.BinlogFormat, error) {
	return replication.BinlogFormat{FormatVersion: 1}, nil
}
func (ev formatEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type invalidFormatEvent struct{ formatEvent }

func (invalidFormatEvent) Format() (replication.BinlogFormat, error) {
	return replication.BinlogFormat{}, errors.New("invalid format event")
}
func (ev invalidFormatEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type queryEvent struct {
	fakeEvent
	query replication.Query
}

func (queryEvent) IsQuery() bool { return true }
func (ev queryEvent) Query(replication.BinlogFormat) (replication.Query, error) {
	return ev.query, nil
}
func (ev queryEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type invalidQueryEvent struct{ queryEvent }

func (invalidQueryEvent) Query(replication.BinlogFormat) (replication.Query, error) {
	return replication.Query{}, errors.New("invalid query event")
}
func (ev invalidQueryEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type xidEvent struct{ fakeEvent }

func (xidEvent) IsXID() bool { return true }
func (ev xidEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type intVarEvent struct {
	fakeEvent
	name  string
	value uint64
}

func (intVarEvent) IsIntVar() bool { return true }
func (ev intVarEvent) IntVar(replication.BinlogFormat) (string, uint64, error) {
	return ev.name, ev.value, nil
}
func (ev intVarEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	return ev, nil, nil
}

type invalidIntVarEvent struct{ intVarEvent }

func (invalidIntVarEvent) IntVar(replication.BinlogFormat) (string, uint64, error) {
	return "", 0, errors.New("invalid intvar event")
}
func (ev invalidIntVarEvent) StripChecksum(replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
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

	charset = &binlogdatapb.Charset{Client: 33, Conn: 33, Server: 33}
)

func sendTestEvents(channel chan<- replication.BinlogEvent, events []replication.BinlogEvent) {
	for _, ev := range events {
		channel <- ev
	}
	close(channel)
}

func TestStreamerParseEventsXID(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsCommit(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "COMMIT"}},
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerStop(t *testing.T) {
	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsClientEOF(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}
	want := ErrClientEOF

	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return io.EOF
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsServerEOF(t *testing.T) {
	want := ErrServerEOF

	events := make(chan replication.BinlogEvent)
	close(events)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsSendErrorXID(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}
	want := "send reply error: foobar"

	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return fmt.Errorf("foobar")
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsSendErrorCommit(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "COMMIT"}},
	}
	want := "send reply error: foobar"

	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return fmt.Errorf("foobar")
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsInvalid(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		invalidEvent{},
		xidEvent{},
	}
	want := "can't parse binlog event, invalid data:"

	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsInvalidFormat(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		invalidFormatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}
	want := "can't parse FORMAT_DESCRIPTION_EVENT:"

	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsNoFormat(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		//formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}
	want := "got a real event before FORMAT_DESCRIPTION_EVENT:"

	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsInvalidQuery(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		invalidQueryEvent{},
		xidEvent{},
	}
	want := "can't get query from binlog event:"

	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsRollback(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "ROLLBACK"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: nil,
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsDMLWithoutBegin(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
		{
			Statements: nil,
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsBeginWithoutCommit(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		xidEvent{},
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsSetInsertID(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		intVarEvent{name: "INSERT_ID", value: 101},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET INSERT_ID=101")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsInvalidIntVar(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		invalidIntVarEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}
	want := "can't parse INTVAR_EVENT:"

	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

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

func TestStreamerParseEventsOtherDB(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "other",
			SQL:      "INSERT INTO test values (3, 4)"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsOtherDBBegin(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "other",
			SQL:      "BEGIN"}}, // Check that this doesn't get filtered out.
		queryEvent{query: replication.Query{
			Database: "other",
			SQL:      "INSERT INTO test values (3, 4)"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		xidEvent{},
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DML, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsBeginAgain(t *testing.T) {
	input := []replication.BinlogEvent{
		rotateEvent{},
		formatEvent{},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}},
		queryEvent{query: replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}},
	}

	events := make(chan replication.BinlogEvent)

	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)
	before := binlogStreamerErrors.Counts()["ParseEvents"]

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}
	after := binlogStreamerErrors.Counts()["ParseEvents"]
	if got := after - before; got != 1 {
		t.Errorf("error count change = %v, want 1", got)
	}
}

func TestStreamerParseEventsMariadbBeginGTID(t *testing.T) {
	input := []replication.BinlogEvent{
		mariadbRotateEvent,
		mariadbFormatEvent,
		mariadbBeginGTIDEvent,
		mariadbInsertEvent,
		mariadbXidEvent,
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Charset: charset, Sql: []byte("SET TIMESTAMP=1409892744")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DML, Charset: charset, Sql: []byte("insert into vt_insert_test(msg) values ('test 0') /* _stream vt_insert_test (id ) (null ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1409892744,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 10,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsMariadbStandaloneGTID(t *testing.T) {
	input := []replication.BinlogEvent{
		mariadbRotateEvent,
		mariadbFormatEvent,
		mariadbStandaloneGTIDEvent,
		mariadbCreateEvent,
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Charset: &binlogdatapb.Charset{Client: 8, Conn: 8, Server: 33}, Sql: []byte("SET TIMESTAMP=1409892744")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DDL, Charset: &binlogdatapb.Charset{Client: 8, Conn: 8, Server: 33}, Sql: []byte("create table if not exists vt_insert_test (\nid bigint auto_increment,\nmsg varchar(64),\nprimary key (id)\n) Engine=InnoDB")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1409892744,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 9,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, nil, replication.Position{}, sendTransaction)

	go sendTestEvents(events, input)
	svm := &sync2.ServiceManager{}
	svm.Go(func(ctx *sync2.ServiceContext) error {
		_, err := bls.parseEvents(ctx, events)
		return err
	})
	if err := svm.Join(); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestGetStatementCategory(t *testing.T) {
	table := map[string]binlogdatapb.BinlogTransaction_Statement_Category{
		"":  binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED,
		" ": binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED,
		" UPDATE we don't try to fix leading spaces": binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED,
		"FOOBAR unknown query prefix":                binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED,

		"BEGIN":    binlogdatapb.BinlogTransaction_Statement_BL_BEGIN,
		"COMMIT":   binlogdatapb.BinlogTransaction_Statement_BL_COMMIT,
		"ROLLBACK": binlogdatapb.BinlogTransaction_Statement_BL_ROLLBACK,
		"INSERT something (something, something)": binlogdatapb.BinlogTransaction_Statement_BL_DML,
		"UPDATE something SET something=nothing":  binlogdatapb.BinlogTransaction_Statement_BL_DML,
		"DELETE something":                        binlogdatapb.BinlogTransaction_Statement_BL_DML,
		"CREATE something":                        binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"ALTER something":                         binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"DROP something":                          binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"TRUNCATE something":                      binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"RENAME something":                        binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"SET something=nothing":                   binlogdatapb.BinlogTransaction_Statement_BL_SET,
	}

	for input, want := range table {
		if got := getStatementCategory(input); got != want {
			t.Errorf("getStatementCategory(%v) = %v, want %v", input, got, want)
		}
	}
}
