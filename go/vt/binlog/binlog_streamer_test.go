/*
Copyright 2017 Google Inc.

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

package binlog

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// fullBinlogTransaction is a helper type for tests.
type fullBinlogTransaction struct {
	eventToken *querypb.EventToken
	statements []FullBinlogStatement
}

type binlogStatements []binlogdatapb.BinlogTransaction

func (bs *binlogStatements) sendTransaction(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
	var s []*binlogdatapb.BinlogTransaction_Statement
	if len(statements) > 0 {
		s = make([]*binlogdatapb.BinlogTransaction_Statement, len(statements))
		for i, statement := range statements {
			s[i] = statement.Statement
		}
	}
	*bs = append(*bs, binlogdatapb.BinlogTransaction{
		Statements: s,
		EventToken: eventToken,
	})
	return nil
}

func (bs *binlogStatements) equal(bts []binlogdatapb.BinlogTransaction) bool {
	if len(*bs) != len(bts) {
		return false
	}
	for i, s := range *bs {
		if !proto.Equal(&s, &bts[i]) {
			return false
		}
	}
	return true
}

func sendTestEvents(channel chan<- mysql.BinlogEvent, events []mysql.BinlogEvent) {
	for _, ev := range events {
		channel <- ev
	}
	close(channel)
}

func TestStreamerParseEventsXID(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got:\n%v\nwant:\n%v", got, want)
	}
}

func TestStreamerParseEventsCommit(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "COMMIT"}),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerStop(t *testing.T) {
	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return nil
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	// Start parseEvents(), but don't send it anything, so it just waits.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		_, err := bls.parseEvents(ctx, events)
		done <- err
	}()

	// close the context, expect the parser to return
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("wrong context interruption returned value: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("timed out waiting for binlogConnStreamer.Stop()")
	}
}

func TestStreamerParseEventsClientEOF(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}
	want := ErrClientEOF

	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return io.EOF
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err != want {
		t.Errorf("wrong error, got %#v, want %#v", err, want)
	}
}

func TestStreamerParseEventsServerEOF(t *testing.T) {
	want := ErrServerEOF

	events := make(chan mysql.BinlogEvent)
	close(events)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return nil
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	_, err := bls.parseEvents(context.Background(), events)
	if err != want {
		t.Errorf("wrong error, got %#v, want %#v", err, want)
	}
}

func TestStreamerParseEventsSendErrorXID(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}
	want := "send reply error: foobar"

	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return fmt.Errorf("foobar")
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)

	_, err := bls.parseEvents(context.Background(), events)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestStreamerParseEventsSendErrorCommit(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "COMMIT"}),
	}
	want := "send reply error: foobar"

	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return fmt.Errorf("foobar")
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestStreamerParseEventsInvalid(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewInvalidEvent(),
		mysql.NewXIDEvent(f, s),
	}
	want := "can't parse binlog event, invalid data:"

	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return nil
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestStreamerParseEventsInvalidFormat(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewInvalidFormatDescriptionEvent(f, s),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}
	want := "can't parse FORMAT_DESCRIPTION_EVENT:"

	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return nil
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestStreamerParseEventsNoFormat(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		//mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}
	want := "got a real event before FORMAT_DESCRIPTION_EVENT:"

	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return nil
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestStreamerParseEventsInvalidQuery(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewInvalidQueryEvent(f, s),
		mysql.NewXIDEvent(f, s),
	}
	want := "can't get query from binlog event:"

	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return nil
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestStreamerParseEventsRollback(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "ROLLBACK"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: nil,
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
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
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	if _, err := bls.parseEvents(context.Background(), events); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got:\n%v\nwant:\n%v", got, want)
	}
}

func TestStreamerParseEventsDMLWithoutBegin(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
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
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	if _, err := bls.parseEvents(context.Background(), events); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got:\n%v\nwant:\n%v", got, want)
	}
}

func TestStreamerParseEventsBeginWithoutCommit(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
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
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	if _, err := bls.parseEvents(context.Background(), events); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got:\n%v\nwant:\n%v", got, want)
	}
}

func TestStreamerParseEventsSetInsertID(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewIntVarEvent(f, s, mysql.IntVarInsertID, 101),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET INSERT_ID=101")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	if _, err := bls.parseEvents(context.Background(), events); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsInvalidIntVar(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewIntVarEvent(f, s, mysql.IntVarInvalidInt, 0), // Invalid intvar.
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}
	want := "can't parse INTVAR_EVENT:"

	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return nil
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestStreamerParseEventsOtherDB(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "other",
			SQL:      "INSERT INTO test values (3, 4)"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	if _, err := bls.parseEvents(context.Background(), events); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsOtherDBBegin(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "other",
			SQL:      "BEGIN"}), // Check that this doesn't get filtered out.
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "other",
			SQL:      "INSERT INTO test values (3, 4)"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Sql: []byte("SET TIMESTAMP=1407805592")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	if _, err := bls.parseEvents(context.Background(), events); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got %v, want %v", got, want)
	}
}

func TestStreamerParseEventsBeginAgain(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "insert into vt_a(eid, id) values (1, 1) /* _stream vt_a (eid id ) (1 1 ); */"}),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
	}

	events := make(chan mysql.BinlogEvent)

	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		return nil
	}
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, sendTransaction)
	before := binlogStreamerErrors.Counts()["ParseEvents"]

	go sendTestEvents(events, input)
	if _, err := bls.parseEvents(context.Background(), events); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}
	after := binlogStreamerErrors.Counts()["ParseEvents"]
	if got := after - before; got != 1 {
		t.Errorf("error count change = %v, want 1", got)
	}
}

// TestStreamerParseEventsMariadbStandaloneGTID tests a MariaDB server
// with no checksum, using a GTID with a Begin.
func TestStreamerParseEventsMariadbBeginGTID(t *testing.T) {
	f := mysql.NewMariaDBBinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344
	s.Timestamp = 1409892744

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 4, "filename.0001"),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 10}, true /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Charset: &binlogdatapb.Charset{Client: 33, Conn: 33, Server: 33},
			SQL:     "insert into vt_insert_test(msg) values ('test 0') /* _stream vt_insert_test (id ) (null ); */",
		}),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
					Charset:  &binlogdatapb.Charset{Client: 33, Conn: 33, Server: 33},
					Sql:      []byte("SET TIMESTAMP=1409892744"),
				},
				{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
					Charset:  &binlogdatapb.Charset{Client: 33, Conn: 33, Server: 33},
					Sql:      []byte("insert into vt_insert_test(msg) values ('test 0') /* _stream vt_insert_test (id ) (null ); */"),
				},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1409892744,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 10,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	if _, err := bls.parseEvents(context.Background(), events); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got:\n%v\nwant:\n%v", got, want)
	}
}

// TestStreamerParseEventsMariadbStandaloneGTID tests a MariaDB server
// with no checksum, using a standalone GTID.
func TestStreamerParseEventsMariadbStandaloneGTID(t *testing.T) {
	f := mysql.NewMariaDBBinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344
	s.Timestamp = 1409892744

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 4, "filename.0001"),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 9}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Charset: &binlogdatapb.Charset{Client: 8, Conn: 8, Server: 33},
			SQL:     "create table if not exists vt_insert_test (\nid bigint auto_increment,\nmsg varchar(64),\nprimary key (id)\n) Engine=InnoDB",
		}),
	}

	events := make(chan mysql.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_SET, Charset: &binlogdatapb.Charset{Client: 8, Conn: 8, Server: 33}, Sql: []byte("SET TIMESTAMP=1409892744")},
				{Category: binlogdatapb.BinlogTransaction_Statement_BL_DDL, Charset: &binlogdatapb.Charset{Client: 8, Conn: 8, Server: 33}, Sql: []byte("create table if not exists vt_insert_test (\nid bigint auto_increment,\nmsg varchar(64),\nprimary key (id)\n) Engine=InnoDB")},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1409892744,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 9,
					},
				}),
			},
		},
	}
	var got binlogStatements
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, nil, nil, mysql.Position{}, 0, (&got).sendTransaction)

	go sendTestEvents(events, input)
	if _, err := bls.parseEvents(context.Background(), events); err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.equal(want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got:\n%v\nwant:\n%v", got, want)
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
		"INSERT something (something, something)": binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
		"UPDATE something SET something=nothing":  binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
		"DELETE something":                        binlogdatapb.BinlogTransaction_Statement_BL_DELETE,
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
