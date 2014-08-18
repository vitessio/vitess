// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/binlog/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var dmlErrorCases = []string{
	"query",
	"query /* _stream 10 (eid id name ) (null 1 'bmFtZQ==' ); */",
	"query /* _stream vtocc_e eid id name ) (null 1 'bmFtZQ==' ); */",
	"query /* _stream vtocc_e (10 id name ) (null 1 'bmFtZQ==' ); */",
	"query /* _stream vtocc_e (eid id name  (null 1 'bmFtZQ==' ); */",
	"query /* _stream vtocc_e (eid id name)  (null 'aaa' 'bmFtZQ==' ); */",
	"query /* _stream vtocc_e (eid id name)  (null 'bmFtZQ==' ); */",
	"query /* _stream vtocc_e (eid id name)  (null 1.1 'bmFtZQ==' ); */",
	"query /* _stream vtocc_e (eid id name)  (null a 'bmFtZQ==' ); */",
}

func TestEventErrors(t *testing.T) {
	var got *proto.StreamEvent
	evs := &EventStreamer{
		sendEvent: func(event *proto.StreamEvent) error {
			if event.Category != "POS" {
				got = event
			}
			return nil
		},
	}
	for _, sql := range dmlErrorCases {
		trans := &proto.BinlogTransaction{
			Statements: []proto.Statement{
				{
					Category: proto.BL_DML,
					Sql:      []byte(sql),
				},
			},
		}
		err := evs.transactionToEvent(trans)
		if err != nil {
			t.Errorf("%s: %v", sql, err)
			continue
		}
		want := &proto.StreamEvent{
			Category: "ERR",
			Sql:      sql,
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got: %+v, want: %+v", got, want)
		}
	}
}

func TestSetErrors(t *testing.T) {
	evs := &EventStreamer{
		sendEvent: func(event *proto.StreamEvent) error {
			return nil
		},
	}
	trans := &proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("SET INSERT_ID=abcd"),
			},
		},
	}
	before := binlogStreamerErrors.Counts()["EventStreamer"]
	err := evs.transactionToEvent(trans)
	if err != nil {
		t.Error(err)
	}
	got := binlogStreamerErrors.Counts()["EventStreamer"]
	if got != before+1 {
		t.Errorf("got: %v, want: %+v", got, before+1)
	}
}

func TestDMLEvent(t *testing.T) {
	trans := &proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("SET TIMESTAMP=2"),
			}, {
				Category: proto.BL_SET,
				Sql:      []byte("SET INSERT_ID=10"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("query /* _stream vtocc_e (eid id name)  (null -1 'bmFtZQ==' ) (null 18446744073709551615 'bmFtZQ==' ); */"),
			}, {
				Category: proto.BL_DML,
				Sql:      []byte("query"),
			},
		},
		Timestamp: 1,
		GTIDField: myproto.GTIDField{Value: myproto.MustParseGTID(blsMysqlFlavor, "20")},
	}
	evs := &EventStreamer{
		sendEvent: func(event *proto.StreamEvent) error {
			switch event.Category {
			case "DML":
				want := `&{DML vtocc_e [eid id name] [[10 -1 [110 97 109 101]] [11 18446744073709551615 [110 97 109 101]]]  1 <nil>}`
				got := fmt.Sprintf("%v", event)
				if got != want {
					t.Errorf("got \n%s, want \n%s", got, want)
				}
			case "ERR":
				want := `&{ERR  [] [] query 1 <nil>}`
				got := fmt.Sprintf("%v", event)
				if got != want {
					t.Errorf("got %s, want %s", got, want)
				}
			case "POS":
				want := `&{POS  [] []  1 20}`
				got := fmt.Sprintf("%v", event)
				if got != want {
					t.Errorf("got %s, want %s", got, want)
				}
			default:
				t.Errorf("unexppected: %#v", event)
			}
			return nil
		},
	}
	err := evs.transactionToEvent(trans)
	if err != nil {
		t.Error(err)
	}
}

func TestDDLEvent(t *testing.T) {
	trans := &proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("SET TIMESTAMP=2"),
			}, {
				Category: proto.BL_DDL,
				Sql:      []byte("DDL"),
			},
		},
		Timestamp: 1,
		GTIDField: myproto.GTIDField{Value: myproto.MustParseGTID(blsMysqlFlavor, "20")},
	}
	evs := &EventStreamer{
		sendEvent: func(event *proto.StreamEvent) error {
			switch event.Category {
			case "DDL":
				want := `&{DDL  [] [] DDL 1 <nil>}`
				got := fmt.Sprintf("%v", event)
				if got != want {
					t.Errorf("got %s, want %s", got, want)
				}
			case "POS":
				want := `&{POS  [] []  1 20}`
				got := fmt.Sprintf("%v", event)
				if got != want {
					t.Errorf("got %s, want %s", got, want)
				}
			default:
				t.Errorf("unexppected: %#v", event)
			}
			return nil
		},
	}
	err := evs.transactionToEvent(trans)
	if err != nil {
		t.Error(err)
	}
}
