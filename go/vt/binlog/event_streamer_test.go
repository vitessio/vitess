// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/binlog/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

type eventErrorCase struct {
	Category int
	Sql      string
	want     string
}

var eventErrorCases = []eventErrorCase{
	{
		Category: proto.BL_SET,
		Sql:      "abcd",
		want:     `unrecognized: abcd`,
	}, {
		Category: proto.BL_SET,
		Sql:      "SET TIMESTAMP=abcd",
		want:     `strconv.ParseInt: parsing "abcd": invalid syntax: SET TIMESTAMP=abcd`,
	}, {
		Category: proto.BL_SET,
		Sql:      "SET INSERT_ID=abcd",
		want:     `strconv.ParseInt: parsing "abcd": invalid syntax: SET INSERT_ID=abcd`,
	}, {
		Category: proto.BL_DML,
		Sql:      "query /* _stream 10 (eid id name ) (null 1 'bmFtZQ==' ); */",
		want:     `expecting table name in stream comment: query /* _stream 10 (eid id name ) (null 1 'bmFtZQ==' ); */`,
	}, {
		Category: proto.BL_DML,
		Sql:      "query /* _stream vtocc_e eid id name ) (null 1 'bmFtZQ==' ); */",
		want:     `expecting '(': query /* _stream vtocc_e eid id name ) (null 1 'bmFtZQ==' ); */`,
	}, {
		Category: proto.BL_DML,
		Sql:      "query /* _stream vtocc_e (10 id name ) (null 1 'bmFtZQ==' ); */",
		want:     `syntax error at position: 12: query /* _stream vtocc_e (10 id name ) (null 1 'bmFtZQ==' ); */`,
	}, {
		Category: proto.BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name  (null 1 'bmFtZQ==' ); */",
		want:     `syntax error at position: 24: query /* _stream vtocc_e (eid id name  (null 1 'bmFtZQ==' ); */`,
	}, {
		Category: proto.BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name)  (null 'aaa' 'bmFtZQ==' ); */",
		want:     `illegal base64 data at input byte 0: query /* _stream vtocc_e (eid id name)  (null 'aaa' 'bmFtZQ==' ); */`,
	}, {
		Category: proto.BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name)  (null 'bmFtZQ==' ); */",
		want:     `length mismatch in values: query /* _stream vtocc_e (eid id name)  (null 'bmFtZQ==' ); */`,
	}, {
		Category: proto.BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name)  (null 1.1 'bmFtZQ==' ); */",
		want:     `strconv.ParseUint: parsing "1.1": invalid syntax: query /* _stream vtocc_e (eid id name)  (null 1.1 'bmFtZQ==' ); */`,
	}, {
		Category: proto.BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name)  (null a 'bmFtZQ==' ); */",
		want:     `syntax error at position: 31: query /* _stream vtocc_e (eid id name)  (null a 'bmFtZQ==' ); */`,
	},
}

func TestEventErrors(t *testing.T) {
	evs := &EventStreamer{
		sendEvent: func(event *proto.StreamEvent) error {
			return nil
		},
	}
	for _, ecase := range eventErrorCases {
		trans := &proto.BinlogTransaction{
			Statements: []proto.Statement{
				{
					Category: ecase.Category,
					Sql:      []byte(ecase.Sql),
				},
			},
		}
		err := evs.transactionToEvent(trans)
		if ecase.want != err.Error() {
			t.Errorf("want \n%q, got \n%q", ecase.want, err.Error())
		}
	}
}

func TestDMLEvent(t *testing.T) {
	trans := &proto.BinlogTransaction{
		Statements: []proto.Statement{
			{
				Category: proto.BL_SET,
				Sql:      []byte("SET TIMESTAMP=1"),
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
		GTIDField: myproto.GTIDField{myproto.MustParseGTID(blsMysqlFlavor, "20")},
	}
	evs := &EventStreamer{
		sendEvent: func(event *proto.StreamEvent) error {
			switch event.Category {
			case "DML":
				want := `&{DML vtocc_e [eid id name] [[10 -1 [110 97 109 101]] [11 18446744073709551615 [110 97 109 101]]]  1 <nil>}`
				got := fmt.Sprintf("%v", event)
				if want != got {
					t.Errorf("want \n%s, got \n%s", want, got)
				}
			case "ERR":
				want := `&{ERR  [] [] query 1 <nil>}`
				got := fmt.Sprintf("%v", event)
				if want != got {
					t.Errorf("want %s, got %s", want, got)
				}
			case "POS":
				want := `&{POS  [] []  0 20}`
				got := fmt.Sprintf("%v", event)
				if want != got {
					t.Errorf("want %s, got %s", want, got)
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
				Sql:      []byte("SET TIMESTAMP=1"),
			}, {
				Category: proto.BL_DDL,
				Sql:      []byte("DDL"),
			},
		},
		GTIDField: myproto.GTIDField{myproto.MustParseGTID(blsMysqlFlavor, "20")},
	}
	evs := &EventStreamer{
		sendEvent: func(event *proto.StreamEvent) error {
			switch event.Category {
			case "DDL":
				want := `&{DDL  [] [] DDL 1 <nil>}`
				got := fmt.Sprintf("%v", event)
				if want != got {
					t.Errorf("want %s, got %s", want, got)
				}
			case "POS":
				want := `&{POS  [] []  0 20}`
				got := fmt.Sprintf("%v", event)
				if want != got {
					t.Errorf("want %s, got %s", want, got)
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
