// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"testing"
)

type eventErrorCase struct {
	Category int
	Sql      string
	want     string
}

var eventErrorCases = []eventErrorCase{
	{
		Category: BL_SET,
		Sql:      "abcd",
		want:     `unrecognized: abcd`,
	}, {
		Category: BL_SET,
		Sql:      "SET TIMESTAMP=abcd",
		want:     `strconv.ParseInt: parsing "abcd": invalid syntax: SET TIMESTAMP=abcd`,
	}, {
		Category: BL_SET,
		Sql:      "SET INSERT_ID=abcd",
		want:     `strconv.ParseInt: parsing "abcd": invalid syntax: SET INSERT_ID=abcd`,
	}, {
		Category: BL_DML,
		Sql:      "query /* _stream 10 (eid id name ) (null 1 'bmFtZQ==' ); */",
		want:     `expecting table name in stream comment: query /* _stream 10 (eid id name ) (null 1 'bmFtZQ==' ); */`,
	}, {
		Category: BL_DML,
		Sql:      "query /* _stream vtocc_e eid id name ) (null 1 'bmFtZQ==' ); */",
		want:     `expecting '(': query /* _stream vtocc_e eid id name ) (null 1 'bmFtZQ==' ); */`,
	}, {
		Category: BL_DML,
		Sql:      "query /* _stream vtocc_e (10 id name ) (null 1 'bmFtZQ==' ); */",
		want:     `expecting column name: 10: query /* _stream vtocc_e (10 id name ) (null 1 'bmFtZQ==' ); */`,
	}, {
		Category: BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name  (null 1 'bmFtZQ==' ); */",
		want:     `unexpected token: '(': query /* _stream vtocc_e (eid id name  (null 1 'bmFtZQ==' ); */`,
	}, {
		Category: BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name)  (null 'aaa' 'bmFtZQ==' ); */",
		want:     `illegal base64 data at input byte 0: query /* _stream vtocc_e (eid id name)  (null 'aaa' 'bmFtZQ==' ); */`,
	}, {
		Category: BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name)  (null 'bmFtZQ==' ); */",
		want:     `length mismatch in values: query /* _stream vtocc_e (eid id name)  (null 'bmFtZQ==' ); */`,
	}, {
		Category: BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name)  (null 1.1 'bmFtZQ==' ); */",
		want:     `strconv.ParseUint: parsing "1.1": invalid syntax: query /* _stream vtocc_e (eid id name)  (null 1.1 'bmFtZQ==' ); */`,
	}, {
		Category: BL_DML,
		Sql:      "query /* _stream vtocc_e (eid id name)  (null a 'bmFtZQ==' ); */",
		want:     `unexpected token: 'a': query /* _stream vtocc_e (eid id name)  (null a 'bmFtZQ==' ); */`,
	},
}

func TestEventErrors(t *testing.T) {
	evs := &EventStreamer{
		sendEvent: func(event *StreamEvent) error {
			return nil
		},
	}
	for _, ecase := range eventErrorCases {
		trans := &BinlogTransaction{
			Statements: []Statement{
				{
					Category: ecase.Category,
					Sql:      []byte(ecase.Sql),
				},
			},
		}
		err := evs.transactionToEvent(trans)
		if ecase.want != err.Error() {
			t.Errorf("want %s, got %v", ecase.want, err)
		}
	}
}

func TestDMLEvent(t *testing.T) {
	trans := &BinlogTransaction{
		Statements: []Statement{
			{
				Category: BL_SET,
				Sql:      []byte("SET TIMESTAMP=1"),
			}, {
				Category: BL_SET,
				Sql:      []byte("SET INSERT_ID=10"),
			}, {
				Category: BL_DML,
				Sql:      []byte("query /* _stream vtocc_e (eid id name)  (null -1 'bmFtZQ==' ) (null 18446744073709551615 'bmFtZQ==' ); */"),
			}, {
				Category: BL_DML,
				Sql:      []byte("query"),
			},
		},
		GroupId: "20",
	}
	evs := &EventStreamer{
		sendEvent: func(event *StreamEvent) error {
			switch event.Category {
			case "DML":
				want := `&{DML vtocc_e [eid id name] [[10 -1 name] [11 18446744073709551615 name]]  1 }`
				got := fmt.Sprintf("%v", event)
				if want != got {
					t.Errorf("want %s, got %s", want, got)
				}
			case "ERR":
				want := `&{ERR  [] [] query 1 }`
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
	if evs.DmlCount != 2 {
		t.Errorf("want 1, got %d", evs.DmlCount)
	}
	if evs.TransactionCount != 1 {
		t.Errorf("want 1, got %d", evs.TransactionCount)
	}
}

func TestDDLEvent(t *testing.T) {
	trans := &BinlogTransaction{
		Statements: []Statement{
			{
				Category: BL_SET,
				Sql:      []byte("SET TIMESTAMP=1"),
			}, {
				Category: BL_DDL,
				Sql:      []byte("DDL"),
			},
		},
		GroupId: "20",
	}
	evs := &EventStreamer{
		sendEvent: func(event *StreamEvent) error {
			switch event.Category {
			case "DDL":
				want := `&{DDL  [] [] DDL 1 }`
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
	if evs.DdlCount != 1 {
		t.Errorf("want 1, got %d", evs.DdlCount)
	}
	if evs.TransactionCount != 1 {
		t.Errorf("want 1, got %d", evs.TransactionCount)
	}
}
