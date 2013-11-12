// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
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
		sendEvent: func(event interface{}) error {
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
