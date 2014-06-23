// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctabletconn

import (
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
)

func TestAppendResult(t *testing.T) {
	qr := new(mproto.QueryResult)
	innerqr1 := &mproto.QueryResult{
		Fields: []mproto.Field{},
		Rows:   [][]sqltypes.Value{},
	}
	innerqr2 := &mproto.QueryResult{
		Fields: []mproto.Field{
			{Name: "foo", Type: 1},
		},
		RowsAffected: 1,
		InsertId:     1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("abcd"))},
		},
	}
	// test one empty result
	AppendResultBson(qr, innerqr1)
	AppendResultBson(qr, innerqr2)
	if len(qr.Fields) != 1 {
		t.Errorf("want 1, got %v", len(qr.Fields))
	}
	if qr.RowsAffected != 1 {
		t.Errorf("want 1, got %v", qr.RowsAffected)
	}
	if qr.InsertId != 1 {
		t.Errorf("want 1, got %v", qr.InsertId)
	}
	if len(qr.Rows) != 1 {
		t.Errorf("want 1, got %v", len(qr.Rows))
	}
	// test two valid results
	qr = new(mproto.QueryResult)
	AppendResultBson(qr, innerqr2)
	AppendResultBson(qr, innerqr2)
	if len(qr.Fields) != 1 {
		t.Errorf("want 1, got %v", len(qr.Fields))
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
	if qr.InsertId != 1 {
		t.Errorf("want 1, got %v", qr.InsertId)
	}
	if len(qr.Rows) != 2 {
		t.Errorf("want 2, got %v", len(qr.Rows))
	}
}
