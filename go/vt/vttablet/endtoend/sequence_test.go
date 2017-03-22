// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/vttablet/endtoend/framework"
)

func TestSequence(t *testing.T) {
	want := sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("0")),
		}},
	}
	for wantval := int64(1); wantval < 10; wantval += 2 {
		want.Rows[0][0] = sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, wantval, 10))
		qr, err := framework.NewClient().Execute("select next 2 values from vitess_seq", nil)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(*qr, want) {
			t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
		}
	}
	// Verify that the table got updated according to chunk size.
	want = sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("13")),
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("3")),
		}},
	}
	qr, err := framework.NewClient().Execute("select next_id, cache from vitess_seq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	qr.Fields = nil
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}
