// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestRowsToProto3(t *testing.T) {
	rows := [][]sqltypes.Value{{
		sqltypes.MakeString([]byte("aa")),
		sqltypes.NULL,
		sqltypes.MakeString([]byte("12")),
	}, {
		sqltypes.MakeString([]byte("bb")),
		sqltypes.NULL,
		sqltypes.NULL,
	}}
	p3 := RowsToProto3(rows)
	want := []*querypb.Row{
		&querypb.Row{
			Lengths: []int64{2, -1, 2},
			Values:  []byte("aa12"),
		},
		&querypb.Row{
			Lengths: []int64{2, -1, -1},
			Values:  []byte("bb"),
		},
	}
	if !reflect.DeepEqual(p3, want) {
		t.Errorf("P3: %v, want %v", p3, want)
	}

	reverse := Proto3ToRows(p3)
	if !reflect.DeepEqual(reverse, rows) {
		t.Errorf("reverse: \n%#v, want \n%#v", reverse, rows)
	}
}
