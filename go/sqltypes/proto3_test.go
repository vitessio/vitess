// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/proto/query"
)

// TODO(sougou): need more tests here.
func TestRowsToProto3(t *testing.T) {
	rows := [][]Value{{
		MakeString([]byte("aa")),
		NULL,
		MakeString([]byte("12")),
	}, {
		MakeString([]byte("bb")),
		NULL,
		NULL,
	}}
	p3 := RowsToProto3(rows)
	want := []*query.Row{
		&query.Row{
			Lengths: []int64{2, -1, 2},
			Values:  []byte("aa12"),
		},
		&query.Row{
			Lengths: []int64{2, -1, -1},
			Values:  []byte("bb"),
		},
	}
	if !reflect.DeepEqual(p3, want) {
		t.Errorf("P3: %v, want %v", p3, want)
	}

	f := []*query.Field{{
		Type: VarBinary,
	}, {
		Type: VarBinary,
	}, {
		Type: VarBinary,
	}}

	reverse := Proto3ToRows(f, p3)
	if !reflect.DeepEqual(reverse, rows) {
		t.Errorf("reverse: \n%#v, want \n%#v", reverse, rows)
	}
}
