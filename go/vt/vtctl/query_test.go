// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctl

import (
	"bytes"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestPrintQueryResult(t *testing.T) {
	input := &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "a"}, {Name: "b"}},
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("1")), sqltypes.MakeString([]byte("2"))},
			{sqltypes.MakeString([]byte("3")), sqltypes.MakeString([]byte("4"))},
		},
	}
	// Use a simple example so we're not sensitive to alignment settings, etc.
	want := `
+---+---+
| a | b |
+---+---+
| 1 | 2 |
| 3 | 4 |
+---+---+
`[1:]

	buf := &bytes.Buffer{}
	printQueryResult(buf, input)
	if got := buf.String(); got != want {
		t.Errorf("printQueryResult() = %q, want %q", got, want)
	}
}
