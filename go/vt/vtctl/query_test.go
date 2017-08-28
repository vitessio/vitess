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
			{sqltypes.NewVarBinary("1"), sqltypes.NewVarBinary("2")},
			{sqltypes.NewVarBinary("3"), sqltypes.NewVarBinary("4")},
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
