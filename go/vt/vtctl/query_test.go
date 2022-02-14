package vtctl

import (
	"bytes"
	"testing"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
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
