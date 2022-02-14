package sqlparser

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
)

func TestEncodable(t *testing.T) {
	tcases := []struct {
		in  Encodable
		out string
	}{{
		in: InsertValues{{
			sqltypes.NewInt64(1),
			sqltypes.NewVarBinary("foo('a')"),
		}, {
			sqltypes.NewInt64(2),
			sqltypes.NewVarBinary("bar(`b`)"),
		}},
		out: "(1, 'foo(\\'a\\')'), (2, 'bar(`b`)')",
	}, {
		// Single column.
		in: &TupleEqualityList{
			Columns: []ColIdent{NewColIdent("pk")},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt64(1)},
				{sqltypes.NewVarBinary("aa")},
			},
		},
		out: "pk in (1, 'aa')",
	}, {
		// Multiple columns.
		in: &TupleEqualityList{
			Columns: []ColIdent{NewColIdent("pk1"), NewColIdent("pk2")},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.NewInt64(1),
					sqltypes.NewVarBinary("aa"),
				},
				{
					sqltypes.NewInt64(2),
					sqltypes.NewVarBinary("bb"),
				},
			},
		},
		out: "(pk1 = 1 and pk2 = 'aa') or (pk1 = 2 and pk2 = 'bb')",
	}}
	for _, tcase := range tcases {
		buf := new(strings.Builder)
		tcase.in.EncodeSQL(buf)
		if out := buf.String(); out != tcase.out {
			t.Errorf("EncodeSQL(%v): %s, want %s", tcase.in, out, tcase.out)
		}
	}
}
