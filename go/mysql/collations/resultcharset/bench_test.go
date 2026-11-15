/*
Copyright 2026 The Vitess Authors.

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

package resultcharset

import (
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// benchResult returns a result of ten columns and a hundred rows: an INT and
// nine VARCHAR(20) with the collation text, each with the value.
func benchResult(text collations.ID, value []byte) *sqltypes.Result {
	qr := &sqltypes.Result{Fields: []*querypb.Field{{Name: "id", Type: sqltypes.Int64, Charset: uint32(collations.CollationBinaryID), ColumnLength: 20}}}
	for range 9 {
		qr.Fields = append(qr.Fields, &querypb.Field{Name: "c", Type: sqltypes.VarChar, Charset: uint32(text), ColumnLength: 20})
	}
	for range 100 {
		row := sqltypes.Row{sqltypes.NewInt64(1)}
		for range 9 {
			row = append(row, sqltypes.MakeTrusted(sqltypes.VarChar, value))
		}
		qr.Rows = append(qr.Rows, row)
	}
	return qr
}

// The benchmarks convert a result of ten columns and a hundred rows the way a
// vtgate with --convert-result-charset does for every result of a tablet
// (ToConnection) and for every result it returns to a client (ToClient).
func BenchmarkConversion(b *testing.B) {
	const (
		utf8mb4 = collations.ID(255)
		latin1  = collations.ID(8)
	)
	cases := []struct {
		name    string
		qr      *sqltypes.Result
		convert func(qr *sqltypes.Result) *sqltypes.Result
	}{
		{"ToConnection/utf8mb4 columns", benchResult(utf8mb4, []byte("Müller")), func(qr *sqltypes.Result) *sqltypes.Result {
			return ToConnection(qr.Fields, utf8mb4, false).Result(qr)
		}},
		{"ToConnection/latin1 columns, ASCII", benchResult(latin1, []byte("Muller")), func(qr *sqltypes.Result) *sqltypes.Result {
			return ToConnection(qr.Fields, utf8mb4, false).Result(qr)
		}},
		{"ToConnection/latin1 columns", benchResult(latin1, []byte("M\xFCller")), func(qr *sqltypes.Result) *sqltypes.Result {
			return ToConnection(qr.Fields, utf8mb4, false).Result(qr)
		}},
		{"ToClient/utf8mb4 client", benchResult(utf8mb4, []byte("Müller")), func(qr *sqltypes.Result) *sqltypes.Result {
			return ToClient(qr.Fields, utf8mb4, utf8mb4, false).Result(qr)
		}},
		{"ToClient/latin1 results", benchResult(utf8mb4, []byte("Müller")), func(qr *sqltypes.Result) *sqltypes.Result {
			return ToClient(qr.Fields, utf8mb4, latin1, false).Result(qr)
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tc.convert(tc.qr)
			}
		})
	}
}
