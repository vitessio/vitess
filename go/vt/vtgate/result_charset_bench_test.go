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

package vtgate

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// The callbacks escape, as they do when the handler passes them to the
// executor.
var (
	benchResultsCallback   func(*sqltypes.Result) error
	benchResponsesCallback func(sqltypes.QueryResponse, bool, bool) error
)

func benchFields(n int, nonASCII bool) []*querypb.Field {
	fields := make([]*querypb.Field, n)
	for i := range fields {
		name := fmt.Sprintf("col_%d", i)
		if nonASCII {
			name = fmt.Sprintf("colé_%d", i)
		}
		fields[i] = &querypb.Field{Name: name, OrgName: name, Table: "t", OrgTable: "t", Database: "ks", Type: sqltypes.Int64}
	}
	return fields
}

func benchHandler(b *testing.B, resultsCharset string) (*vtgateHandler, *mysql.Conn) {
	b.Helper()
	vh := &vtgateHandler{vtg: &VTGate{executor: &Executor{env: vtenv.NewTestEnv()}}}
	c := &mysql.Conn{ClientData: &vtgatepb.Session{Options: &querypb.ExecuteOptions{}, CharacterSetResults: resultsCharset}}
	return vh, c
}

// BenchmarkEncodingResults measures the wrapper that the MySQL protocol
// server puts around the callback of each command: an OLTP command sends one
// result, an OLAP command streams one result with fields, then row packets.
func BenchmarkEncodingResults(b *testing.B) {
	row := []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(3), sqltypes.NewInt64(4), sqltypes.NewInt64(5)}
	for _, cs := range []string{"", "latin1"} {
		name := cs
		if name == "" {
			name = "utf8mb4"
		}
		fieldsQR := &sqltypes.Result{Fields: benchFields(5, false), Rows: [][]sqltypes.Value{row}}
		rowsQR := &sqltypes.Result{Rows: [][]sqltypes.Value{row}}
		sink := func(*sqltypes.Result) error { return nil }
		b.Run(name+"/oltp", func(b *testing.B) {
			vh, c := benchHandler(b, cs)
			b.ReportAllocs()
			for b.Loop() {
				callback := vh.encodingResults(c, sink)
				benchResultsCallback = callback
				_ = callback(fieldsQR)
			}
		})
		b.Run(name+"/olap-100-packets", func(b *testing.B) {
			vh, c := benchHandler(b, cs)
			b.ReportAllocs()
			for b.Loop() {
				callback := vh.encodingResults(c, sink)
				benchResultsCallback = callback
				_ = callback(fieldsQR)
				for range 99 {
					_ = callback(rowsQR)
				}
			}
		})
		multiSink := func(sqltypes.QueryResponse, bool, bool) error { return nil }
		b.Run(name+"/multi-olap-100-packets", func(b *testing.B) {
			vh, c := benchHandler(b, cs)
			b.ReportAllocs()
			for b.Loop() {
				callback := vh.encodingResponses(c, multiSink)
				benchResponsesCallback = callback
				_ = callback(sqltypes.QueryResponse{QueryResult: fieldsQR}, false, true)
				for range 99 {
					_ = callback(sqltypes.QueryResponse{QueryResult: rowsQR}, false, false)
				}
			}
		})
	}
}

// BenchmarkEncodeResult measures converting the metadata of a result with
// fields to a latin1 character_set_results.
func BenchmarkEncodeResult(b *testing.B) {
	vh, c := benchHandler(b, "latin1")
	cs := resultCharset(vh.Env().CollationEnv(), vh.session(c))
	for _, n := range []int{5, 50} {
		for _, nonASCII := range []bool{false, true} {
			kind := "ascii"
			if nonASCII {
				kind = "nonascii"
			}
			qr := &sqltypes.Result{Fields: benchFields(n, nonASCII)}
			b.Run(fmt.Sprintf("fields=%d/%s", n, kind), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = encodeResult(cs, qr)
				}
			})
		}
	}
}
