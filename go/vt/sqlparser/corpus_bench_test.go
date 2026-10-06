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

package sqlparser

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// boundQuery is one query after the vtgate half of the path: the normalized
// text with its placeholders, and the bind variables vtgate would send.
type boundQuery struct {
	pq       *ParsedQuery
	bindVars map[string]*querypb.BindVariable
}

// bindQueries runs each query through Parse2 and Normalize exactly as
// BenchmarkNormalizeVTGate does, and returns what vttablet would receive.
func bindQueries(b *testing.B, parser *Parser, queries []string) []boundQuery {
	b.Helper()
	bound := make([]boundQuery, 0, len(queries))
	for _, sql := range queries {
		stmt, reservedVars, err := parser.Parse2(sql)
		require.NoError(b, err)
		bindVars := make(map[string]*querypb.BindVariable)
		if CanNormalize(stmt) || MustRewriteAST(stmt, false) {
			result, err := Normalize(stmt, NewReservedVars("vtg", reservedVars), bindVars, true, "main_keyspace", SQLSelectLimitUnset, "", nil, nil, nil)
			require.NoError(b, err)
			stmt = result.AST
		}
		bound = append(bound, boundQuery{pq: NewParsedQuery(stmt), bindVars: bindVars})
	}
	return bound
}

// BenchmarkGenerateQueryCorpus measures bind substitution over the lobsters log,
// with one op per pass over the whole corpus.
func BenchmarkGenerateQueryCorpus(b *testing.B) {
	parser := NewTestParser()
	bound := bindQueries(b, parser, loadQueries(b, "lobsters.sql.gz"))
	var total int
	for _, q := range bound {
		out, err := q.pq.GenerateQuery(q.bindVars, nil)
		require.NoError(b, err)
		total += len(out)
	}
	b.ReportAllocs()
	b.SetBytes(int64(total))
	for b.Loop() {
		for _, q := range bound {
			if _, err := q.pq.GenerateQuery(q.bindVars, nil); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// tailQueries covers larger query shapes missing from the lobsters log.
func tailQueries() []struct{ name, sql string } {
	text := strings.Repeat("The quick brown fox jumps over the lazy dog; it's quick, isn't it. ", 16)[:1024]

	var ins strings.Builder
	ins.WriteString("insert into posts(id, title, body) values ")
	for i := range 100 {
		if i > 0 {
			ins.WriteString(", ")
		}
		fmt.Fprintf(&ins, "(%d, %s, %s)", i, sqltypes.EncodeStringSQL(text[:64]), sqltypes.EncodeStringSQL(text))
	}

	var in strings.Builder
	in.WriteString("select id, title from posts where id in (")
	for i := range 500 {
		if i > 0 {
			in.WriteString(", ")
		}
		fmt.Fprintf(&in, "%d", 1000003*i)
	}
	in.WriteString(")")

	type entry struct {
		Name string `json:"name"`
		Note string `json:"note"`
		N    int    `json:"n"`
	}
	var entries []entry
	for i := 0; ; i++ {
		entries = append(entries, entry{Name: "O'Brien", Note: "line one\nline \"two\"", N: i})
		if len(entries)%64 == 0 {
			if doc, _ := json.Marshal(entries); len(doc) >= 64*1024 {
				break
			}
		}
	}
	doc, _ := json.Marshal(entries)

	return []struct{ name, sql string }{
		{"insert-100x1KB", ins.String()},
		{"in-500", in.String()},
		{"json-64KB", "insert into docs(id, doc) values (1, " + sqltypes.EncodeStringSQL(string(doc)) + ")"},
	}
}

// BenchmarkGenerateQueryTail is vttablet's bind substitution for each tail
// shape, after vtgate has normalized it.
func BenchmarkGenerateQueryTail(b *testing.B) {
	parser := NewTestParser()
	for _, tc := range tailQueries() {
		q := bindQueries(b, parser, []string{tc.sql})[0]
		out, err := q.pq.GenerateQuery(q.bindVars, nil)
		require.NoError(b, err)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(out)))
			for b.Loop() {
				if _, err := q.pq.GenerateQuery(q.bindVars, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
