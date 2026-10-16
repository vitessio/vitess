/*
Copyright 2019 The Vitess Authors.

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

package mysql

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
)

// Override the default here to test with different values.
var testReadConnBufferSize = connBufferSize

const benchmarkQueryPrefix = "benchmark "

type mkListenerCfg func(AuthServer, Handler) ListenerConfig

func mkDefaultListenerCfg(authServer AuthServer, handler Handler) ListenerConfig {
	return ListenerConfig{
		Protocol:           "tcp",
		Address:            "127.0.0.1:",
		AuthServer:         authServer,
		Handler:            handler,
		ConnReadBufferSize: testReadConnBufferSize,
	}
}

func mkReadBufferPoolingCfg(authServer AuthServer, handler Handler) ListenerConfig {
	cfg := mkDefaultListenerCfg(authServer, handler)
	cfg.ConnBufferPooling = true
	return cfg
}

func benchmarkQuery(b *testing.B, threads int, query string, mkCfg mkListenerCfg) {
	th := &testHandler{}

	authServer := NewAuthServerNone()

	lCfg := mkCfg(authServer, th)

	l, err := NewListenerWithConfig(lCfg)
	if err != nil {
		b.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()

	go func() {
		l.Accept()
	}()

	b.SetParallelism(threads)
	if query != "" {
		b.SetBytes(int64(len(query)))
	}

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	ctx := context.Background()

	b.ResetTimer()

	// MaxPacketSize is too big for benchmarks, so choose something smaller
	maxPacketSize := connBufferSize * 4

	b.RunParallel(func(pb *testing.PB) {
		conn, err := Connect(ctx, params)
		if err != nil {
			b.Fatal(err)
		}
		defer func() {
			conn.writeComQuit()
			conn.Close()
		}()

		for pb.Next() {
			execQuery := query
			if execQuery == "" {
				// generate random query
				n := rand.IntN(maxPacketSize-len(benchmarkQueryPrefix)) + 1
				execQuery = benchmarkQueryPrefix + strings.Repeat("x", n)
			}
			if _, err := conn.ExecuteFetch(execQuery, 1000, true); err != nil {
				b.Fatalf("ExecuteFetch failed: %v", err)
			}
		}
	})
}

// This file contains various long-running tests for mysql.

// BenchmarkParallelShortQueries creates N simultaneous connections, then
// executes M queries on them, then closes them.
// It is meant as a somewhat real load test.
func BenchmarkParallelShortQueries(b *testing.B) {
	benchmarkQuery(b, 10, benchmarkQueryPrefix+"select rows", mkDefaultListenerCfg)
}

func BenchmarkParallelMediumQueries(b *testing.B) {
	benchmarkQuery(
		b,
		10,
		benchmarkQueryPrefix+"select"+strings.Repeat("x", connBufferSize),
		mkDefaultListenerCfg,
	)
}

func BenchmarkParallelRandomQueries(b *testing.B) {
	benchmarkQuery(b, 10, "", mkDefaultListenerCfg)
}

func BenchmarkParallelShortQueriesWithReadBufferPooling(b *testing.B) {
	benchmarkQuery(b, 10, benchmarkQueryPrefix+"select rows", mkReadBufferPoolingCfg)
}

func BenchmarkParallelMediumQueriesWithReadBufferPooling(b *testing.B) {
	benchmarkQuery(
		b,
		10,
		benchmarkQueryPrefix+"select"+strings.Repeat("x", connBufferSize),
		mkReadBufferPoolingCfg,
	)
}

func BenchmarkParallelRandomQueriesWithReadBufferPooling(b *testing.B) {
	benchmarkQuery(b, 10, "", mkReadBufferPoolingCfg)
}

// BenchmarkParseRow compares the per-row copy against the copy per value it replaces.
// The saving scales with column count, so the sweep goes wide enough to show it: a
// 2-column row understates it by roughly 3x. The NULL-heavy and zero-length shapes are
// here because they take different branches through the copy.
func BenchmarkParseRow(b *testing.B) {
	shapes := []struct {
		name   string
		values [][]byte
	}{
		{"1col", benchRowValues(1, benchValueNormal)},
		{"2col", benchRowValues(2, benchValueNormal)},
		{"5col", benchRowValues(5, benchValueNormal)},
		{"20col", benchRowValues(20, benchValueNormal)},
		{"50col", benchRowValues(50, benchValueNormal)},
		{"20col-narrow", benchRowValues(20, benchValueNarrow)},
		{"20col-null-heavy", benchRowValues(20, benchValueNull)},
		{"20col-empty-heavy", benchRowValues(20, benchValueEmpty)},
	}

	var sink []sqltypes.Value
	c := &Conn{}

	for _, shape := range shapes {
		data := rowPacket(b, shape.values)
		fields := varcharFields(len(shape.values))

		b.Run(shape.name+"/per-value-copy", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sink, _ = c.parseRow(data, fields, readLenEncStringAsBytesCopy, nil)
			}
		})

		b.Run(shape.name+"/per-row-copy", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sink, _ = c.parseRowCopy(data, fields)
			}
		})
	}

	_ = sink
}

type benchValueKind int

const (
	// benchValueNormal is a 7-8 byte value, which happens to land in the same size
	// class either way, so these shapes isolate the allocation count.
	benchValueNormal benchValueKind = iota
	// benchValueNarrow is a 1-2 byte value, where per-value size-class rounding is
	// most of the waste _(a 1-byte `pk` occupies an 8-byte class)_ and the per-row
	// copy wins on bytes as well as allocations.
	benchValueNarrow
	benchValueNull
	benchValueEmpty
)

// lobstersRows are real row shapes taken from `go/vt/sqlparser/testdata/lobsters.sql.gz`,
// a 44k-statement trace of the Lobsters Rails app. Each entry is the per-column wire
// length of that table's median INSERT, converted to how MySQL's text protocol would
// send it back _(numbers as decimal text, booleans as one byte)_, so the shapes are
// what a `SELECT *` on that table actually parses rather than a round number someone
// picked. Ordered by how often the trace hits each table
//
// Two things these shapes capture that a uniform sweep does not: real widths cluster
// at 5-9 columns, not 20-50, and the value lengths inside one row are wildly uneven —
// `comments` carries two ~275-byte bodies next to six values under 20 bytes
//
// Widths are a floor. INSERTs omit columns with defaults, so a true `SELECT *` is at
// least this wide and the shapes understate the saving if anything
var lobstersRows = []struct {
	name string
	lens []int
}{
	{"select-1", []int{1}},                                // SELECT 1 AS one, 2121 hits
	{"keystores", []int{25, 1}},                           // 2379 hits
	{"votes", []int{2, 3, 4, 1, 19}},                      // 1433 hits
	{"stories", []int{19, 2, 30, 21, 6, 14, 3}},           // 5017 hits, the hottest
	{"users", []int{17, 25, 60, 19, 60, 60, 10, 227}},     // 4319 hits
	{"comments", []int{19, 19, 6, 3, 2, 4, 272, 18, 280}}, // 2290 hits
}

// BenchmarkParseRowLobsters runs the same comparison as BenchmarkParseRow over real row
// shapes rather than a synthetic width sweep, so the numbers can be read as what the
// change is worth on a workload somebody actually ran.
func BenchmarkParseRowLobsters(b *testing.B) {
	var sink []sqltypes.Value
	c := &Conn{}

	for _, shape := range lobstersRows {
		values := make([][]byte, len(shape.lens))
		for i, n := range shape.lens {
			values[i] = []byte(strings.Repeat("x", n))
		}
		data := rowPacket(b, values)
		fields := varcharFields(len(values))

		b.Run(shape.name+"/per-value-copy", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sink, _ = c.parseRow(data, fields, readLenEncStringAsBytesCopy, nil)
			}
		})

		b.Run(shape.name+"/per-row-copy", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sink, _ = c.parseRowCopy(data, fields)
			}
		})
	}

	_ = sink
}

// benchRowValues builds a row of n columns. The NULL and empty kinds make every third
// column a NULL or zero-length value respectively, since those take different branches
// through the copy.
func benchRowValues(n int, kind benchValueKind) [][]byte {
	values := make([][]byte, n)
	for i := range values {
		switch {
		case kind == benchValueNull && i%3 == 0:
			values[i] = nil
		case kind == benchValueEmpty && i%3 == 0:
			values[i] = []byte{}
		case kind == benchValueNarrow:
			values[i] = []byte(strconv.Itoa(i))
		default:
			values[i] = []byte(fmt.Sprintf("value-%d", i))
		}
	}
	return values
}
