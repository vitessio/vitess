/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

/*
import (
	"flag"
	"math/rand"
	"net"
	"strings"
	"testing"

	"golang.org/x/net/context"
)

var testReadConnBufferSize = connBufferSize

func init() {
	flag.IntVar(&testReadConnBufferSize, "test.read_conn_buffer_size", connBufferSize, "buffer size for reads from connections in tests")
}

const BenchmarkQueryPrefix = "benchmark "

func benchmarkQuery(b *testing.B, threads int, query string) {
	th := &testHandler{}

	authServer := &AuthServerNone{}

	lCfg := ListenerConfig{
		Protocol:           "tcp",
		Address:            ":0",
		AuthServer:         authServer,
		Handler:            th,
		ConnReadBufferSize: testReadConnBufferSize,
	}
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
				n := rand.Intn(maxPacketSize-len(BenchmarkQueryPrefix)) + 1
				execQuery = BenchmarkQueryPrefix + strings.Repeat("x", n)

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
	benchmarkQuery(b, 10, BenchmarkQueryPrefix+"select rows")
}

func BenchmarkParallelMediumQueries(b *testing.B) {
	benchmarkQuery(b, 10, BenchmarkQueryPrefix+"select"+strings.Repeat("x", connBufferSize))
}

func BenchmarkParallelRandomQueries(b *testing.B) {
	benchmarkQuery(b, 10, "")
}
*/
