package mysql

import (
	"flag"
	"math/rand"
	"net"
	"strings"
	"testing"

	"context"
)

var testReadConnBufferSize = connBufferSize

func init() {
	flag.IntVar(&testReadConnBufferSize, "test.read_conn_buffer_size", connBufferSize, "buffer size for reads from connections in tests")
}

const benchmarkQueryPrefix = "benchmark "

func benchmarkQuery(b *testing.B, threads int, query string) {
	th := &testHandler{}

	authServer := NewAuthServerNone()

	lCfg := ListenerConfig{
		Protocol:           "tcp",
		Address:            "127.0.0.1:",
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
				n := rand.Intn(maxPacketSize-len(benchmarkQueryPrefix)) + 1
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
	benchmarkQuery(b, 10, benchmarkQueryPrefix+"select rows")
}

func BenchmarkParallelMediumQueries(b *testing.B) {
	benchmarkQuery(b, 10, benchmarkQueryPrefix+"select"+strings.Repeat("x", connBufferSize))
}

func BenchmarkParallelRandomQueries(b *testing.B) {
	benchmarkQuery(b, 10, "")
}
