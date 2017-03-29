package mysqlconn

import (
	"fmt"
	"net"
	"sync"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/vttest"
)

// This file contains various long-running tests for mysqlconn.

// BenchmarkWithRealDatabase runs a real MySQL database, and runs all kinds
// of benchmarks on it. To minimize overhead, we only run one database, and
// run all the benchmarks on it.
func BenchmarkWithRealDatabase(b *testing.B) {
	// Common setup code.
	hdl, err := vttest.LaunchVitess(
		vttest.MySQLOnly("vttest"),
		vttest.Schema("create table a(id int, name varchar(128), primary key(id))"),
		vttest.NoStderr())
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = hdl.TearDown()
		if err != nil {
			b.Error(err)
		}
	}()
	params, err := hdl.MySQLConnParams()
	if err != nil {
		b.Error(err)
	}

	b.Run("Inserts", func(b *testing.B) {
		benchmarkInserts(b, &params)
	})
	b.Run("ParallelReads", func(b *testing.B) {
		benchmarkParallelReads(b, &params, 10)
	})
	b.Run("OldParallelReads", func(b *testing.B) {
		benchmarkOldParallelReads(b, params, 10)
	})
}

func benchmarkInserts(b *testing.B, params *sqldb.ConnParams) {
	// Connect.
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	// Delete what we may already have in the database.
	if _, err := conn.ExecuteFetch("delete from a", 0, false); err != nil {
		b.Fatalf("delete failed: %v", err)
	}

	// Now reset timer.
	b.ResetTimer()

	// Do the insert.
	for i := 0; i < b.N; i++ {
		_, err := conn.ExecuteFetch(fmt.Sprintf("insert into a(id, name) values(%v, 'nice name %v')", i, i), 0, false)
		if err != nil {
			b.Fatalf("ExecuteFetch(%v) failed: %v", i, err)
		}
	}
}

func benchmarkParallelReads(b *testing.B, params *sqldb.ConnParams, parallelCount int) {
	ctx := context.Background()
	wg := sync.WaitGroup{}
	for i := 0; i < parallelCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			conn, err := Connect(ctx, params)
			if err != nil {
				b.Fatal(err)
			}

			for j := 0; j < b.N; j++ {
				if _, err := conn.ExecuteFetch("select * from a", 10000, true); err != nil {
					b.Fatalf("ExecuteFetch(%v, %v) failed: %v", i, j, err)
				}
			}
			conn.Close()
		}(i)
	}
	wg.Wait()
}

func benchmarkOldParallelReads(b *testing.B, params sqldb.ConnParams, parallelCount int) {
	wg := sync.WaitGroup{}
	for i := 0; i < parallelCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			conn, err := mysql.Connect(params)
			if err != nil {
				b.Fatal(err)
			}

			for j := 0; j < b.N; j++ {
				if _, err := conn.ExecuteFetch("select * from a", 10000, true); err != nil {
					b.Fatalf("ExecuteFetch(%v, %v) failed: %v", i, j, err)
				}
			}
			conn.Close()
		}(i)
	}
	wg.Wait()
}

// BenchmarkParallelShortQueries creates N simultaneous connections, then
// executes M queries on them, then closes them.
// It is meant as a somewhat real load test.
func BenchmarkParallelShortQueries(b *testing.B) {
	th := &testHandler{}

	authServer := &AuthServerNone{}

	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		b.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()

	go func() {
		l.Accept()
	}()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port
	params := &sqldb.ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	ctx := context.Background()
	threadCount := 10

	wg := sync.WaitGroup{}
	conns := make([]*Conn, threadCount)
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var err error
			conns[i], err = Connect(ctx, params)
			if err != nil {
				b.Errorf("cannot connect: %v", err)
				return
			}
		}(i)
	}
	wg.Wait()

	b.ResetTimer()
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer func() {
				wg.Done()
				conns[i].writeComQuit()
				conns[i].Close()
			}()
			for j := 0; j < b.N; j++ {
				_, err = conns[i].ExecuteFetch("select rows", 1000, true)
				if err != nil {
					b.Errorf("ExecuteFetch failed: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

}
