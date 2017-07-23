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

import (
	"net"
	"sync"
	"testing"

	"golang.org/x/net/context"
)

// This file contains various long-running tests for mysql.

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
	params := &ConnParams{
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
