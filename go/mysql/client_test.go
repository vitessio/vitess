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
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
)

// assertSQLError makes sure we get the right error.
func assertSQLError(t *testing.T, err error, code int, sqlState string, subtext string, query string) {
	t.Helper()

	if err == nil {
		t.Fatalf("was expecting SQLError %v / %v / %v but got no error.", code, sqlState, subtext)
	}
	serr, ok := err.(*SQLError)
	if !ok {
		t.Fatalf("was expecting SQLError %v / %v / %v but got: %v", code, sqlState, subtext, err)
	}
	if serr.Num != code {
		t.Fatalf("was expecting SQLError %v / %v / %v but got code %v", code, sqlState, subtext, serr.Num)
	}
	if serr.State != sqlState {
		t.Fatalf("was expecting SQLError %v / %v / %v but got state %v", code, sqlState, subtext, serr.State)
	}
	if subtext != "" && !strings.Contains(serr.Message, subtext) {
		t.Fatalf("was expecting SQLError %v / %v / %v but got message %v", code, sqlState, subtext, serr.Message)
	}
	if serr.Query != query {
		t.Fatalf("was expecting SQLError %v / %v / %v with Query '%v' but got query '%v'", code, sqlState, subtext, query, serr.Query)
	}
}

// TestConnectTimeout runs connection failure scenarios against a
// server that's not listening or has trouble.  This test is not meant
// to use a valid server. So we do not test bad handshakes here.
func TestConnectTimeout(t *testing.T) {
	// Create a socket, but it's not accepting. So all Dial
	// attempts will timeout.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}
	host, port := GetHostPort(t, listener.Addr())
	params := &ConnParams{
		Host: host,
		Port: port,
	}
	defer listener.Close()

	// Test that canceling the context really interrupts the Connect.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, err := Connect(ctx, params)
		if err != context.Canceled {
			t.Errorf("Was expecting context.Canceled but got: %v", err)
		}
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Tests a connection timeout works.
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = Connect(ctx, params)
	cancel()
	if err != context.DeadlineExceeded {
		t.Errorf("Was expecting context.DeadlineExceeded but got: %v", err)
	}

	// Now the server will listen, but close all connections on accept.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Listener was closed.
				return
			}
			conn.Close()
		}
	}()
	ctx = context.Background()
	_, err = Connect(ctx, params)
	assertSQLError(t, err, CRServerLost, SSUnknownSQLState, "initial packet read failed", "")

	// Now close the listener. Connect should fail right away,
	// check the error.
	listener.Close()
	wg.Wait()
	_, err = Connect(ctx, params)
	assertSQLError(t, err, CRConnHostError, SSUnknownSQLState, "connection refused", "")

	// Tests a connection where Dial to a unix socket fails
	// properly returns the right error. To simulate exactly the
	// right failure, try to dial a Unix socket that's just a temp file.
	fd, err := ioutil.TempFile("", "mysql")
	if err != nil {
		t.Fatalf("cannot create TemFile: %v", err)
	}
	name := fd.Name()
	fd.Close()
	params.UnixSocket = name
	ctx = context.Background()
	_, err = Connect(ctx, params)
	os.Remove(name)
	assertSQLError(t, err, CRConnectionError, SSUnknownSQLState, "connection refused", "")
}
