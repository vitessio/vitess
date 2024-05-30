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

package netutil

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func createSocketPair(t *testing.T) (net.Listener, net.Conn, net.Conn) {
	// Create a listener.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	addr := listener.Addr().String()

	// Dial a client, Accept a server.
	wg := sync.WaitGroup{}

	var clientConn net.Conn
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		clientConn, err = net.Dial("tcp", addr)
		assert.NoError(t, err)
	}()

	var serverConn net.Conn
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		serverConn, err = listener.Accept()
		assert.NoError(t, err)
	}()

	wg.Wait()

	return listener, serverConn, clientConn
}

func TestReadTimeout(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	cConnWithTimeout := NewConnWithTimeouts(cConn, 1*time.Millisecond, 1*time.Millisecond)

	c := make(chan error, 1)
	go func() {
		_, err := cConnWithTimeout.Read(make([]byte, 10))
		c <- err
	}()

	select {
	case err := <-c:
		assert.ErrorContains(t, err, "i/o timeout", "Expected error timeout")
	case <-time.After(10 * time.Second):
		t.Errorf("Timeout did not happen")
	}
}

func TestWriteTimeout(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	sConnWithTimeout := NewConnWithTimeouts(sConn, 1*time.Millisecond, 1*time.Millisecond)

	c := make(chan error, 1)
	go func() {
		// The timeout will trigger when the buffer is full, so to test this we need to write multiple times.
		for {
			_, err := sConnWithTimeout.Write([]byte("payload"))
			if err != nil {
				c <- err
				return
			}
		}
	}()

	select {
	case err := <-c:
		assert.ErrorContains(t, err, "i/o timeout", "Expected error timeout")
	case <-time.After(10 * time.Second):
		t.Errorf("Timeout did not happen")
	}
}

func TestNoTimeouts(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	cConnWithTimeout := NewConnWithTimeouts(cConn, 0, 24*time.Hour)

	c := make(chan error, 1)
	go func() {
		_, err := cConnWithTimeout.Read(make([]byte, 10))
		c <- err
	}()

	select {
	case <-c:
		t.Fatalf("Connection timeout, without a timeout")
	case <-time.After(100 * time.Millisecond):
		// NOOP
	}

	c2 := make(chan error, 1)
	sConnWithTimeout := NewConnWithTimeouts(sConn, 24*time.Hour, 0)
	go func() {
		// This should not fail as there is not timeout on write.
		for {
			_, err := sConnWithTimeout.Write([]byte("payload"))
			if err != nil {
				c2 <- err
				return
			}
		}
	}()
	select {
	case <-c2:
		t.Fatalf("Connection timeout, without a timeout")
	case <-time.After(100 * time.Millisecond):
		// NOOP
	}
}

func TestSetDeadline(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	cConnWithTimeout := NewConnWithTimeouts(cConn, 0, 24*time.Hour)

	assert.Panics(t, func() { _ = cConnWithTimeout.SetDeadline(time.Now()) })
}

func TestSetReadDeadline(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	cConnWithTimeout := NewConnWithTimeouts(cConn, 0, 24*time.Hour)

	assert.Panics(t, func() { _ = cConnWithTimeout.SetReadDeadline(time.Now()) })
}

func TestSetWriteDeadline(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	cConnWithTimeout := NewConnWithTimeouts(cConn, 0, 24*time.Hour)

	assert.Panics(t, func() { _ = cConnWithTimeout.SetWriteDeadline(time.Now()) })
}
