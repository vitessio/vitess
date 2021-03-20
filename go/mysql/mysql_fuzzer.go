/*
Copyright 2021 The Vitess Authors.

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
// +build gofuzz

package mysql

import (
	"fmt"
	"net"
	"sync"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func createFuzzingSocketPair() (net.Listener, *Conn, *Conn) {
	// Create a listener.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		fmt.Println("We got an error early on")
		return nil, nil, nil
	}
	addr := listener.Addr().String()
	listener.(*net.TCPListener).SetDeadline(time.Now().Add(10 * time.Second))

	// Dial a client, Accept a server.
	wg := sync.WaitGroup{}

	var clientConn net.Conn
	var clientErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientConn, clientErr = net.DialTimeout("tcp", addr, 10*time.Second)
	}()

	var serverConn net.Conn
	var serverErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverConn, serverErr = listener.Accept()
	}()

	wg.Wait()

	if clientErr != nil {
		return nil, nil, nil
	}
	if serverErr != nil {
		return nil, nil, nil
	}

	// Create a Conn on both sides.
	cConn := newConn(clientConn)
	sConn := newConn(serverConn)

	return listener, sConn, cConn
}

type fuzztestRun struct{}

func (t fuzztestRun) NewConnection(c *Conn) {
	panic("implement me")
}

func (t fuzztestRun) ConnectionClosed(c *Conn) {
	panic("implement me")
}

func (t fuzztestRun) ComQuery(c *Conn, query string, callback func(*sqltypes.Result) error) error {
	return nil
}

func (t fuzztestRun) ComPrepare(c *Conn, query string, bindVars map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	panic("implement me")
}

func (t fuzztestRun) ComStmtExecute(c *Conn, prepare *PrepareData, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (t fuzztestRun) WarningCount(c *Conn) uint16 {
	return 0
}

func (t fuzztestRun) ComResetConnection(c *Conn) {
	panic("implement me")
}

var _ Handler = (*fuzztestRun)(nil)

type fuzztestConn struct {
	writeToPass []bool
	pos         int
	queryPacket []byte
}

func (t fuzztestConn) Read(b []byte) (n int, err error) {
	for j, i := range t.queryPacket {
		b[j] = i
	}
	return len(b), nil
}

func (t fuzztestConn) Write(b []byte) (n int, err error) {
	t.pos = t.pos + 1
	if t.writeToPass[t.pos] {
		return 0, nil
	}
	return 0, fmt.Errorf("error in writing to connection")
}

func (t fuzztestConn) Close() error {
	panic("implement me")
}

func (t fuzztestConn) LocalAddr() net.Addr {
	panic("implement me")
}

func (t fuzztestConn) RemoteAddr() net.Addr {
	return fuzzmockAddress{s: "a"}
}

func (t fuzztestConn) SetDeadline(t1 time.Time) error {
	panic("implement me")
}

func (t fuzztestConn) SetReadDeadline(t1 time.Time) error {
	panic("implement me")
}

func (t fuzztestConn) SetWriteDeadline(t1 time.Time) error {
	panic("implement me")
}

var _ net.Conn = (*fuzztestConn)(nil)

type fuzzmockAddress struct {
	s string
}

func (m fuzzmockAddress) Network() string {
	return m.s
}

func (m fuzzmockAddress) String() string {
	return m.s
}

var _ net.Addr = (*fuzzmockAddress)(nil)

// Fuzzers begin here:
func FuzzWritePacket(data []byte) int {
	if len(data) < 10 {
		return -1
	}
	listener, sConn, cConn := createFuzzingSocketPair()
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	err := cConn.writePacket(data)
	if err != nil {
		return 0
	}
	_, err = sConn.ReadPacket()
	if err != nil {
		return 0
	}
	return 1
}

func FuzzHandleNextCommand(data []byte) int {
	if len(data) < 10 {
		return -1
	}
	sConn := newConn(fuzztestConn{
		writeToPass: []bool{false},
		pos:         -1,
		queryPacket: data,
	})

	handler := &fuzztestRun{}
	_ = sConn.handleNextCommand(handler)
	return 1
}

func FuzzReadQueryResults(data []byte) int {
	listener, sConn, cConn := createFuzzingSocketPair()
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()
	err := cConn.WriteComQuery(string(data))
	if err != nil {
		return 0
	}
	handler := &fuzztestRun{}
	_ = sConn.handleNextCommand(handler)
	_, _, _, err = cConn.ReadQueryResult(100, true)
	if err != nil {
		return 0
	}
	return 1
}
