/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package umgmt

import (
	"code.google.com/p/vitess/go/relog"
	"expvar"
	"net"
	"net/http"
	"sync"
	"syscall"
)

var connectionCount = expvar.NewInt("connection-count")
var connectionAccepted = expvar.NewInt("connection-accepted")

type connCountConn struct {
	net.Conn
	listener *connCountListener
	closed   bool
}

func (c *connCountConn) Close() (err error) {
	// in case there is a race closing a client
	if c.closed {
		return nil
	}
	err = c.Conn.Close()
	c.closed = true
	connectionCount.Add(-1)
	c.listener.Lock()
	delete(c.listener.connMap, c)
	c.listener.Unlock()
	c.listener = nil
	return
}

// wrap up listener and server-side connection so we can count them
type connCountListener struct {
	sync.Mutex
	net.Listener
	connMap map[*connCountConn]bool
}

func newHttpListener(l net.Listener) *connCountListener {
	return &connCountListener{Listener: l,
		connMap: make(map[*connCountConn]bool, 8192)}
}

func (l *connCountListener) CloseClients() {
	l.Lock()
	conns := make([]*connCountConn, 0, len(l.connMap))
	for conn := range l.connMap {
		conns = append(conns, conn)
	}
	l.Unlock()

	for _, conn := range conns {
		conn.Close()
	}
}

func (l *connCountListener) Accept() (c net.Conn, err error) {
	c, err = l.Listener.Accept()
	connectionAccepted.Add(1)
	if err == nil {
		connectionCount.Add(1)
	}
	if c != nil {
		ccc := &connCountConn{c, l, false}
		l.Lock()
		l.connMap[ccc] = true
		l.Unlock()
		c = ccc
	}
	return
}

// this is a callback to bind and startup an http server.
// usually it is called like:
//   umgmt.AddStartupCallback(func () { umgmt.StartHttpServer(addr) })
func StartHttpServer(addr string) {
	httpListener, httpErr := net.Listen("tcp", addr)

	go func() {
		if httpErr == nil {
			httpListener = newHttpListener(httpListener)
			AddListener(httpListener)
			httpErr = http.Serve(httpListener, nil)
			httpListener.Close()
		}
		if httpErr != nil {
			switch e := httpErr.(type) {
			case *net.OpError:
				switch e.Err {
				case syscall.EADDRINUSE:
					relog.Fatal("StartHttpServer failed: %v", e)
				}
			case error:
				// NOTE(msolomon) even though these are Errno objects, the constants
				// are typed as os.Error.
				switch e {
				// FIXME(msolomon) this needs to be migrated into the system library
				// because this needs to be properly handled in the accept loop.
				case syscall.EMFILE, syscall.ENFILE:
					relog.Error("non-fatal error serving HTTP: %s", e.Error())
				case syscall.EINVAL:
					// nothing - listener was probably closed
				default:
					relog.Error("http.ListenAndServe: " + httpErr.Error())
				}
			default:
				relog.Error("http.ListenAndServe: " + httpErr.Error())
			}
		}
	}()
}
