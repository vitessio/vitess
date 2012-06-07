// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
