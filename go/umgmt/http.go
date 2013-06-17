// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package umgmt

import (
	"crypto/tls"
	"crypto/x509"
	"expvar"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"code.google.com/p/vitess/go/relog"
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

func asyncListener(listener net.Listener) {
	httpListener := newHttpListener(listener)
	AddListener(httpListener)
	httpErr := http.Serve(httpListener, nil)
	httpListener.Close()
	if httpErr != nil {
		// This is net.errClosing, which is conveniently non-public.
		// Squelch this expected case.
		if !strings.Contains(httpErr.Error(), "use of closed network connection") {
			relog.Error("StartHttpServer error: %v", httpErr)
		}
	}
}

// StartHttpServer binds and starts an http server.
// usually it is called like:
//   umgmt.AddStartupCallback(func () { umgmt.StartHttpServer(addr) })
func StartHttpServer(addr string) {
	httpListener, httpErr := net.Listen("tcp", addr)
	if httpErr != nil {
		relog.Fatal("StartHttpServer failed: %v", httpErr)
	}
	go asyncListener(httpListener)
}

// StartHttpsServer binds and starts an https server.
func StartHttpsServer(addr string, certFile, keyFile, caFile string) {
	config := tls.Config{}

	// load the server cert / key
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		relog.Fatal("StartHttpsServer.LoadX509KeyPair failed: %v", err)
	}
	config.Certificates = []tls.Certificate{cert}

	// load the ca if necessary
	// FIXME(alainjobart) this doesn't quite work yet, have
	// to investigate
	if caFile != "" {
		config.ClientCAs = x509.NewCertPool()

		ca, err := os.Open(caFile)
		if err != nil {
			relog.Fatal("StartHttpsServer failed to open caFile %v: %v", caFile, err)
		}
		defer ca.Close()

		fi, err := ca.Stat()
		if err != nil {
			relog.Fatal("StartHttpsServer failed to stat caFile %v: %v", caFile, err)
		}

		pemCerts := make([]byte, fi.Size())
		if _, err = ca.Read(pemCerts); err != nil {
			relog.Fatal("StartHttpsServer failed to read caFile %v: %v", caFile, err)
		}

		if !config.ClientCAs.AppendCertsFromPEM(pemCerts) {
			relog.Fatal("StartHttpsServer failed to parse caFile %v", caFile)
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	httpsListener, err := tls.Listen("tcp", addr, &config)
	if err != nil {
		relog.Fatal("StartHttpsServer failed: %v", err)
	}
	go asyncListener(httpsListener)
}
