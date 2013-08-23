// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proc

import (
	"net"

	"github.com/youtube/vitess/go/stats"
)

type CountingListener struct {
	net.Listener
	ConnCount, ConnAccept *stats.Int
}

type countingConnection struct {
	net.Conn
	listener *CountingListener
}

// Published creates a wrapper for net.Listener that
// publishes connection stats.
func Published(l net.Listener, countTag, acceptTag string) net.Listener {
	return &CountingListener{
		Listener:   l,
		ConnCount:  stats.NewInt(countTag),
		ConnAccept: stats.NewInt(acceptTag),
	}
}

// Accept increments stats counters before returning
// a connection.
func (l *CountingListener) Accept() (c net.Conn, err error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.ConnCount.Add(1)
	l.ConnAccept.Add(1)
	return &countingConnection{conn, l}, nil
}

// Close decrements the stats counter and
// closes the connection.
func (c *countingConnection) Close() error {
	if c.listener != nil {
		c.listener.ConnCount.Add(-1)
		c.listener = nil
	}
	return c.Conn.Close()
}
