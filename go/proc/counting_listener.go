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

package proc

import (
	"net"

	"vitess.io/vitess/go/stats"
)

type CountingListener struct {
	net.Listener
	ConnCount  *stats.Gauge
	ConnAccept *stats.Counter
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
		ConnCount:  stats.NewGauge(countTag, "Active connections accepted by counting listener"),
		ConnAccept: stats.NewCounter(acceptTag, "Count of connections accepted by the counting listener"),
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
