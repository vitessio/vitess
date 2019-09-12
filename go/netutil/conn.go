/*
Copyright 2018 The Vitess Authors.
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
	"time"
)

var _ net.Conn = (*ConnWithTimeouts)(nil)

// A ConnWithTimeouts is a wrapper to net.Comm that allows to set a read and write timeouts.
type ConnWithTimeouts struct {
	net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewConnWithTimeouts wraps a net.Conn with read and write deadilnes.
func NewConnWithTimeouts(conn net.Conn, readTimeout time.Duration, writeTimeout time.Duration) ConnWithTimeouts {
	return ConnWithTimeouts{Conn: conn, readTimeout: readTimeout, writeTimeout: writeTimeout}
}

// Implementation of the Conn interface.

// Read sets a read deadilne and delegates to conn.Read.
func (c ConnWithTimeouts) Read(b []byte) (int, error) {
	if c.readTimeout == 0 {
		return c.Conn.Read(b)
	}
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}

// Write sets a write deadline and delegates to conn.Write
func (c ConnWithTimeouts) Write(b []byte) (int, error) {
	if c.writeTimeout == 0 {
		return c.Conn.Write(b)
	}
	if err := c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

// SetDeadline implements the Conn SetDeadline method.
func (c ConnWithTimeouts) SetDeadline(t time.Time) error {
	panic("can't call SetDeadline for ConnWithTimeouts")
}

// SetReadDeadline implements the Conn SetReadDeadline method.
func (c ConnWithTimeouts) SetReadDeadline(t time.Time) error {
	panic("can't call SetReadDeadline for ConnWithTimeouts")
}

// SetWriteDeadline implements the Conn SetWriteDeadline method.
func (c ConnWithTimeouts) SetWriteDeadline(t time.Time) error {
	panic("can't call SetWriteDeadline for ConnWithTimeouts")
}
