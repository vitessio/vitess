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
package grpcoptionaltls

import (
	"bytes"
	"io"
	"net"
)

// WrappedConn imitates MSG_PEEK behaviour
// Unlike net.Conn is not thread-safe for reading already peeked bytes
type WrappedConn struct {
	net.Conn
	rd io.Reader
}

func NewWrappedConn(conn net.Conn, peeked []byte) net.Conn {
	var rd = io.MultiReader(bytes.NewReader(peeked), conn)
	return &WrappedConn{
		Conn: conn,
		rd:   rd,
	}
}

func (wc *WrappedConn) Read(b []byte) (n int, err error) {
	return wc.rd.Read(b)
}
