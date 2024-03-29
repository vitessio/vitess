//go:build !windows

/*
Copyright 2023 The Vitess Authors.

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

package mysql

import (
	"crypto/tls"
	"io"
	"syscall"

	"vitess.io/vitess/go/mysql/sqlerror"
)

// ConnCheck ensures that this connection to the MySQL server hasn't been broken.
// This is a fast, non-blocking check. For details on its implementation, please read
// "Three Bugs in the Go MySQL Driver" (Vicent Marti, GitHub, 2020)
// https://github.blog/2020-05-20-three-bugs-in-the-go-mysql-driver/
func (c *Conn) ConnCheck() error {
	conn := c.conn
	if tlsconn, ok := conn.(*tls.Conn); ok {
		conn = tlsconn.NetConn()
	}
	if conn, ok := conn.(syscall.Conn); ok {
		rc, err := conn.SyscallConn()
		if err != nil {
			return err
		}

		var n int
		var buff [1]byte
		rerr := rc.Read(func(fd uintptr) bool {
			n, err = syscall.Read(int(fd), buff[:])
			return true
		})

		switch {
		case rerr != nil:
			return rerr
		case n == 0 && err == nil:
			return io.EOF
		case n > 0:
			return sqlerror.NewSQLError(sqlerror.CRUnknownError, sqlerror.SSUnknownSQLState, "unexpected read from conn")
		case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
			return nil
		default:
			return err
		}
	}
	return nil
}
