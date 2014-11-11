// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package gorpcmysqlctlserver contains the Go RPC implementation of the server
side of the remote execution of mysqlctl commands.
*/
package gorpcmysqlctlserver

import (
	"time"

	"code.google.com/p/go.net/context"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
)

// MysqlctlServer is our RPC server.
type MysqlctlServer struct {
	mysqld *mysqlctl.Mysqld
}

// Start implements the server side of the MysqlctlClient interface.
func (s *MysqlctlServer) Start(ctx context.Context, args *time.Duration, reply *int) error {
	return s.mysqld.Start(*args)
}

// Shutdown implements the server side of the MysqlctlClient interface.
func (s *MysqlctlServer) Shutdown(ctx context.Context, args *time.Duration, reply *int) error {
	return s.mysqld.Shutdown(*args > 0, *args)
}

// StartServer registers the Server for RPCs.
func StartServer(mysqld *mysqlctl.Mysqld) {
	servenv.Register("mysqlctl", &MysqlctlServer{mysqld})
}
