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

	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/servenv"
	"golang.org/x/net/context"
)

// MysqlctlServer is our RPC server.
type MysqlctlServer struct {
	mysqld *mysqlctl.Mysqld
}

// Start implements the server side of the MysqlctlClient interface.
func (s *MysqlctlServer) Start(ctx context.Context, args *time.Duration, reply *rpc.Unused) error {
	if *args != 0 {
		// if a duration was passed in, add it to the Context.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *args)
		defer cancel()
	}
	return s.mysqld.Start(ctx)
}

// Shutdown implements the server side of the MysqlctlClient interface.
func (s *MysqlctlServer) Shutdown(ctx context.Context, args *time.Duration, reply *rpc.Unused) error {
	waitForMysqld := false
	if *args != 0 {
		// if a duration was passed in, add it to the Context.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *args)
		defer cancel()
		waitForMysqld = true
	}
	return s.mysqld.Shutdown(ctx, waitForMysqld)
}

// RunMysqlUpgrade implements the server side of the MysqlctlClient interface.
func (s *MysqlctlServer) RunMysqlUpgrade(ctx context.Context, args *rpc.Unused, reply *rpc.Unused) error {
	return s.mysqld.RunMysqlUpgrade()
}

// StartServer registers the Server for RPCs.
func StartServer(mysqld *mysqlctl.Mysqld) {
	servenv.Register("mysqlctl", &MysqlctlServer{mysqld})
}
