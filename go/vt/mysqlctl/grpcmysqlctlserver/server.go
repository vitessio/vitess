// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package grpcmysqlctlserver contains the gRPC implementation of the server
side of the remote execution of mysqlctl commands.
*/
package grpcmysqlctlserver

import (
	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/mysqlctl"
	"golang.org/x/net/context"

	mysqlctlpb "github.com/youtube/vitess/go/vt/proto/mysqlctl"
)

// server is our gRPC server.
type server struct {
	mysqld *mysqlctl.Mysqld
}

// Start implements the server side of the MysqlctlClient interface.
func (s *server) Start(ctx context.Context, request *mysqlctlpb.StartRequest) (*mysqlctlpb.StartResponse, error) {
	return &mysqlctlpb.StartResponse{}, s.mysqld.Start(ctx, request.MysqldArgs...)
}

// Shutdown implements the server side of the MysqlctlClient interface.
func (s *server) Shutdown(ctx context.Context, request *mysqlctlpb.ShutdownRequest) (*mysqlctlpb.ShutdownResponse, error) {
	return &mysqlctlpb.ShutdownResponse{}, s.mysqld.Shutdown(ctx, request.WaitForMysqld)
}

// RunMysqlUpgrade implements the server side of the MysqlctlClient interface.
func (s *server) RunMysqlUpgrade(ctx context.Context, request *mysqlctlpb.RunMysqlUpgradeRequest) (*mysqlctlpb.RunMysqlUpgradeResponse, error) {
	return &mysqlctlpb.RunMysqlUpgradeResponse{}, s.mysqld.RunMysqlUpgrade()
}

// ReinitConfig implements the server side of the MysqlctlClient interface.
func (s *server) ReinitConfig(ctx context.Context, request *mysqlctlpb.ReinitConfigRequest) (*mysqlctlpb.ReinitConfigResponse, error) {
	return &mysqlctlpb.ReinitConfigResponse{}, s.mysqld.ReinitConfig(ctx)
}

// StartServer registers the Server for RPCs.
func StartServer(s *grpc.Server, mysqld *mysqlctl.Mysqld) {
	mysqlctlpb.RegisterMysqlCtlServer(s, &server{mysqld})
}
