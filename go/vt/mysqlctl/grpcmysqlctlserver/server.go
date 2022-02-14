/*
Package grpcmysqlctlserver contains the gRPC implementation of the server
side of the remote execution of mysqlctl commands.
*/
package grpcmysqlctlserver

import (
	"google.golang.org/grpc"

	"context"

	"vitess.io/vitess/go/vt/mysqlctl"

	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
)

// server is our gRPC server.
type server struct {
	mysqlctlpb.UnimplementedMysqlCtlServer
	cnf    *mysqlctl.Mycnf
	mysqld *mysqlctl.Mysqld
}

// Start implements the server side of the MysqlctlClient interface.
func (s *server) Start(ctx context.Context, request *mysqlctlpb.StartRequest) (*mysqlctlpb.StartResponse, error) {
	return &mysqlctlpb.StartResponse{}, s.mysqld.Start(ctx, s.cnf, request.MysqldArgs...)
}

// Shutdown implements the server side of the MysqlctlClient interface.
func (s *server) Shutdown(ctx context.Context, request *mysqlctlpb.ShutdownRequest) (*mysqlctlpb.ShutdownResponse, error) {
	return &mysqlctlpb.ShutdownResponse{}, s.mysqld.Shutdown(ctx, s.cnf, request.WaitForMysqld)
}

// RunMysqlUpgrade implements the server side of the MysqlctlClient interface.
func (s *server) RunMysqlUpgrade(ctx context.Context, request *mysqlctlpb.RunMysqlUpgradeRequest) (*mysqlctlpb.RunMysqlUpgradeResponse, error) {
	return &mysqlctlpb.RunMysqlUpgradeResponse{}, s.mysqld.RunMysqlUpgrade()
}

// ReinitConfig implements the server side of the MysqlctlClient interface.
func (s *server) ReinitConfig(ctx context.Context, request *mysqlctlpb.ReinitConfigRequest) (*mysqlctlpb.ReinitConfigResponse, error) {
	return &mysqlctlpb.ReinitConfigResponse{}, s.mysqld.ReinitConfig(ctx, s.cnf)
}

// RefreshConfig implements the server side of the MysqlctlClient interface.
func (s *server) RefreshConfig(ctx context.Context, request *mysqlctlpb.RefreshConfigRequest) (*mysqlctlpb.RefreshConfigResponse, error) {
	return &mysqlctlpb.RefreshConfigResponse{}, s.mysqld.RefreshConfig(ctx, s.cnf)
}

// StartServer registers the Server for RPCs.
func StartServer(s *grpc.Server, cnf *mysqlctl.Mycnf, mysqld *mysqlctl.Mysqld) {
	mysqlctlpb.RegisterMysqlCtlServer(s, &server{cnf: cnf, mysqld: mysqld})
}
