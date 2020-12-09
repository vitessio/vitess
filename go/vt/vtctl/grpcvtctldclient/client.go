// Package grpcvtctldclient contains the gRPC version of the vtctld client
// protocol.
package grpcvtctldclient

import (
	"context"
	"flag"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

const connClosedMsg = "grpc: the client connection is closed"

// (TODO:@amason) - These flags match exactly the flags used in grpcvtctlclient.
// If a program attempts to import both of these packages, it will panic during
// startup due to the duplicate flags.
//
// For everything else I've been doing a sed s/vtctl/vtctld, but I cannot do
// that here, since these flags are already "vtctld_*". My other options are to
// name them "vtctld2_*" or to omit them completely.
//
// Not to pitch project ideas in comments, but a nice project to solve
var (
	cert = flag.String("vtctld_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("vtctld_grpc_key", "", "the key to use to connect")
	ca   = flag.String("vtctld_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name = flag.String("vtctld_grpc_server_name", "", "the server name to use to validate server certificate")
)

type gRPCVtctldClient struct {
	cc *grpc.ClientConn
	c  vtctlservicepb.VtctldClient
}

func gRPCVtctldClientFactory(addr string) (vtctldclient.VtctldClient, error) {
	opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}

	conn, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, err
	}

	return &gRPCVtctldClient{
		cc: conn,
		c:  vtctlservicepb.NewVtctldClient(conn),
	}, nil
}

func (client *gRPCVtctldClient) Close() error {
	err := client.cc.Close()
	if err == nil {
		client.c = nil
	}

	return err
}

// (TODO:@amason) - This boilerplate should end up the same for all ~70 commands
// .... we should do this with code gen.

func (client *gRPCVtctldClient) GetKeyspace(ctx context.Context, in *vtctldatapb.GetKeyspaceRequest, opts ...grpc.CallOption) (*vtctldatapb.GetKeyspaceResponse, error) {
	if client.c == nil {
		return nil, status.Error(codes.Unavailable, connClosedMsg)
	}

	return client.c.GetKeyspace(ctx, in, opts...)
}

func (client *gRPCVtctldClient) GetKeyspaces(ctx context.Context, in *vtctldatapb.GetKeyspacesRequest, opts ...grpc.CallOption) (*vtctldatapb.GetKeyspacesResponse, error) {
	if client.c == nil {
		return nil, status.Error(codes.Unavailable, connClosedMsg)
	}

	return client.c.GetKeyspaces(ctx, in, opts...)
}

func init() {
	vtctldclient.Register("grpc", gRPCVtctldClientFactory)
}
