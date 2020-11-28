package grpcvtctldserver

import (
	"context"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/topo"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

// VtctldServer implements the Vtctld RPC service protocol.
type VtctldServer struct {
	ts *topo.Server
}

// NewVtctldServer returns a new VtctldServer for the given topo server.
func NewVtctldServer(ts *topo.Server) *VtctldServer {
	return &VtctldServer{ts: ts}
}

// GetKeyspace is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetKeyspace(ctx context.Context, req *vtctldatapb.GetKeyspaceRequest) (*vtctldatapb.Keyspace, error) {
	keyspace, err := s.ts.GetKeyspace(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.Keyspace{
		Name:     req.Keyspace,
		Keyspace: keyspace.Keyspace,
	}, nil
}

// GetKeyspaces is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) GetKeyspaces(ctx context.Context, req *vtctldatapb.GetKeyspacesRequest) (*vtctldatapb.GetKeyspacesResponse, error) {
	keyspaces, err := s.ts.GetKeyspaces(ctx)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetKeyspacesResponse{Keyspaces: keyspaces}, nil
}

// ShowAllKeyspaces is part of the vtctlservicepb.VtctldServer interface.
func (s *VtctldServer) ShowAllKeyspaces(req *vtctldatapb.ShowAllKeyspacesRequest, stream vtctlservicepb.Vtctld_ShowAllKeyspacesServer) error {
	ctx := stream.Context()

	keyspaces, err := s.ts.GetKeyspaces(ctx)
	if err != nil {
		return err
	}

	for _, keyspace := range keyspaces {
		ks, err := s.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: keyspace})
		if err != nil {
			return err
		}

		stream.Send(ks)
	}

	return nil
}

// StartServer registers a VtctldServer for RPCs on the given gRPC server.
func StartServer(s *grpc.Server, ts *topo.Server) {
	vtctlservicepb.RegisterVtctldServer(s, NewVtctldServer(ts))
}
