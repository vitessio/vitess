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

/*
Package grpcvtctlserver contains the gRPC implementation of the server side
of the remote execution of vtctl commands.
*/
package grpcvtctlserver

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

// VtctlServer is our RPC server
type VtctlServer struct {
	ts *topo.Server
}

// NewVtctlServer returns a new Vtctl Server for the topo server.
func NewVtctlServer(ts *topo.Server) *VtctlServer {
	return &VtctlServer{ts}
}

// ExecuteVtctlCommand is part of the vtctldatapb.VtctlServer interface
func (s *VtctlServer) ExecuteVtctlCommand(args *vtctldatapb.ExecuteVtctlCommandRequest, stream vtctlservicepb.Vtctl_ExecuteVtctlCommandServer) (err error) {
	defer servenv.HandlePanic("vtctl", &err)

	// Create a logger, send the result back to the caller.
	// We may execute this in parallel (inside multiple go routines),
	// but the stream.Send() method is not thread safe in gRPC.
	// So use a mutex to protect it.
	mu := sync.Mutex{}
	logstream := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		// If the client disconnects, we will just fail
		// to send the log events, but won't interrupt
		// the command.
		mu.Lock()
		stream.Send(&vtctldatapb.ExecuteVtctlCommandResponse{
			Event: e,
		})
		mu.Unlock()
	})
	logger := logutil.NewTeeLogger(logstream, logutil.NewConsoleLogger())

	// create the wrangler
	tmc := tmclient.NewTabletManagerClient()
	defer tmc.Close()
	wr := wrangler.New(logger, s.ts, tmc)

	// execute the command
	return vtctl.RunCommand(stream.Context(), wr, args.Args)
}

// StartServer registers the VtctlServer for RPCs
func StartServer(s *grpc.Server, ts *topo.Server) {
	vtctlservicepb.RegisterVtctlServer(s, NewVtctlServer(ts))
	vtctlservicepb.RegisterVtctldServer(s, NewVtctldServer(ts))
}

type VtctldServer struct {
	ts *topo.Server
}

func NewVtctldServer(ts *topo.Server) *VtctldServer {
	return &VtctldServer{ts}
}

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

func (s *VtctldServer) GetKeyspaces(ctx context.Context, req *vtctldatapb.GetKeyspacesRequest) (*vtctldatapb.GetKeyspacesResponse, error) {
	keyspaces, err := s.ts.GetKeyspaces(ctx)
	if err != nil {
		return nil, err
	}

	return &vtctldatapb.GetKeyspacesResponse{Keyspaces: keyspaces}, nil
}

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
