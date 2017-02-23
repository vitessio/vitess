// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package grpcvtworkerserver contains the gRPC implementation of the server side
of the remote execution of vtworker commands.
*/
package grpcvtworkerserver

import (
	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/worker"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	vtworkerdatapb "github.com/youtube/vitess/go/vt/proto/vtworkerdata"
	vtworkerservicepb "github.com/youtube/vitess/go/vt/proto/vtworkerservice"
)

// VtworkerServer is our RPC server
type VtworkerServer struct {
	wi *worker.Instance
}

// NewVtworkerServer returns a new VtworkerServer for the given vtworker instance.
func NewVtworkerServer(wi *worker.Instance) *VtworkerServer {
	return &VtworkerServer{wi}
}

// ExecuteVtworkerCommand is part of the vtworkerdatapb.VtworkerServer interface
func (s *VtworkerServer) ExecuteVtworkerCommand(args *vtworkerdatapb.ExecuteVtworkerCommandRequest, stream vtworkerservicepb.Vtworker_ExecuteVtworkerCommandServer) (err error) {
	// Please note that this panic handler catches only panics occuring in the code below.
	// The actual execution of the vtworker command takes place in a new go routine
	// (started in Instance.setAndStartWorker()) which has its own panic handler.
	defer servenv.HandlePanic("vtworker", &err)

	// Stream everything back what the Wrangler is logging.
	logstream := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		stream.Send(&vtworkerdatapb.ExecuteVtworkerCommandResponse{
			Event: e,
		})
	})
	// Let the Wrangler also log everything to the console (and thereby
	// effectively to a logfile) to make sure that any information or errors
	// is preserved in the logs in case the RPC or vtworker crashes.
	logger := logutil.NewTeeLogger(logstream, logutil.NewConsoleLogger())

	wr := s.wi.CreateWrangler(logger)

	// Run the command as long as the RPC Context is valid.
	worker, done, err := s.wi.RunCommand(stream.Context(), args.Args, wr, false /*runFromCli*/)
	if err == nil && worker != nil && done != nil {
		err = s.wi.WaitForCommand(worker, done)
	}

	return vterrors.ToGRPC(err)
}

// StartServer registers the VtworkerServer for RPCs
func StartServer(s *grpc.Server, wi *worker.Instance) {
	vtworkerservicepb.RegisterVtworkerServer(s, NewVtworkerServer(wi))
}
