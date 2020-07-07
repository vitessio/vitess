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
Package grpcvtworkerserver contains the gRPC implementation of the server side
of the remote execution of vtworker commands.
*/
package grpcvtworkerserver

import (
	"sync"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/worker"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	vtworkerdatapb "vitess.io/vitess/go/vt/proto/vtworkerdata"
	vtworkerservicepb "vitess.io/vitess/go/vt/proto/vtworkerservice"
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
	// Please note that this panic handler catches only panics occurring in the code below.
	// The actual execution of the vtworker command takes place in a new go routine
	// (started in Instance.setAndStartWorker()) which has its own panic handler.
	defer servenv.HandlePanic("vtworker", &err)

	// Stream everything back what the Wrangler is logging.
	// We may execute this in parallel (inside multiple go routines),
	// but the stream.Send() method is not thread safe in gRPC.
	// So use a mutex to protect it.
	mu := sync.Mutex{}
	logstream := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		mu.Lock()
		stream.Send(&vtworkerdatapb.ExecuteVtworkerCommandResponse{
			Event: e,
		})
		mu.Unlock()
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
