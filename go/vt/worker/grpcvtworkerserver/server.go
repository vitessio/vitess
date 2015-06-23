// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package grpcvtworkerserver contains the gRPC implementation of the server side
of the remote execution of vtworker commands.
*/
package grpcvtworkerserver

import (
	"sync"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/wrangler"

	pbvd "github.com/youtube/vitess/go/vt/proto/vtctldata"
	pb "github.com/youtube/vitess/go/vt/proto/vtworkerdata"
	pbs "github.com/youtube/vitess/go/vt/proto/vtworkerservice"
)

// VtworkerServer is our RPC server
type VtworkerServer struct {
	wi *worker.Instance
}

// NewVtworkerServer returns a new Vtworker Server for the topo server.
func NewVtworkerServer(wi *worker.Instance) *VtworkerServer {
	return &VtworkerServer{wi}
}

// ExecuteVtworkerCommand is part of the pb.VtworkerServer interface
func (s *VtworkerServer) ExecuteVtworkerCommand(args *pb.ExecuteVtworkerCommandRequest, stream pbs.Vtworker_ExecuteVtworkerCommandServer) (err error) {
	defer vtctl.HandlePanic(&err)

	// create a logger, send the result back to the caller
	logstream := logutil.NewChannelLogger(10)
	logger := logutil.NewTeeLogger(logstream, logutil.NewConsoleLogger())

	// send logs to the caller
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for e := range logstream {
			// Note we don't interrupt the loop here, as
			// we still need to flush and finish the
			// command, even if the channel to the client
			// has been broken. We'll just keep trying.
			stream.Send(&pb.ExecuteVtworkerCommandResponse{
				Event: &pbvd.LoggerEvent{
					Time: &pbvd.Time{
						Seconds:     e.Time.Unix(),
						Nanoseconds: int64(e.Time.Nanosecond()),
					},
					Level: int64(e.Level),
					File:  e.File,
					Line:  int64(e.Line),
					Value: e.Value,
				},
			})
		}
		wg.Done()
	}()

	// create the wrangler
	wr := wrangler.New(logger, s.wi.TopoServer, tmclient.NewTabletManagerClient(), s.wi.LockTimeout)

	// execute the command
	err = s.wi.RunCommand(args.Args, wr)

	// close the log channel, and wait for them all to be sent
	close(logstream)
	wg.Wait()

	return err
}

// StartServer registers the VtworkerServer for RPCs
func StartServer(s *grpc.Server, wi *worker.Instance) {
	pbs.RegisterVtworkerServer(s, NewVtworkerServer(wi))
}
