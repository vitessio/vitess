// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package grpcvtctlserver contains the gRPC implementation of the server side
of the remote execution of vtctl commands.
*/
package grpcvtctlserver

import (
	"sync"
	"time"

	log "github.com/golang/glog"
	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"

	"github.com/youtube/vitess/go/vt/vtctl"
	pb "github.com/youtube/vitess/go/vt/vtctl/grpcproto"
)

// VtctlServer is our RPC server
type VtctlServer struct {
	ts topo.Server
}

// NewVtctlServer returns a new Vtctl Server for the topo server.
func NewVtctlServer(ts topo.Server) *VtctlServer {
	return &VtctlServer{ts}
}

// ExecuteVtctlCommand is part of the pb.VtctlServer interface
func (s *VtctlServer) ExecuteVtctlCommand(args *pb.ExecuteVtctlCommandArgs, reply pb.Vtctl_ExecuteVtctlCommandServer) error {
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
			reply.Send(&pb.LoggerEvent{
				Time: &pb.Time{
					Seconds:     e.Time.Unix(),
					Nanoseconds: int64(e.Time.Nanosecond()),
				},
				Level: int64(e.Level),
				File:  e.File,
				Line:  int64(e.Line),
				Value: e.Value,
			})
		}
		wg.Done()
	}()

	// create the wrangler
	wr := wrangler.New(logger, s.ts, tmclient.NewTabletManagerClient(), time.Duration(args.LockTimeout))

	// execute the command
	err := vtctl.RunCommand(reply.Context(), wr, args.Args)

	// close the log channel, and wait for them all to be sent
	close(logstream)
	wg.Wait()

	return err
}

// StartServer registers the VtctlServer for RPCs
func StartServer(s *grpc.Server, ts topo.Server) {
	if !servenv.ServiceMap["grpc-vtctl"] {
		log.Infof("Disabling gRPC vtctl service")
		return
	}
	pb.RegisterVtctlServer(s, NewVtctlServer(ts))
}
