// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package gorpcvtctlserver contains the Go RPC implementation of the server side
of the remote execution of vtctl commands.
*/
package gorpcvtctlserver

import (
	"sync"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/vtctl/gorpcproto"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

// VtctlServer is our RPC server
type VtctlServer struct {
	ts topo.Server
}

// ExecuteVtctlCommand is the server side method that will execute the query,
// and stream the results.
func (s *VtctlServer) ExecuteVtctlCommand(ctx context.Context, query *gorpcproto.ExecuteVtctlCommandArgs, sendReply func(interface{}) error) error {
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
			sendReply(&e)
		}
		wg.Done()
	}()

	// create the wrangler
	wr := wrangler.New(logger, s.ts, query.ActionTimeout, query.LockTimeout)
	// FIXME(alainjobart) use a single context, copy the source info from it
	ctx, cancel := context.WithTimeout(context.TODO(), query.ActionTimeout)

	// execute the command
	err := vtctl.RunCommand(ctx, wr, query.Args)
	cancel()

	// close the log channel, and wait for them all to be sent
	close(logstream)
	wg.Wait()

	return err
}

// NewVtctlServer returns a new Vtctl Server for the topo server.
func NewVtctlServer(ts topo.Server) *VtctlServer {
	return &VtctlServer{ts}
}

// StartServer registers the Server for RPCs
func StartServer(ts topo.Server) {
	servenv.Register("vtctl", NewVtctlServer(ts))
}
