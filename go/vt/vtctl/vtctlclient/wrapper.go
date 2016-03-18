// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctlclient

import (
	"errors"
	"fmt"
	"io"
	"time"

	"golang.org/x/net/context"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// RunCommandAndWait executes a single command on a given vtctld and blocks until the command did return or timed out.
// Output from vtctld is streamed as logutilpb.Event messages which
// have to be consumed by the caller who has to specify a "recv" function.
func RunCommandAndWait(ctx context.Context, server string, args []string, dialTimeout, actionTimeout time.Duration, recv func(*logutilpb.Event)) error {
	if recv == nil {
		return errors.New("No function closure for Event stream specified")
	}
	// create the client
	client, err := New(server, dialTimeout)
	if err != nil {
		return fmt.Errorf("Cannot dial to server %v: %v", server, err)
	}
	defer client.Close()

	// run the command
	ctx, cancel := context.WithTimeout(context.Background(), actionTimeout)
	defer cancel()
	stream, err := client.ExecuteVtctlCommand(ctx, args, actionTimeout)
	if err != nil {
		return fmt.Errorf("Cannot execute remote command: %v", err)
	}

	// stream the result
	for {
		e, err := stream.Recv()
		switch err {
		case nil:
			recv(e)
		case io.EOF:
			return nil
		default:
			return fmt.Errorf("Remote error: %v", err)
		}
	}
}
