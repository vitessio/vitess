// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtworkerclient

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/net/context"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	"github.com/youtube/vitess/go/vt/vterrors"
)

// RunCommandAndWait executes a single command on a given vtworker and blocks until the command did return or timed out.
// Output from vtworker is streamed as logutil.Event messages which
// have to be consumed by the caller who has to specify a "recv" function.
func RunCommandAndWait(ctx context.Context, server string, args []string, recv func(*logutilpb.Event)) error {
	if recv == nil {
		return errors.New("No function closure for Event stream specified")
	}
	// create the client
	// TODO(mberlin): vtctlclient exposes dialTimeout as flag. If there are no use cases, remove it there as well to be consistent?
	client, err := New(server, 30*time.Second /* dialTimeout */)
	if err != nil {
		return fmt.Errorf("Cannot dial to server %v: %v", server, err)
	}
	defer client.Close()

	// run the command
	stream, err := client.ExecuteVtworkerCommand(ctx, args)
	if err != nil {
		return fmt.Errorf("Cannot execute remote command: %v", err)
	}

	for {
		e, err := stream.Recv()
		if err != nil {
			if vterrors.IsEOF(err) {
				return nil
			}
			return fmt.Errorf("Remote error: %v", err)
		}
		recv(e)
	}
}
