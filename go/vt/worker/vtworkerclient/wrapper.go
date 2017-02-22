// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtworkerclient

import (
	"io"
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
		panic("no function closure for Event stream specified")
	}
	// create the client
	// TODO(mberlin): vtctlclient exposes dialTimeout as flag. If there are no use cases, remove it there as well to be consistent?
	client, err := New(server, 30*time.Second /* dialTimeout */)
	if err != nil {
		return vterrors.Errorf(vterrors.Code(err), "cannot dial to server "+server+": %v", err)
	}
	defer client.Close()

	// run the command
	stream, err := client.ExecuteVtworkerCommand(ctx, args)
	if err != nil {
		return vterrors.Errorf(vterrors.Code(err), "cannot execute remote command: %v", err)
	}

	for {
		e, err := stream.Recv()
		switch err {
		case nil:
			recv(e)
		case io.EOF:
			return nil
		default:
			return vterrors.Errorf(vterrors.Code(err), "stream error: %v", err)
		}
	}
}
