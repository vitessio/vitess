/*
Copyright 2017 Google Inc.

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

package vtworkerclient

import (
	"io"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/vterrors"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// RunCommandAndWait executes a single command on a given vtworker and blocks until the command did return or timed out.
// Output from vtworker is streamed as logutil.Event messages which
// have to be consumed by the caller who has to specify a "recv" function.
func RunCommandAndWait(ctx context.Context, server string, args []string, recv func(*logutilpb.Event)) error {
	if recv == nil {
		panic("no function closure for Event stream specified")
	}
	// create the client
	client, err := New(server)
	if err != nil {
		return vterrors.Wrapf(err, "cannot dial to server %v", server)
	}
	defer client.Close()

	// run the command
	stream, err := client.ExecuteVtworkerCommand(ctx, args)
	if err != nil {
		return vterrors.Wrap(err, "cannot execute remote command")
	}

	for {
		e, err := stream.Recv()
		switch err {
		case nil:
			recv(e)
		case io.EOF:
			return nil
		default:
			return vterrors.Wrap(err, "stream error")
		}
	}
}
