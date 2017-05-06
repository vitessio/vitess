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
