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
func RunCommandAndWait(ctx context.Context, server string, args []string, actionTimeout time.Duration, recv func(*logutilpb.Event)) error {
	if recv == nil {
		return errors.New("No function closure for Event stream specified")
	}
	// create the client
	client, err := New(server)
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
