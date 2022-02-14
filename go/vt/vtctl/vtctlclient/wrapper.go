package vtctlclient

import (
	"errors"
	"fmt"
	"io"
	"time"

	"context"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

var (
	defaultTimeout = time.Hour
)

// RunCommandAndWait executes a single command on a given vtctld and blocks until the command did return or timed out.
// Output from vtctld is streamed as logutilpb.Event messages which
// have to be consumed by the caller who has to specify a "recv" function.
func RunCommandAndWait(ctx context.Context, server string, args []string, recv func(*logutilpb.Event)) error {
	if recv == nil {
		return errors.New("no function closure for Event stream specified")
	}
	// create the client
	client, err := New(server)
	if err != nil {
		return fmt.Errorf("cannot dial to server %v: %v", server, err)
	}
	defer client.Close()

	// run the command ( get the timeout from the context )
	timeout := defaultTimeout
	deadline, ok := ctx.Deadline()
	if ok {
		timeout = time.Until(deadline)
	}
	stream, err := client.ExecuteVtctlCommand(ctx, args, timeout)
	if err != nil {
		return fmt.Errorf("cannot execute remote command: %v", err)
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
			return fmt.Errorf("remote error: %v", err)
		}
	}
}
