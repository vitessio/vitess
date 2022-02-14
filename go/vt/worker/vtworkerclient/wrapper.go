package vtworkerclient

import (
	"io"

	"context"

	"vitess.io/vitess/go/vt/vterrors"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
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
