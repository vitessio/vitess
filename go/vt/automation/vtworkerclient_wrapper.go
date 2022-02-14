package automation

import (
	"bytes"
	"fmt"

	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/worker/vtworkerclient"
)

// ExecuteVtworker executes the vtworker command in "args" via an RPC to
// "server".
// The output of the RPC, a stream of LoggerEvent messages, is concatenated into
// one output string.
// If a retryable error is encountered (e.g. the vtworker process is already
// executing another command), this function will keep retrying infinitely until
// "ctx" is cancelled.
func ExecuteVtworker(ctx context.Context, server string, args []string) (string, error) {
	var output bytes.Buffer
	loggerToBufferFunc := createLoggerEventToBufferFunction(&output)
	outputLogger := newOutputLogger(loggerToBufferFunc)

	startMsg := fmt.Sprintf("Executing remote vtworker command: %v server: %v", args, server)
	outputLogger.Infof(startMsg)
	log.Info(startMsg)

	err := vtworkerclient.RunCommandAndWait(ctx, server, args, loggerToBufferFunc)

	endMsg := fmt.Sprintf("Executed remote vtworker command: %v server: %v err: %v", args, server, err)
	outputLogger.Infof(endMsg)
	// Log full output to log file (but not to the buffer).
	log.Infof("%v output (starting on next line):\n%v", endMsg, output.String())

	return output.String(), err
}
