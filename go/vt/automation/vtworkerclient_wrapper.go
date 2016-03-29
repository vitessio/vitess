// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"bytes"
	"fmt"
	"time"

	log "github.com/golang/glog"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"golang.org/x/net/context"
)

const retryInterval time.Duration = 5 * time.Second

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

	var err error
	var loggedRetryStart bool
	var retryStart time.Time
	for {
		err = vtworkerclient.RunCommandAndWait(
			ctx, server, args,
			loggerToBufferFunc)
		if err == nil {
			break
		}

		if !isRetryable(err) {
			break
		}

		if !loggedRetryStart {
			loggedRetryStart = true
			retryStart = time.Now()
			retryStartMsg := fmt.Sprintf("vtworker responded with a retryable error (%v). keeping retrying every %.0f seconds until cancelled.", err, retryInterval.Seconds())
			outputLogger.Infof(retryStartMsg)
			log.Info(retryStartMsg)
		}

		// Sleep until the next retry.
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			// Context is up. The next retry should result in a non-retryable error.
			timer.Stop()
		case <-timer.C:
		}
	} // retry loop

	if loggedRetryStart {
		// Log end of retrying explicitly as well.
		d := time.Now().Sub(retryStart)
		retryEndMsg := fmt.Sprintf("Stopped retrying after %.1f seconds.", d.Seconds())
		outputLogger.Infof(retryEndMsg)
		log.Info(retryEndMsg)
	}

	endMsg := fmt.Sprintf("Executed remote vtworker command: %v server: %v err: %v", args, server, err)
	outputLogger.Infof(endMsg)
	// Log full output to log file (but not to the buffer).
	log.Infof("%v output (starting on next line):\n%v", endMsg, output.String())

	return output.String(), err
}

// TODO(mberlin): Discuss with the team if it should go to the vterrors package.
// TODO(mberlin): Add other error codes here as well?
func isRetryable(err error) bool {
	switch vterrors.RecoverVtErrorCode(err) {
	case vtrpcpb.ErrorCode_TRANSIENT_ERROR:
		return true
	default:
		return false
	}
}
