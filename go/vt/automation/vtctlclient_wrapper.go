// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"bytes"
	"fmt"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// ExecuteVtctl runs vtctl using vtctlclient. The stream of Event
// messages is concatenated into one output string.
// Additionally, the start and the end of the command will be logged to make
// it easier to debug which command was executed and how long it took.
func ExecuteVtctl(ctx context.Context, server string, args []string) (string, error) {
	var output bytes.Buffer
	loggerToBufferFunc := createLoggerEventToBufferFunction(&output)
	outputLogger := newOutputLogger(loggerToBufferFunc)

	startMsg := fmt.Sprintf("Executing remote vtctl command: %v server: %v", args, server)
	outputLogger.Infof(startMsg)
	log.Info(startMsg)

	err := vtctlclient.RunCommandAndWait(
		ctx, server, args,
		// TODO(mberlin): Should these values be configurable as flags?
		30*time.Second, // dialTimeout
		time.Hour,      // actionTimeout
		loggerToBufferFunc)

	endMsg := fmt.Sprintf("Executed remote vtctl command: %v server: %v err: %v", args, server, err)
	outputLogger.Infof(endMsg)
	// Log full output to log file (but not to the buffer).
	log.Infof("%v output (starting on next line):\n%v", endMsg, output.String())

	return output.String(), err
}

// createLoggerEventToBufferFunction returns a function to add LoggerEvent
// structs to a given buffer, one line per event.
// The buffer can be used to return a multi-line string with all events.
func createLoggerEventToBufferFunction(output *bytes.Buffer) func(*logutilpb.Event) {
	return func(e *logutilpb.Event) {
		logutil.EventToBuffer(e, output)
		output.WriteRune('\n')
	}
}

// newOutputLogger returns a logger which makes it easy to log to a bytes.Buffer
// output. When calling this function, pass in the result of
// createLoggerEventToBufferFunction().
func newOutputLogger(loggerToBufferFunc func(*logutilpb.Event)) logutil.Logger {
	return logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		loggerToBufferFunc(e)
	})
}
