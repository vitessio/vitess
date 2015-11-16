// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"bytes"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// ExecuteVtctl runs vtctl using vtctlclient. The stream of Event
// messages is concatenated into one output string.
func ExecuteVtctl(ctx context.Context, server string, args []string) (string, error) {
	var output bytes.Buffer

	log.Infof("Executing remote vtctl command: %v server: %v", args, server)
	err := vtctlclient.RunCommandAndWait(
		ctx, server, args,
		// TODO(mberlin): Should these values be configurable as flags?
		30*time.Second, // dialTimeout
		time.Hour,      // actionTimeout
		CreateLoggerEventToBufferFunction(&output))

	return output.String(), err
}

// CreateLoggerEventToBufferFunction returns a function to add LoggerEvent
// structs to a given buffer, one line per event.
// The buffer can be used to return a multi-line string with all events.
func CreateLoggerEventToBufferFunction(output *bytes.Buffer) func(*logutilpb.Event) {
	return func(e *logutilpb.Event) {
		logutil.EventToBuffer(e, output)
		output.WriteRune('\n')
	}
}
