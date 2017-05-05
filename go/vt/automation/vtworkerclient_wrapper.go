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

package automation

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
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
