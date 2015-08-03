// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"bytes"

	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"golang.org/x/net/context"
)

// ExecuteVtworker runs vtworker using vtworkerclient. The stream of LoggerEvent messages is concatenated into one output string.
func ExecuteVtworker(ctx context.Context, server string, args []string) (string, error) {
	var output bytes.Buffer

	err := vtworkerclient.RunCommandAndWait(
		ctx, server, args,
		CreateLoggerEventToBufferFunction(&output))

	return output.String(), err
}
