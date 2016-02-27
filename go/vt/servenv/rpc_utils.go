// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/tb"
)

// HandlePanic should be called using 'defer' in the RPC code that executes the command.
func HandlePanic(component string, err *error) {
	if x := recover(); x != nil {
		// gRPC 0.13 chokes when you return a streaming error that contains newlines.
		*err = fmt.Errorf("uncaught %v panic: %v, %s", component, x,
			strings.Replace(string(tb.Stack(4)), "\n", ";", -1))
	}
}
